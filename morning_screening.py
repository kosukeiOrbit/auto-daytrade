"""
朝6:30実行スクリーニング（出来高急増 + 地合いチェック + Claude材料判定）
"""
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
import jpholiday
import os
import pandas as pd
from src.screening import Screener
from src.utils.market_sentiment import MarketSentiment
from src.utils.material_judge import MaterialJudge
from src.utils.tdnet_scraper import TDnetScraper
from src.utils.news_scraper import NewsScraper
from src.utils.notifier import DiscordNotifier
from src.utils.kabu_client import KabuClient
from src.utils.config import Config
from src.utils.jquants_client import JQuantsClient

# ログファイル設定（タスクスケジューラ実行時のログ保存用）
os.makedirs("logs", exist_ok=True)
logger.add(
    "logs/morning_screening_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    encoding="utf-8",
    level="INFO"
)


def get_previous_business_day(date):
    """
    直前の営業日を取得（土日祝日を除外）

    Args:
        date: 基準日 (datetime)

    Returns:
        datetime: 直前の営業日
    """
    prev_date = date - timedelta(days=1)
    while prev_date.weekday() >= 5 or jpholiday.is_holiday(prev_date):
        prev_date -= timedelta(days=1)
    return prev_date


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("朝スクリーニング開始（morning_screening.py）")
    logger.info("=" * 60)

    # 日本時間
    jst = tz.gettz("Asia/Tokyo")
    now = datetime.now(jst)

    logger.info(f"実行日時: {now.strftime('%Y-%m-%d %H:%M:%S')}")

    # スクリーニング対象日: 直前の営業日（前日の引け後データを使用）
    target_date = get_previous_business_day(now)
    weekday_name = ['月', '火', '水', '木', '金', '土', '日'][target_date.weekday()]
    logger.info(f"スクリーニング対象日: {target_date.strftime('%Y-%m-%d')} ({weekday_name}・前営業日)")

    # STEP 0: 買付余力取得（kabu Station API）
    logger.info("\n" + "=" * 60)
    logger.info("STEP 0: 買付余力取得")
    logger.info("=" * 60)

    try:
        kabu_client = KabuClient()
        wallet = kabu_client.get_wallet_cash()
        available_cash = wallet['stock_account_wallet']

        # 検証環境の場合（nullの場合）は固定予算を使用
        if available_cash is None:
            budget = 800_000
            logger.warning(f"検証環境のため固定予算を使用: {budget:,}円")
        else:
            # 本番環境: 買付余力 × 投資比率
            budget = int(available_cash * Config.INVESTMENT_RATIO)
            logger.info(f"買付余力: {available_cash:,}円 × {Config.INVESTMENT_RATIO} = 本日の投資予算: {budget:,}円")

    except Exception as e:
        # API取得失敗時は固定予算を使用
        budget = 800_000
        logger.warning(f"買付余力取得エラー、固定予算を使用: {e}")

    # STEP 1a: 出来高急増銘柄を抽出
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1a: 出来高急増銘柄を抽出")
    logger.info("=" * 60)

    screener = Screener(budget=budget)
    volume_candidates = screener.get_volume_surge_candidates(
        surge_threshold=2.0,  # 20日平均の2倍以上
        lookback_days=20,
        date=target_date  # 前営業日のデータを使用
    )

    if len(volume_candidates) == 0:
        logger.warning("出来高急増銘柄が見つかりませんでした")

        # Discord通知（候補なし）
        notifier = DiscordNotifier()
        notifier.send_morning_report(
            candidates_df=pd.DataFrame(),
            judgments={},
            sentiment=None,
            tdnet_count=0,
            budget=budget
        )

        logger.info("=" * 60)
        logger.info("朝スクリーニング終了（候補なし）")
        logger.info("=" * 60)
        return

    logger.success(f"出来高急増銘柄: {len(volume_candidates)}銘柄")

    # STEP 1b: TDnet引け後適時開示取得
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1b: TDnet引け後適時開示取得")
    logger.info("=" * 60)

    tdnet = TDnetScraper()
    tdnet_codes = tdnet.get_disclosure_codes(now)  # 実行日で渡す（内部で営業日判定）

    if len(tdnet_codes) > 0:
        logger.success(f"引け後開示銘柄: {len(tdnet_codes)}銘柄")
        logger.info(f"  コード: {', '.join(tdnet_codes[:10])}{'...' if len(tdnet_codes) > 10 else ''}")
    else:
        logger.info("引け後開示銘柄なし")

    # STEP 1c: 候補プール（出来高急増 + TDnet開示銘柄）
    # 出来高急増銘柄リストに、TDnet銘柄を追加
    candidates = volume_candidates.copy()

    # TDnet銘柄で出来高急増リストに含まれていないものを追加
    # ※ただし、TDnet銘柄の詳細データ（OHLCV等）は別途取得が必要
    # 今回は出来高急増銘柄のみでフィルタ済みのため、TDnetコードは参考情報として保持
    tdnet_codes_set = set(tdnet_codes)

    # STEP 2: 地合いチェック
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: 地合いチェック（NYダウ・ナスダック）")
    logger.info("=" * 60)

    sentiment = MarketSentiment()
    # target_date（日本の前営業日）を渡して、対応する米国市場の終値を取得
    us_market = sentiment.get_us_market_close(date=target_date)

    if us_market is None:
        logger.warning("米国市場データ取得失敗 → 地合いチェックスキップ")
        market_status = 'normal'
    else:
        market_status = sentiment.check_market_sentiment(
            us_market['dow_change_pct'],
            us_market['nasdaq_change_pct'],
            threshold=-2.0
        )

    # 地合いによる候補絞り込み
    if market_status == 'skip_all':
        logger.error("地合い悪化のため全スキップします")
        logger.info("=" * 60)
        logger.info("朝スクリーニング終了（地合い悪化）")
        logger.info("=" * 60)
        return
    elif market_status == 'volume_only':
        logger.warning("地合いやや悪化: 出来高急増銘柄のみ対象（TDnet銘柄は除外）")
        # 現時点ではTDnet銘柄がないため、そのまま継続

    # STEP 3: ニュース取得（株探スクレイピング）
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: ニュース・開示内容取得")
    logger.info("=" * 60)

    news_scraper = NewsScraper()

    # 各銘柄のニュースと会社名を取得（上位10銘柄のみ、API制限対策）
    news_data = {}
    for idx, row in candidates.head(10).iterrows():
        code = str(row['Code'])

        # 会社名取得
        company_name = news_scraper.get_company_name(code)

        # ニュース取得
        news_text = news_scraper.get_stock_news(code, max_articles=3)

        news_data[code] = {
            'company_name': company_name,
            'news_text': news_text,
            'volume_surge_ratio': row['VolumeSurgeRatio']
        }

    logger.success(f"ニュース取得完了: {len(news_data)}銘柄")

    # STEP 4: Claude API材料判定
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: Claude API材料判定")
    logger.info("=" * 60)

    judge = MaterialJudge()
    judgments = {}

    for code, data in news_data.items():
        company_name = data['company_name']

        # ニューステキスト + 出来高急増情報を結合
        combined_text = f"出来高急増: {data['volume_surge_ratio']:.2f}倍\n\n{data['news_text']}"

        # Claude APIで判定
        judgment = judge.judge_material(code, company_name, combined_text)
        # 企業名を判定結果に追加
        if judgment:
            judgment['company_name'] = company_name
        judgments[code] = judgment

        # 結果表示
        if judge.should_exclude(judgment):
            logger.info(f"  → 除外: {code} {company_name}")
        else:
            logger.info(
                f"  → 採用: {code} {company_name} "
                f"[{judgment['material_type']}・{judgment['strength']}] "
                f"{judgment['summary']}"
            )

    # STEP 4.5: 前日ストップ高除外フィルタ
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4.5: 前日ストップ高除外フィルタ")
    logger.info("=" * 60)

    jquants = JQuantsClient()
    filtered_codes = []

    for idx, row in candidates.iterrows():
        code = str(row['Code'])

        # 前日ストップ高チェック
        try:
            # 過去7日分を取得して前日・前々日を抽出
            start_date = target_date - timedelta(days=7)
            df_prices = jquants.get_daily_quotes(code=code, date=start_date)

            if df_prices is not None and len(df_prices) >= 2:
                # 日付でソート（新しい順）
                df_prices['Date'] = pd.to_datetime(df_prices['Date'])
                df_prices = df_prices.sort_values('Date', ascending=False)

                # target_dateより前のデータのみ抽出
                df_prices = df_prices[df_prices['Date'] < target_date.strftime('%Y-%m-%d')]

                # 前日と前々日の終値を取得
                if len(df_prices) >= 2:
                    prev_close_1 = df_prices.iloc[0]['C']  # 前日終値
                    prev_close_2 = df_prices.iloc[1]['C']  # 前々日終値

                    if pd.notna(prev_close_1) and pd.notna(prev_close_2) and prev_close_2 > 0:
                        # 前日の上昇率を計算
                        prev_day_change_pct = ((prev_close_1 - prev_close_2) / prev_close_2) * 100

                        # +25%以上ならストップ高と判定
                        if prev_day_change_pct >= 25.0:
                            logger.info(f"  {code}: 前日ストップ高検出 (+{prev_day_change_pct:.1f}%) → 除外")
                            filtered_codes.append(code)

        except Exception as e:
            logger.debug(f"{code}: 前日データ取得エラー: {e}")
            # エラー時はフィルタしない（保守的）

    # フィルタ適用
    if len(filtered_codes) > 0:
        before_count = len(candidates)
        candidates = candidates[~candidates['Code'].astype(str).isin(filtered_codes)]
        logger.info(f"前日ストップ高除外: {len(filtered_codes)}件除外 (残り{len(candidates)}件)")
    else:
        logger.info("前日ストップ高銘柄なし")

    # STEP 5: 最終候補リスト出力
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: 最終候補リスト出力")
    logger.info("=" * 60)

    # 材料判定結果をDataFrameにマージ
    candidates['has_material'] = False
    candidates['material_strength'] = ''
    candidates['material_type'] = ''
    candidates['material_summary'] = ''

    for code, judgment in judgments.items():
        if judgment and not judge.should_exclude(judgment):
            # 材料あり（Claudeが採用と判定）
            mask = candidates['Code'].astype(str) == code
            candidates.loc[mask, 'has_material'] = True
            candidates.loc[mask, 'material_strength'] = judgment.get('strength', '')
            candidates.loc[mask, 'material_type'] = judgment.get('material_type', '')
            candidates.loc[mask, 'material_summary'] = judgment.get('summary', '')

    logger.info(f"材料判定済み銘柄: {candidates['has_material'].sum()}件")

    # CSV保存（当日の日付で保存）
    output_path = f"data/candidates_{now.strftime('%Y%m%d')}.csv"
    screener.save_candidates(candidates, filepath=output_path)

    logger.success(f"\n最終候補: {len(candidates)}銘柄")
    logger.success(f"保存先: {output_path}")

    # 上位5銘柄を表示
    logger.info("\n【上位5銘柄】")
    for idx, row in candidates.head(5).iterrows():
        logger.info(
            f"  {idx + 1}. {row['Code']}: "
            f"出来高急増{row['VolumeSurgeRatio']:.2f}倍, "
            f"終値{row['C']:.0f}円"
        )

    # STEP 6: Discord通知
    logger.info("\n" + "=" * 60)
    logger.info("STEP 6: Discord通知送信")
    logger.info("=" * 60)

    notifier = DiscordNotifier()
    notifier.send_morning_report(
        candidates_df=candidates,
        judgments=judgments,
        sentiment=us_market,
        tdnet_count=len(tdnet_codes),
        budget=budget
    )

    logger.info("\n" + "=" * 60)
    logger.info("朝スクリーニング完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"朝スクリーニング実行エラー: {e}")
        logger.exception("詳細なエラー情報:")

        # Discord通知
        try:
            notifier = DiscordNotifier()
            notifier.send_error(f"朝スクリーニング実行エラー: {str(e)[:100]}")
        except:
            pass  # 通知失敗しても続行

        raise
