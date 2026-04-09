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

    # STEP 0: 買付余力取得（kabu Station API → 前日レポートJSON → 固定予算）
    logger.info("\n" + "=" * 60)
    logger.info("STEP 0: 買付余力取得")
    logger.info("=" * 60)

    available_cash = None
    try:
        kabu_client = KabuClient()
        wallet = kabu_client.get_wallet_cash()
        available_cash = wallet['stock_account_wallet']

        if available_cash is not None:
            budget = int(available_cash * Config.INVESTMENT_RATIO)
            logger.info(f"買付余力: {available_cash:,}円 × {Config.INVESTMENT_RATIO} = 本日の投資予算: {budget:,}円")
        else:
            logger.warning("検証環境（買付余力null）、フォールバックへ")

    except Exception as e:
        logger.warning(f"買付余力取得エラー: {e}")

    # フォールバック1: 前日レポートJSONから closing_wallet を取得
    if available_cash is None:
        import json
        for days_back in range(1, 8):
            report_date = (now - timedelta(days=days_back)).strftime('%Y%m%d')
            report_path = f"data/daily_report_{report_date}.json"
            if os.path.exists(report_path):
                try:
                    with open(report_path, 'r', encoding='utf-8') as f:
                        report = json.load(f)
                    closing_wallet = report.get('closing_wallet')
                    if closing_wallet is not None and closing_wallet > 0:
                        available_cash = closing_wallet
                        budget = int(available_cash * Config.INVESTMENT_RATIO)
                        logger.info(f"前日レポートから買付余力を取得: {closing_wallet:,}円（{report_date}）")
                        logger.info(f"投資予算: {budget:,}円")
                        break
                except Exception as e:
                    logger.debug(f"レポート読み込み失敗 {report_path}: {e}")

    # フォールバック2: 固定予算
    if available_cash is None:
        budget = 800_000
        logger.warning("前日レポートも取得できず、固定予算を使用: 800,000円")

    # スクリーニングフィルター用：1銘柄あたり上限（信用余力ベースに合わせる）
    filter_budget = 400_000

    # STEP 1a: 出来高急増銘柄を抽出
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1a: 出来高急増銘柄を抽出")
    logger.info("=" * 60)

    screener = Screener(budget=filter_budget)
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
            sentiment_message='未チェック（候補なし）',
            tdnet_count=0,
            budget=budget
        )

        logger.info("=" * 60)
        logger.info("朝スクリーニング終了（候補なし）")
        logger.info("=" * 60)
        return

    logger.success(f"出来高急増銘柄: {len(volume_candidates)}銘柄")

    # J-Quantsクライアント（TDnet追加や前日フィルタで使用）
    jquants = JQuantsClient()

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
    existing_codes = set(
        pd.to_numeric(candidates['Code'], errors='coerce')
        .dropna()
        .astype(int)
        .astype(str)
        .str.zfill(4)
    )
    normalized_tdnet_codes = [str(code).zfill(4) for code in tdnet_codes]
    new_tdnet_codes = [code for code in normalized_tdnet_codes if code not in existing_codes]

    if len(new_tdnet_codes) > 0:
        logger.info(f"TDnet銘柄のうち出来高急増リストに未含有: {len(new_tdnet_codes)}銘柄")
        logger.info(f"  コード: {', '.join(new_tdnet_codes[:10])}")

        # J-QuantsからOHLCVを取得してcandidatesに追加
        df_prices = jquants.get_daily_quotes(date=target_date)

        if df_prices is not None and len(df_prices) > 0:
            tdnet_additions = []
            for code in new_tdnet_codes:
                # J-Quants APIのCodeは多くが末尾0付き5桁
                code_for_api = code if len(code) == 5 else f"{code}0"
                match = df_prices[df_prices['Code'].astype(str) == code_for_api]

                if len(match) == 0:
                    logger.info(f"  {code}: 株価データなし（スキップ）")
                    continue

                row = match.iloc[0]

                # 予算フィルタ（1単元=100株が買える銘柄のみ）
                unit_price = row['C'] * 100
                if unit_price > filter_budget:
                    logger.info(f"  {code}: 予算超過（{unit_price:,.0f}円 > 上限{filter_budget:,}円）")
                    continue

                tdnet_additions.append({
                    'Code': int(code),
                    'Date': row.get('Date'),
                    'O': row.get('O'),
                    'H': row.get('H'),
                    'L': row.get('L'),
                    'C': row.get('C'),
                    'Vo': row.get('Vo'),
                    'AvgVolume': None,
                    'VolumeSurgeRatio': 0.0,  # 出来高急増ではないので0
                    'MA25': None,
                    'TradingValue': row.get('C', 0) * row.get('Vo', 0),
                    'AvgTradingValue5d': None,
                    'MarketCap': 0,
                })

            if len(tdnet_additions) > 0:
                df_tdnet = pd.DataFrame(tdnet_additions)
                candidates = pd.concat([candidates, df_tdnet], ignore_index=True)
                logger.success(f"TDnet銘柄 {len(tdnet_additions)}銘柄を候補に追加")
            else:
                logger.info("TDnet銘柄: フィルタ後に追加対象なし")
        else:
            logger.warning("TDnet銘柄の株価データ取得失敗")
    else:
        logger.info("TDnet銘柄: 全て出来高急増リストに含まれている、または開示なし")

    # STEP 2: 地合いチェック（日経先物 → NYダウ・ナスダック フォールバック）
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: 地合いチェック")
    logger.info("=" * 60)

    sentiment = MarketSentiment()

    # 日経先物を取得（メイン指標）
    nikkei_futures = sentiment.get_nikkei_futures()
    nikkei_change_pct = nikkei_futures['change_pct'] if nikkei_futures else None

    # NYダウ・ナスダックを取得（フォールバック用）
    us_market = sentiment.get_us_market_close(date=target_date)
    dow_change_pct = us_market['dow_change_pct'] if us_market else None
    nasdaq_change_pct = us_market['nasdaq_change_pct'] if us_market else None

    # 地合い判定
    sentiment_result = sentiment.check_market_sentiment(
        nikkei_change_pct=nikkei_change_pct,
        dow_change_pct=dow_change_pct,
        nasdaq_change_pct=nasdaq_change_pct,
        threshold=-1.5
    )
    market_status = sentiment_result['status']
    sentiment_message = sentiment_result['message']

    logger.info(f"地合い判定: {sentiment_message}")

    # 地合いによる候補絞り込み
    if market_status == 'skip_all':
        logger.error("地合い悪化のため全スキップします")

        # Discord通知（地合い悪化で終了）
        notifier = DiscordNotifier()
        notifier.send_morning_report(
            candidates_df=pd.DataFrame(),
            judgments={},
            sentiment_message=sentiment_message,
            tdnet_count=len(tdnet_codes),
            budget=budget
        )

        logger.info("=" * 60)
        logger.info("朝スクリーニング終了（地合い悪化）")
        logger.info("=" * 60)
        return

    # STEP 3: ニュース取得（株探スクレイピング）
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: ニュース・開示内容取得")
    logger.info("=" * 60)

    news_scraper = NewsScraper()

    # 各銘柄のニュースと会社名を取得（上位20銘柄、Claude APIコストとのバランス）
    news_data = {}
    for idx, row in candidates.head(20).iterrows():
        code = str(row['Code'])

        # 会社名取得
        company_name = news_scraper.get_company_name(code)

        # ニュース取得（鮮度フィルタ適用）
        # 実行日時（now）を基準として、前営業日15:30以降のニュースのみ取得
        news_text = news_scraper.get_stock_news(code, max_articles=3, reference_date=now)

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

    # STEP 4.6: TOB・MBO銘柄除外フィルター
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4.6: TOB・MBO銘柄除外フィルター")
    logger.info("=" * 60)

    tob_keywords = ['TOB', 'MBO', '公開買付', '株式交換', '完全子会社化', '非公開化']
    tob_excluded = []
    for code, judgment in list(judgments.items()):
        if not judgment:
            continue
        summary = str(judgment.get('summary', ''))
        material_type = str(judgment.get('material_type', ''))
        if any(kw in summary or kw in material_type for kw in tob_keywords):
            tob_excluded.append(code)
            del judgments[code]
            logger.info(f"  TOB・MBO銘柄除外: {code} {judgment.get('company_name', '')} {summary}")

    if tob_excluded:
        logger.info(f"TOB・MBO銘柄を{len(tob_excluded)}件除外しました")
    else:
        logger.info("TOB・MBO銘柄なし")

    # STEP 4.5: 前日ストップ高除外フィルタ
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4.5: 前日ストップ高除外フィルタ")
    logger.info("=" * 60)

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
                            logger.info(f"  {code}: 前日ストップ高除外 ({prev_day_change_pct:.1f}% >= 25%)")
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
        sentiment_message=sentiment_message,
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
        except Exception:
            pass  # 通知失敗しても続行

        raise
