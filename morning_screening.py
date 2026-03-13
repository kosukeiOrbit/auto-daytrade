"""
朝6:30実行スクリーニング（出来高急増 + 地合いチェック + Claude材料判定）
"""
from datetime import datetime
from dateutil import tz
from loguru import logger
from src.screening import Screener
from src.utils.market_sentiment import MarketSentiment
from src.utils.material_judge import MaterialJudge
from src.utils.tdnet_scraper import TDnetScraper
from src.utils.news_scraper import NewsScraper


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("朝スクリーニング開始（morning_screening.py）")
    logger.info("=" * 60)

    # 日本時間
    jst = tz.gettz("Asia/Tokyo")
    today = datetime.now(jst)

    logger.info(f"実行日時: {today.strftime('%Y-%m-%d %H:%M:%S')}")

    # STEP 1a: 出来高急増銘柄を抽出
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1a: 出来高急増銘柄を抽出")
    logger.info("=" * 60)

    screener = Screener(budget=800_000)  # 予算80万円
    volume_candidates = screener.get_volume_surge_candidates(
        surge_threshold=2.0,  # 20日平均の2倍以上
        lookback_days=20,
        date=today
    )

    if len(volume_candidates) == 0:
        logger.warning("出来高急増銘柄が見つかりませんでした")
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
    tdnet_codes = tdnet.get_disclosure_codes(today)

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
    us_market = sentiment.get_us_market_close()

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

    # STEP 5: 最終候補リスト出力
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: 最終候補リスト出力")
    logger.info("=" * 60)

    # CSV保存
    output_path = f"data/candidates_{today.strftime('%Y%m%d')}.csv"
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

    logger.info("\n" + "=" * 60)
    logger.info("朝スクリーニング完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
