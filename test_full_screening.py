"""
完全版スクリーニングテスト（TDnet + 株探 + Claude判定）
"""
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
from src.screening import Screener
from src.utils.market_sentiment import MarketSentiment
from src.utils.news_scraper import NewsScraper


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("完全版スクリーニングテスト")
    logger.info("=" * 60)

    jst = tz.gettz("Asia/Tokyo")
    # 過去の営業日でテスト（3月11日）
    test_date = datetime(2026, 3, 11, tzinfo=jst)

    logger.info(f"テスト日: {test_date.strftime('%Y-%m-%d')}")

    # STEP 1a: 出来高急増銘柄
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1a: 出来高急増銘柄")
    logger.info("=" * 60)

    screener = Screener(budget=800_000)
    candidates = screener.get_volume_surge_candidates(
        surge_threshold=2.0,
        lookback_days=20,
        date=test_date
    )

    if len(candidates) == 0:
        logger.warning("候補銘柄なし → テスト終了")
        return

    logger.success(f"出来高急増銘柄: {len(candidates)}銘柄")

    # STEP 2: 地合いチェック
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: 地合いチェック")
    logger.info("=" * 60)

    sentiment = MarketSentiment()
    us_market = sentiment.get_us_market_close(test_date)

    if us_market:
        market_status = sentiment.check_market_sentiment(
            us_market['dow_change_pct'],
            us_market['nasdaq_change_pct']
        )
        logger.info(f"地合い判定: {market_status}")
    else:
        logger.warning("米国市場データ取得失敗")

    # STEP 3: ニュース取得（上位3銘柄のみ、テストのため）
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: ニュース取得（上位3銘柄）")
    logger.info("=" * 60)

    news_scraper = NewsScraper()

    for idx, row in candidates.head(3).iterrows():
        code = str(row['Code'])

        # 会社名取得
        company_name = news_scraper.get_company_name(code)

        # ニュース取得
        news_text = news_scraper.get_stock_news(code, max_articles=2)

        logger.info(f"\n【{code} {company_name}】")
        logger.info(f"出来高急増率: {row['VolumeSurgeRatio']:.2f}倍")
        logger.info(f"終値: {row['C']:.0f}円")
        if news_text:
            logger.info(f"ニュース:\n{news_text[:200]}...")
        else:
            logger.info("ニュース: なし")

    logger.info("\n" + "=" * 60)
    logger.info("テスト完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
