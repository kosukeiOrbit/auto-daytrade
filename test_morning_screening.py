"""
morning_screening.py の動作確認テスト
"""
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
from src.screening import Screener
from src.utils.market_sentiment import MarketSentiment


def test_volume_surge():
    """出来高急増フィルタのテスト"""
    logger.info("=" * 60)
    logger.info("【テスト1】出来高急増フィルタ")
    logger.info("=" * 60)

    jst = tz.gettz("Asia/Tokyo")
    # 過去の営業日でテスト（3月12日）
    test_date = datetime(2026, 3, 12, tzinfo=jst)

    screener = Screener(budget=800_000)
    candidates = screener.get_volume_surge_candidates(
        surge_threshold=2.0,
        lookback_days=20,
        date=test_date
    )

    logger.success(f"\n✓ 出来高急増銘柄: {len(candidates)}銘柄")

    if len(candidates) > 0:
        logger.info("\n【上位5銘柄】")
        for idx, row in candidates.head(5).iterrows():
            logger.info(
                f"  {idx + 1}. {row['Code']}: "
                f"出来高急増{row['VolumeSurgeRatio']:.2f}倍, "
                f"終値{row['C']:.0f}円, "
                f"25MA{row['MA25']:.0f}円"
            )


def test_us_market():
    """米国市場終値取得テスト"""
    logger.info("\n" + "=" * 60)
    logger.info("【テスト2】米国市場終値取得")
    logger.info("=" * 60)

    sentiment = MarketSentiment()
    us_market = sentiment.get_us_market_close()

    if us_market:
        logger.success(f"\n✓ 米国市場データ取得成功")
        logger.info(f"  日付: {us_market['date']}")
        logger.info(f"  NYダウ: {us_market['dow_close']:.2f} ({us_market['dow_change_pct']:+.2f}%)")
        logger.info(f"  ナスダック: {us_market['nasdaq_close']:.2f} ({us_market['nasdaq_change_pct']:+.2f}%)")

        # 地合いチェック
        market_status = sentiment.check_market_sentiment(
            us_market['dow_change_pct'],
            us_market['nasdaq_change_pct']
        )
        logger.info(f"  地合い判定: {market_status}")
    else:
        logger.error("✗ 米国市場データ取得失敗")


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("morning_screening.py 動作確認テスト")
    logger.info("=" * 60)

    try:
        # テスト1: 出来高急増フィルタ
        test_volume_surge()

        # テスト2: 米国市場終値取得
        test_us_market()

        logger.info("\n" + "=" * 60)
        logger.success("全テスト完了")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"テスト中にエラーが発生しました: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
