"""
統合バックテストテスト（実データ使用）
"""
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
from src.backtest import IntegratedBacktest


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("統合バックテストテスト開始")
    logger.info("=" * 60)

    # 日本時間
    jst = tz.gettz("Asia/Tokyo")

    # テスト期間: 過去1週間（営業日のみ）
    end_date = datetime.now(jst)
    start_date = end_date - timedelta(days=10)  # 10日前から（営業日5日程度）

    logger.info(f"\nテスト期間: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}")
    logger.info("注: 実際のJ-Quants APIから過去データを取得してバックテストを実行します")

    # 統合バックテスト初期化
    backtest = IntegratedBacktest(initial_capital=500_000)

    # バックテスト実行
    try:
        metrics = backtest.run_historical_backtest(
            start_date=start_date,
            end_date=end_date,
            budget=500_000,
            min_change_rate=3.0,
            top_n=20
        )

        # 結果表示
        logger.info("\n")
        metrics.display()

        # 合格基準チェック
        logger.info("\n")
        passed = metrics.passes_criteria()

        if passed:
            logger.success("✓ 全ての合格基準をクリアしました！")
        else:
            logger.warning("✗ 一部の基準を満たしていません（実データなので基準未達は正常）")

    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        import traceback
        logger.error(traceback.format_exc())

    logger.info("\n" + "=" * 60)
    logger.info("テスト完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
