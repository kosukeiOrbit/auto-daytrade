"""
バックテスト可視化テスト
"""
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
from src.backtest import IntegratedBacktest, BacktestVisualizer


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("バックテスト可視化テスト開始")
    logger.info("=" * 60)

    # 日本時間
    jst = tz.gettz("Asia/Tokyo")

    # テスト期間: 過去1週間
    end_date = datetime.now(jst)
    start_date = end_date - timedelta(days=10)

    logger.info(f"\nテスト期間: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}")

    # 統合バックテスト実行
    backtest = IntegratedBacktest(initial_capital=500_000)

    try:
        logger.info("\n[1] バックテスト実行中...")
        metrics = backtest.run_historical_backtest(
            start_date=start_date,
            end_date=end_date,
            budget=500_000,
            min_change_rate=3.0,
            top_n=20
        )

        # パフォーマンス表示
        logger.info("\n")
        metrics.display()

        # 可視化実行
        if len(backtest.simulator.trades) > 0:
            logger.info("\n[2] 可視化処理実行中...")
            visualizer = BacktestVisualizer(
                trades=backtest.simulator.trades,
                metrics=metrics
            )

            # 全ての可視化を生成・保存
            visualizer.plot_all(output_dir="backtest_results")

            logger.success("\n✓ 可視化完了！以下のファイルが生成されました:")
            logger.success("  - backtest_results/equity_curve_*.png")
            logger.success("  - backtest_results/trade_timeline_*.png")
            logger.success("  - backtest_results/performance_summary_*.png")
        else:
            logger.warning("\nトレードデータが空のため、可視化をスキップしました")

    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        import traceback
        logger.error(traceback.format_exc())

    logger.info("\n" + "=" * 60)
    logger.info("テスト完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
