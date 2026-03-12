"""
単一日バックテストテスト（API制限回避）
"""
from datetime import datetime
from dateutil import tz
from loguru import logger
from src.backtest import IntegratedBacktest, BacktestVisualizer


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("単一日バックテストテスト開始")
    logger.info("=" * 60)

    # 日本時間
    jst = tz.gettz("Asia/Tokyo")

    # テスト期間: 2026-03-05（エントリーが発生した日）
    test_date = datetime(2026, 3, 5, tzinfo=jst)

    logger.info(f"\nテスト日: {test_date.strftime('%Y-%m-%d')}")
    logger.info("注: 日足OHLCVを使った近似バックテストです（5分足データではありません）")

    # 統合バックテスト初期化
    backtest = IntegratedBacktest(initial_capital=500_000)

    # バックテスト実行
    try:
        metrics = backtest.run_historical_backtest(
            start_date=test_date,
            end_date=test_date,
            budget=500_000,
            min_change_rate=3.0,
            top_n=20
        )

        # 結果表示
        logger.info("\n")
        metrics.display()

        # トレード詳細表示
        if len(backtest.simulator.trades) > 0:
            logger.info("\n=== トレード詳細 ===")
            for i, trade in enumerate(backtest.simulator.trades, 1):
                logger.info(f"トレード {i}:")
                logger.info(f"  銘柄: {trade.symbol}")
                logger.info(f"  エントリー: {trade.entry_price:.0f}円 ({trade.entry_time.strftime('%Y-%m-%d')})")
                logger.info(f"  決済: {trade.exit_price:.0f}円 ({trade.exit_reason})")
                logger.info(f"  数量: {trade.quantity}株")
                logger.info(f"  損益: {trade.profit_loss:+,.0f}円 ({trade.profit_loss_rate:+.2f}%)")
                logger.info("")

        # 可視化実行
        if len(backtest.simulator.trades) > 0:
            logger.info("可視化処理実行中...")
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
