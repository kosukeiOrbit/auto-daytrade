"""
改善版バックテストテスト（短期間・API制限回避）
"""
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
from src.backtest import IntegratedBacktest, BacktestVisualizer


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("改善版バックテストテスト開始")
    logger.info("=" * 60)

    # 日本時間
    jst = tz.gettz("Asia/Tokyo")

    # テスト期間: 過去5営業日（API制限回避のため短縮）
    end_date = datetime.now(jst)
    start_date = end_date - timedelta(days=7)  # 7日前から（営業日5日程度）

    logger.info(f"\nテスト期間: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}")
    logger.info("注: 日足OHLCVを使った近似バックテストです（5分足データではありません）")

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
            logger.warning("✗ 一部の基準を満たしていません（日足データでの近似シミュレーションのため）")

        # 可視化実行
        if len(backtest.simulator.trades) > 0:
            logger.info("\n可視化処理実行中...")
            visualizer = BacktestVisualizer(
                trades=backtest.simulator.trades,
                metrics=metrics
            )

            # 全ての可視化を生成・保存
            visualizer.plot_all(output_dir="backtest_results_improved")

            logger.success("\n✓ 可視化完了！以下のファイルが生成されました:")
            logger.success("  - backtest_results_improved/equity_curve_*.png")
            logger.success("  - backtest_results_improved/trade_timeline_*.png")
            logger.success("  - backtest_results_improved/performance_summary_*.png")
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
