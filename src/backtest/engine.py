"""
バックテストエンジン
"""
from typing import List, Dict
from datetime import datetime, timedelta
from loguru import logger
from .simulator import TradeSimulator
from .metrics import calculate_metrics, PerformanceMetrics


class BacktestEngine:
    """バックテストエンジン"""

    def __init__(self, initial_capital: float = 500_000):
        """
        初期化

        Args:
            initial_capital: 初期資金（円）
        """
        self.simulator = TradeSimulator(initial_capital)
        logger.info("バックテストエンジン初期化完了")

    def run_simple_backtest(self, test_scenarios: List[Dict]) -> PerformanceMetrics:
        """
        シンプルなバックテストを実行

        Args:
            test_scenarios: テストシナリオリスト
                [
                    {
                        "symbol": "1234",
                        "entry_time": datetime,
                        "entry_price": 1000,
                        "stop_loss": 990,
                        "take_profit": 1020,
                        "exit_price": 1020,  # 実際の終値
                        "exit_time": datetime
                    },
                    ...
                ]

        Returns:
            PerformanceMetrics: パフォーマンス指標
        """
        logger.info("=" * 60)
        logger.info("バックテスト開始")
        logger.info(f"シナリオ数: {len(test_scenarios)}")
        logger.info("=" * 60)

        for i, scenario in enumerate(test_scenarios, 1):
            logger.info(f"\n[シナリオ {i}/{len(test_scenarios)}]")

            # エントリー
            success = self.simulator.open_trade(
                symbol=scenario["symbol"],
                entry_time=scenario["entry_time"],
                entry_price=scenario["entry_price"],
                stop_loss=scenario["stop_loss"],
                take_profit=scenario["take_profit"]
            )

            if not success:
                logger.warning(f"エントリー失敗: {scenario['symbol']}")
                continue

            # 決済チェック
            self.simulator.check_exit(
                current_time=scenario["exit_time"],
                current_price=scenario["exit_price"]
            )

            # まだポジションが残っている場合は強制決済
            if self.simulator.current_trade is not None:
                self.simulator.force_close_all(
                    exit_time=scenario["exit_time"],
                    exit_price=scenario["exit_price"]
                )

        # パフォーマンス計算
        logger.info("\n" + "=" * 60)
        logger.info("バックテスト完了")
        logger.info("=" * 60)

        metrics = calculate_metrics(
            self.simulator.trades,
            self.simulator.initial_capital,
            self.simulator.capital
        )

        return metrics

    def run_historical_backtest(
        self,
        candidates_data: List[Dict],
        entry_logic,
        start_date: datetime,
        end_date: datetime
    ) -> PerformanceMetrics:
        """
        過去データを使った本格的なバックテスト

        Args:
            candidates_data: 候補銘柄データ（スクリーニング結果）
            entry_logic: エントリー判定関数
            start_date: 開始日
            end_date: 終了日

        Returns:
            PerformanceMetrics: パフォーマンス指標
        """
        logger.info("=" * 60)
        logger.info("履歴データバックテスト開始")
        logger.info(f"期間: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}")
        logger.info("=" * 60)

        # TODO: 実際の過去データを使ったバックテスト実装
        # フェーズ5でkabuステーションAPIと連携後に本格実装

        logger.warning("履歴データバックテストは未実装です")
        return calculate_metrics([], self.simulator.initial_capital, self.simulator.capital)
