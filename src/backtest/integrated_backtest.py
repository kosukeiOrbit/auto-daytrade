"""
統合バックテスト（スクリーニング → 判定 → トレード）
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from dateutil import tz
import pandas as pd
from loguru import logger

from ..utils.jquants_client import JQuantsClient
from ..screening import Screener
from ..analysis import check_entry
from .simulator import TradeSimulator
from .metrics import calculate_metrics, PerformanceMetrics


class IntegratedBacktest:
    """統合バックテストシステム"""

    def __init__(self, initial_capital: float = 500_000):
        """
        初期化

        Args:
            initial_capital: 初期資金（円）
        """
        self.client = JQuantsClient()
        self.simulator = TradeSimulator(initial_capital)
        logger.info(f"統合バックテストシステム初期化: 初期資金 {initial_capital:,}円")

    def run_historical_backtest(
        self,
        start_date: datetime,
        end_date: datetime,
        budget: float = 500_000,
        min_change_rate: float = 3.0,
        top_n: int = 20
    ) -> PerformanceMetrics:
        """
        過去データで統合バックテストを実行

        Args:
            start_date: 開始日
            end_date: 終了日
            budget: 予算（円）
            min_change_rate: 最低上昇率（%）
            top_n: 売買代金上位N銘柄

        Returns:
            PerformanceMetrics: パフォーマンス指標
        """
        logger.info("=" * 60)
        logger.info("統合バックテスト開始")
        logger.info(f"期間: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"条件: 上昇率{min_change_rate}%以上, 売買代金上位{top_n}銘柄, 予算{budget:,}円")
        logger.info("=" * 60)

        # スクリーナー初期化
        screener = Screener(budget=budget)

        # 日付リストを生成（営業日のみ）
        current_date = start_date
        trade_days = []

        while current_date <= end_date:
            # 土日をスキップ
            if current_date.weekday() < 5:  # 月曜=0, 金曜=4
                trade_days.append(current_date)
            current_date += timedelta(days=1)

        logger.info(f"対象日数: {len(trade_days)}日")

        # 各営業日でスクリーニング → エントリー判定
        for i, trade_date in enumerate(trade_days, 1):
            logger.info(f"\n[{i}/{len(trade_days)}] {trade_date.strftime('%Y-%m-%d')}")

            try:
                # 1. スクリーニング実行
                candidates = screener.get_candidates(
                    min_price_change_rate=min_change_rate,
                    top_n_by_value=top_n,
                    date=trade_date
                )

                if len(candidates) == 0:
                    logger.info("  候補銘柄なし")
                    continue

                logger.info(f"  候補銘柄: {len(candidates)}銘柄")

                # 2. 各候補銘柄でエントリー判定（簡易版：日次データのみ）
                # ※注：本来は5分足データが必要だが、今回は日次データで簡易判定
                for idx, row in candidates.head(3).iterrows():  # 上位3銘柄のみ
                    symbol = row['Code']
                    current_price = row['C']
                    prev_close = row['C'] - (row['C'] * row['ChangeRate'] / 100)

                    # 簡易的なOHLCVデータ作成（本来は5分足）
                    ohlcv = [{
                        "time": trade_date.strftime('%Y-%m-%d'),
                        "open": row['O'],
                        "high": row['H'],
                        "low": row['L'],
                        "close": row['C'],
                        "volume": row['Vo']
                    }]

                    # エントリー判定（簡易版）
                    # 注：本来は5分足データで判定するが、今回は日次データのみ
                    # VWAPタッチ判定などは省略

                    # 既にポジションがある場合はスキップ
                    if self.simulator.current_trade is not None:
                        continue

                    # 単純な条件でエントリー判定
                    if row['ChangeRate'] >= min_change_rate and row['ChangeRate'] <= 8.0:
                        # エントリー実行
                        entry_price = current_price
                        stop_loss = entry_price * 0.99  # -1%
                        take_profit = entry_price * 1.02  # +2%

                        success = self.simulator.open_trade(
                            symbol=symbol,
                            entry_time=trade_date,
                            entry_price=entry_price,
                            stop_loss=stop_loss,
                            take_profit=take_profit
                        )

                        if success:
                            logger.info(f"    エントリー: {symbol} @{entry_price:.0f}円")
                            break  # 1日1銘柄のみ

                # 3. 決済チェック（当日終値で判定）
                if self.simulator.current_trade is not None:
                    # 当日終値を取得
                    current_symbol = self.simulator.current_trade.symbol
                    exit_row = candidates[candidates['Code'] == current_symbol]

                    if len(exit_row) > 0:
                        exit_price = exit_row.iloc[0]['C']

                        # 決済判定
                        exited = self.simulator.check_exit(trade_date, exit_price)

                        if not exited:
                            # 当日終値で強制決済（日またぎ禁止）
                            self.simulator.force_close_all(trade_date, exit_price)

            except Exception as e:
                logger.error(f"  エラー: {e}")
                continue

        # 残ポジションがあれば強制決済
        if self.simulator.current_trade is not None:
            logger.warning("残ポジションを強制決済")
            self.simulator.force_close_all(end_date, self.simulator.current_trade.entry_price)

        # パフォーマンス計算
        logger.info("\n" + "=" * 60)
        logger.info("統合バックテスト完了")
        logger.info("=" * 60)

        metrics = calculate_metrics(
            self.simulator.trades,
            self.simulator.initial_capital,
            self.simulator.capital
        )

        return metrics
