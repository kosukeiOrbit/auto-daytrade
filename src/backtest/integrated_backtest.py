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

                # 2. 各候補銘柄でエントリー判定（日足ベースの近似バックテスト）
                # ※注意: これは日足OHLCVを使った「近似シミュレーション」です
                # 本物の5分足バックテストではありません。あくまで傾向把握用です。
                for idx, row in candidates.head(3).iterrows():  # 上位3銘柄のみ
                    symbol = row['Code']
                    open_price = row['O']  # 始値
                    high_price = row['H']  # 高値
                    low_price = row['L']   # 安値
                    close_price = row['C'] # 終値
                    prev_close = close_price - (close_price * row['ChangeRate'] / 100)

                    # 既にポジションがある場合はスキップ
                    if self.simulator.current_trade is not None:
                        continue

                    # エントリー条件チェック（日足ベース）

                    # 1. 寄り付きギャップアップが+8%超なら除外
                    gap_rate = (open_price - prev_close) / prev_close * 100
                    if gap_rate > 8.0:
                        continue

                    # 2. スクリーニング条件（前日比+3%〜+8%）を満たすか
                    if row['ChangeRate'] < min_change_rate or row['ChangeRate'] > 8.0:
                        continue

                    # 3. VWAP近似値チェック（日足ベース）
                    # 日足VWAP近似 = (H + L + C) / 3
                    vwap_approx = (high_price + low_price + close_price) / 3

                    # 始値がVWAP近似値を上回っているかチェック
                    if open_price < vwap_approx:
                        continue

                    # エントリー実行（始値でエントリー）
                    entry_price = open_price
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
                        logger.info(f"    エントリー: {symbol} @{entry_price:.0f}円 (始値)")

                        # 3. 決済判定（当日の高値・安値・終値で判定）
                        # 注: これは日足ベースの近似です。実際の時系列順序は考慮していません。

                        exit_price = None
                        exit_reason = None

                        # 利確判定: take_profit が当日高値以下なら利確成立
                        if take_profit <= high_price:
                            exit_price = take_profit
                            exit_reason = "take_profit"

                        # 損切判定: stop_loss が当日安値以上なら損切成立
                        elif stop_loss >= low_price:
                            exit_price = stop_loss
                            exit_reason = "stop_loss"

                        # どちらも発動しない場合: 終値で決済（日またぎ禁止）
                        else:
                            exit_price = close_price
                            exit_reason = "time_exit"

                        # 決済実行
                        self.simulator.close_trade(
                            exit_time=trade_date,
                            exit_price=exit_price,
                            exit_reason=exit_reason
                        )

                        logger.info(f"    決済: {symbol} @{exit_price:.0f}円 ({exit_reason})")

                        break  # 1日1銘柄のみ

            except Exception as e:
                logger.error(f"  エラー: {e}")
                continue

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
