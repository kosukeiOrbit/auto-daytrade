"""
パフォーマンス指標計算
"""
from typing import List
from dataclasses import dataclass
from loguru import logger
from .simulator import Trade


@dataclass
class PerformanceMetrics:
    """パフォーマンス指標"""
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    total_profit_loss: float
    avg_profit: float
    avg_loss: float
    profit_factor: float  # 総利益 / 総損失
    r_multiple: float  # 平均利益 / |平均損失|
    max_consecutive_wins: int
    max_consecutive_losses: int
    max_drawdown: float
    max_drawdown_rate: float
    initial_capital: float
    final_capital: float
    return_rate: float

    def passes_criteria(self) -> bool:
        """
        設計書の合格基準を満たすかチェック

        合格基準:
        - 勝率: 55%以上
        - 平均R倍数: 1.5以上
        - 最大連敗: 5連敗以内
        - 最大ドローダウン: -10%以内
        """
        criteria = {
            "勝率55%以上": self.win_rate >= 55.0,
            "R倍数1.5以上": self.r_multiple >= 1.5,
            "最大連敗5以内": self.max_consecutive_losses <= 5,
            "最大DD-10%以内": self.max_drawdown_rate >= -10.0
        }

        logger.info("=" * 60)
        logger.info("バックテスト合格基準チェック")
        for criterion, passed in criteria.items():
            status = "✓" if passed else "✗"
            logger.info(f"  {status} {criterion}")
        logger.info("=" * 60)

        return all(criteria.values())

    def display(self):
        """指標を表示"""
        logger.info("=" * 60)
        logger.info("パフォーマンスサマリー")
        logger.info("=" * 60)
        logger.info(f"総トレード数: {self.total_trades}")
        logger.info(f"勝ち: {self.wins} / 負け: {self.losses}")
        logger.info(f"勝率: {self.win_rate:.2f}%")
        logger.info(f"総損益: {self.total_profit_loss:+,.0f}円")
        logger.info(f"平均利益: {self.avg_profit:+,.0f}円")
        logger.info(f"平均損失: {self.avg_loss:+,.0f}円")
        logger.info(f"プロフィットファクター: {self.profit_factor:.2f}")
        logger.info(f"平均R倍数: {self.r_multiple:.2f}")
        logger.info(f"最大連勝: {self.max_consecutive_wins}")
        logger.info(f"最大連敗: {self.max_consecutive_losses}")
        logger.info(f"最大ドローダウン: {self.max_drawdown:,.0f}円 ({self.max_drawdown_rate:.2f}%)")
        logger.info(f"初期資金: {self.initial_capital:,.0f}円")
        logger.info(f"最終資金: {self.final_capital:,.0f}円")
        logger.info(f"リターン: {self.return_rate:+.2f}%")
        logger.info("=" * 60)


def calculate_metrics(trades: List[Trade], initial_capital: float, final_capital: float) -> PerformanceMetrics:
    """
    パフォーマンス指標を計算

    Args:
        trades: トレードリスト
        initial_capital: 初期資金
        final_capital: 最終資金

    Returns:
        PerformanceMetrics: パフォーマンス指標
    """
    if len(trades) == 0:
        logger.warning("トレードデータが空です")
        return PerformanceMetrics(
            total_trades=0,
            wins=0,
            losses=0,
            win_rate=0.0,
            total_profit_loss=0.0,
            avg_profit=0.0,
            avg_loss=0.0,
            profit_factor=0.0,
            r_multiple=0.0,
            max_consecutive_wins=0,
            max_consecutive_losses=0,
            max_drawdown=0.0,
            max_drawdown_rate=0.0,
            initial_capital=initial_capital,
            final_capital=final_capital,
            return_rate=0.0
        )

    # 基本統計
    total_trades = len(trades)
    wins = [t for t in trades if t.is_win]
    losses = [t for t in trades if t.is_loss]
    win_count = len(wins)
    loss_count = len(losses)

    win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0.0
    total_profit_loss = sum(t.profit_loss for t in trades)
    avg_profit = sum(t.profit_loss for t in wins) / win_count if win_count > 0 else 0.0
    avg_loss = sum(t.profit_loss for t in losses) / loss_count if loss_count > 0 else 0.0

    # プロフィットファクター
    total_profit = sum(t.profit_loss for t in wins)
    total_loss = abs(sum(t.profit_loss for t in losses))
    profit_factor = total_profit / total_loss if total_loss > 0 else 0.0

    # R倍数（平均利益 / |平均損失|）
    r_multiple = avg_profit / abs(avg_loss) if avg_loss != 0 else 0.0

    # 連勝・連敗
    max_consecutive_wins = 0
    max_consecutive_losses = 0
    current_win_streak = 0
    current_loss_streak = 0

    for trade in trades:
        if trade.is_win:
            current_win_streak += 1
            current_loss_streak = 0
            max_consecutive_wins = max(max_consecutive_wins, current_win_streak)
        elif trade.is_loss:
            current_loss_streak += 1
            current_win_streak = 0
            max_consecutive_losses = max(max_consecutive_losses, current_loss_streak)

    # 最大ドローダウン
    capital = initial_capital
    peak_capital = initial_capital
    max_drawdown = 0.0
    max_drawdown_rate = 0.0

    for trade in trades:
        capital += trade.profit_loss
        if capital > peak_capital:
            peak_capital = capital
        drawdown = peak_capital - capital
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            max_drawdown_rate = (drawdown / peak_capital) * -100

    return_rate = (final_capital - initial_capital) / initial_capital * 100

    return PerformanceMetrics(
        total_trades=total_trades,
        wins=win_count,
        losses=loss_count,
        win_rate=win_rate,
        total_profit_loss=total_profit_loss,
        avg_profit=avg_profit,
        avg_loss=avg_loss,
        profit_factor=profit_factor,
        r_multiple=r_multiple,
        max_consecutive_wins=max_consecutive_wins,
        max_consecutive_losses=max_consecutive_losses,
        max_drawdown=max_drawdown,
        max_drawdown_rate=max_drawdown_rate,
        initial_capital=initial_capital,
        final_capital=final_capital,
        return_rate=return_rate
    )
