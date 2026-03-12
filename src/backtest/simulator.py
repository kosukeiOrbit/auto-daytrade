"""
トレードシミュレーター
"""
from typing import Dict, Optional
from dataclasses import dataclass
from datetime import datetime
from loguru import logger


@dataclass
class Trade:
    """トレード記録"""
    symbol: str
    entry_time: datetime
    entry_price: float
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    stop_loss: float = 0.0
    take_profit: float = 0.0
    quantity: int = 100
    status: str = "open"  # open, closed
    exit_reason: str = ""  # take_profit, stop_loss, time_exit, manual

    @property
    def profit_loss(self) -> float:
        """損益を計算"""
        if self.exit_price is None:
            return 0.0
        return (self.exit_price - self.entry_price) * self.quantity

    @property
    def profit_loss_rate(self) -> float:
        """損益率（%）を計算"""
        if self.exit_price is None:
            return 0.0
        return (self.exit_price - self.entry_price) / self.entry_price * 100

    @property
    def is_win(self) -> bool:
        """勝ちトレードかどうか"""
        return self.profit_loss > 0

    @property
    def is_loss(self) -> bool:
        """負けトレードかどうか"""
        return self.profit_loss < 0


class TradeSimulator:
    """トレードシミュレーター"""

    def __init__(self, initial_capital: float = 500_000):
        """
        初期化

        Args:
            initial_capital: 初期資金（円）
        """
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.trades = []
        self.current_trade: Optional[Trade] = None
        logger.info(f"トレードシミュレーター初期化: 初期資金 {initial_capital:,}円")

    def open_trade(
        self,
        symbol: str,
        entry_time: datetime,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        quantity: int = 100
    ) -> bool:
        """
        新規エントリー

        Args:
            symbol: 銘柄コード
            entry_time: エントリー時刻
            entry_price: エントリー価格
            stop_loss: 損切価格
            take_profit: 利確価格
            quantity: 株数

        Returns:
            bool: エントリー成功時True
        """
        # 既にポジションがある場合は失敗
        if self.current_trade is not None:
            logger.warning(f"既にポジションあり: {self.current_trade.symbol}")
            return False

        # 資金チェック
        required_capital = entry_price * quantity
        if required_capital > self.capital:
            logger.warning(f"資金不足: 必要 {required_capital:,}円 > 残高 {self.capital:,}円")
            return False

        # トレード開始
        self.current_trade = Trade(
            symbol=symbol,
            entry_time=entry_time,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            quantity=quantity,
            status="open"
        )

        self.capital -= required_capital
        logger.info(
            f"エントリー: {symbol} @{entry_price:.0f}円 x{quantity}株 "
            f"(SL: {stop_loss:.0f}, TP: {take_profit:.0f})"
        )
        return True

    def check_exit(self, current_time: datetime, current_price: float) -> bool:
        """
        決済条件をチェック

        Args:
            current_time: 現在時刻
            current_price: 現在価格

        Returns:
            bool: 決済実行時True
        """
        if self.current_trade is None:
            return False

        # 利確チェック
        if current_price >= self.current_trade.take_profit:
            self.close_trade(current_time, current_price, "take_profit")
            return True

        # 損切チェック
        if current_price <= self.current_trade.stop_loss:
            self.close_trade(current_time, current_price, "stop_loss")
            return True

        return False

    def close_trade(
        self,
        exit_time: datetime,
        exit_price: float,
        exit_reason: str = "manual"
    ):
        """
        ポジションをクローズ

        Args:
            exit_time: 決済時刻
            exit_price: 決済価格
            exit_reason: 決済理由
        """
        if self.current_trade is None:
            logger.warning("クローズ対象のトレードがありません")
            return

        # トレード終了
        self.current_trade.exit_time = exit_time
        self.current_trade.exit_price = exit_price
        self.current_trade.exit_reason = exit_reason
        self.current_trade.status = "closed"

        # 資金更新
        proceeds = exit_price * self.current_trade.quantity
        self.capital += proceeds

        profit_loss = self.current_trade.profit_loss
        profit_loss_rate = self.current_trade.profit_loss_rate

        logger.info(
            f"決済: {self.current_trade.symbol} @{exit_price:.0f}円 "
            f"({exit_reason}) | 損益: {profit_loss:+,.0f}円 ({profit_loss_rate:+.2f}%) "
            f"| 残高: {self.capital:,.0f}円"
        )

        # トレード履歴に追加
        self.trades.append(self.current_trade)
        self.current_trade = None

    def force_close_all(self, exit_time: datetime, exit_price: float):
        """
        全ポジションを強制決済

        Args:
            exit_time: 決済時刻
            exit_price: 決済価格
        """
        if self.current_trade is not None:
            self.close_trade(exit_time, exit_price, "time_exit")

    def get_total_profit_loss(self) -> float:
        """総損益を取得"""
        return sum(trade.profit_loss for trade in self.trades)

    def get_win_rate(self) -> float:
        """勝率を計算"""
        if len(self.trades) == 0:
            return 0.0
        wins = sum(1 for trade in self.trades if trade.is_win)
        return wins / len(self.trades) * 100

    def get_summary(self) -> Dict:
        """統計サマリーを取得"""
        total_trades = len(self.trades)
        if total_trades == 0:
            return {
                "total_trades": 0,
                "win_rate": 0.0,
                "total_profit_loss": 0.0,
                "final_capital": self.capital
            }

        wins = [t for t in self.trades if t.is_win]
        losses = [t for t in self.trades if t.is_loss]

        return {
            "total_trades": total_trades,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": self.get_win_rate(),
            "total_profit_loss": self.get_total_profit_loss(),
            "avg_profit": sum(t.profit_loss for t in wins) / len(wins) if wins else 0,
            "avg_loss": sum(t.profit_loss for t in losses) / len(losses) if losses else 0,
            "initial_capital": self.initial_capital,
            "final_capital": self.capital,
            "return_rate": (self.capital - self.initial_capital) / self.initial_capital * 100
        }
