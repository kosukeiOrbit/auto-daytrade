"""
バックテスト結果の可視化
"""
from typing import List
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from loguru import logger
from .simulator import Trade
from .metrics import PerformanceMetrics


class BacktestVisualizer:
    """バックテスト結果の可視化クラス"""

    def __init__(self, trades: List[Trade], metrics: PerformanceMetrics):
        """
        初期化

        Args:
            trades: トレードリスト
            metrics: パフォーマンス指標
        """
        self.trades = trades
        self.metrics = metrics

        # 日本語フォント設定（Windows環境）
        plt.rcParams['font.family'] = 'MS Gothic'
        plt.rcParams['axes.unicode_minus'] = False

    def plot_equity_curve(self, save_path: str = None):
        """
        資金曲線をプロット

        Args:
            save_path: 保存先パス（指定時はファイル保存、Noneの場合は表示のみ）
        """
        if len(self.trades) == 0:
            logger.warning("トレードデータが空のため、資金曲線をプロットできません")
            return

        # 資金曲線データを計算
        capital = self.metrics.initial_capital
        equity_curve = [capital]
        dates = [self.trades[0].entry_time]

        for trade in self.trades:
            capital += trade.profit_loss
            equity_curve.append(capital)
            dates.append(trade.exit_time)

        # プロット
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(dates, equity_curve, linewidth=2, color='#2E86AB', marker='o', markersize=4)
        ax.axhline(y=self.metrics.initial_capital, color='gray', linestyle='--', alpha=0.7, label='初期資金')

        # タイトルと軸ラベル
        ax.set_title('資金曲線（Equity Curve）', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('日時', fontsize=12)
        ax.set_ylabel('資金（円）', fontsize=12)

        # 日付フォーマット
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        plt.xticks(rotation=45)

        # グリッド
        ax.grid(True, alpha=0.3)
        ax.legend()

        # レイアウト調整
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            logger.info(f"資金曲線を保存: {save_path}")
        else:
            plt.show()

        plt.close()

    def plot_trade_timeline(self, save_path: str = None):
        """
        トレード履歴タイムラインをプロット

        Args:
            save_path: 保存先パス（指定時はファイル保存、Noneの場合は表示のみ）
        """
        if len(self.trades) == 0:
            logger.warning("トレードデータが空のため、トレードタイムラインをプロットできません")
            return

        # データ準備
        trade_numbers = list(range(1, len(self.trades) + 1))
        profit_losses = [trade.profit_loss for trade in self.trades]
        colors = ['green' if trade.is_win else 'red' for trade in self.trades]

        # プロット
        fig, ax = plt.subplots(figsize=(14, 6))
        bars = ax.bar(trade_numbers, profit_losses, color=colors, alpha=0.7, edgecolor='black')

        # ゼロラインを強調
        ax.axhline(y=0, color='black', linewidth=1.5)

        # タイトルと軸ラベル
        ax.set_title('トレード履歴タイムライン', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('トレード番号', fontsize=12)
        ax.set_ylabel('損益（円）', fontsize=12)

        # グリッド
        ax.grid(True, alpha=0.3, axis='y')

        # 凡例
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='green', alpha=0.7, label='勝ちトレード'),
            Patch(facecolor='red', alpha=0.7, label='負けトレード')
        ]
        ax.legend(handles=legend_elements)

        # レイアウト調整
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            logger.info(f"トレードタイムラインを保存: {save_path}")
        else:
            plt.show()

        plt.close()

    def plot_performance_summary(self, save_path: str = None):
        """
        パフォーマンス指標サマリーをプロット

        Args:
            save_path: 保存先パス（指定時はファイル保存、Noneの場合は表示のみ）
        """
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        # 1. 勝率 vs 合格基準
        ax1 = axes[0, 0]
        categories = ['勝率', '合格基準']
        values = [self.metrics.win_rate, 55.0]
        colors_bar = ['#2E86AB' if self.metrics.win_rate >= 55.0 else '#A23B72', '#6C757D']
        ax1.bar(categories, values, color=colors_bar, alpha=0.7, edgecolor='black')
        ax1.set_ylabel('勝率（%）', fontsize=12)
        ax1.set_title('勝率 vs 合格基準（55%以上）', fontsize=14, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='y')
        for i, v in enumerate(values):
            ax1.text(i, v + 2, f'{v:.1f}%', ha='center', fontweight='bold')

        # 2. R倍数 vs 合格基準
        ax2 = axes[0, 1]
        categories = ['R倍数', '合格基準']
        values = [self.metrics.r_multiple, 1.5]
        colors_bar = ['#2E86AB' if self.metrics.r_multiple >= 1.5 else '#A23B72', '#6C757D']
        ax2.bar(categories, values, color=colors_bar, alpha=0.7, edgecolor='black')
        ax2.set_ylabel('R倍数', fontsize=12)
        ax2.set_title('平均R倍数 vs 合格基準（1.5以上）', fontsize=14, fontweight='bold')
        ax2.grid(True, alpha=0.3, axis='y')
        for i, v in enumerate(values):
            ax2.text(i, v + 0.1, f'{v:.2f}', ha='center', fontweight='bold')

        # 3. 最大ドローダウン vs 合格基準
        ax3 = axes[1, 0]
        categories = ['最大DD', '合格基準']
        values = [self.metrics.max_drawdown_rate, -10.0]
        colors_bar = ['#2E86AB' if self.metrics.max_drawdown_rate >= -10.0 else '#A23B72', '#6C757D']
        ax3.bar(categories, values, color=colors_bar, alpha=0.7, edgecolor='black')
        ax3.set_ylabel('ドローダウン（%）', fontsize=12)
        ax3.set_title('最大ドローダウン vs 合格基準（-10%以内）', fontsize=14, fontweight='bold')
        ax3.axhline(y=-10.0, color='red', linestyle='--', alpha=0.7, label='合格ライン')
        ax3.grid(True, alpha=0.3, axis='y')
        for i, v in enumerate(values):
            ax3.text(i, v - 0.5, f'{v:.2f}%', ha='center', fontweight='bold')

        # 4. 連勝・連敗
        ax4 = axes[1, 1]
        categories = ['最大連勝', '最大連敗', '連敗基準']
        values = [self.metrics.max_consecutive_wins, self.metrics.max_consecutive_losses, 5]
        colors_bar = ['#2E86AB', '#A23B72' if self.metrics.max_consecutive_losses > 5 else '#2E86AB', '#6C757D']
        ax4.bar(categories, values, color=colors_bar, alpha=0.7, edgecolor='black')
        ax4.set_ylabel('回数', fontsize=12)
        ax4.set_title('連勝・連敗 vs 合格基準（5連敗以内）', fontsize=14, fontweight='bold')
        ax4.grid(True, alpha=0.3, axis='y')
        for i, v in enumerate(values):
            ax4.text(i, v + 0.2, f'{int(v)}', ha='center', fontweight='bold')

        # 全体タイトル
        fig.suptitle('パフォーマンス指標サマリー', fontsize=18, fontweight='bold', y=0.995)

        # レイアウト調整
        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            logger.info(f"パフォーマンスサマリーを保存: {save_path}")
        else:
            plt.show()

        plt.close()

    def plot_all(self, output_dir: str = "backtest_results"):
        """
        全ての可視化を一括生成してファイル保存

        Args:
            output_dir: 保存先ディレクトリ
        """
        import os
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        logger.info("=== バックテスト結果の可視化 ===")
        self.plot_equity_curve(save_path=f"{output_dir}/equity_curve_{timestamp}.png")
        self.plot_trade_timeline(save_path=f"{output_dir}/trade_timeline_{timestamp}.png")
        self.plot_performance_summary(save_path=f"{output_dir}/performance_summary_{timestamp}.png")
        logger.info(f"全ての可視化を保存しました: {output_dir}/")
