"""
パターンB 分足バックテスト実行スクリプト

Usage:
    python run_pattern_b_backtest.py
"""
import sys
import pandas as pd
from loguru import logger
from src.backtest.pattern_b_backtest import PatternBBacktest

# ログ設定
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level:<7} | {message}")


def run():
    """パターンBバックテスト実行 + パターンAとの比較"""

    # --- パターンB バックテスト ---
    bt_b = PatternBBacktest(
        initial_capital=400_000,
        take_profit_pct=2.0,
        stop_loss_pct=-1.0,
        max_gap_from_open_pct=3.0,
        volume_surge_ratio=2.0,
        volume_lookback=20,
        trend_min_up=3,
        trend_window=5,
    )
    bt_b.run()
    b_summary = bt_b.get_summary()
    b_path = bt_b.save_results()

    if b_summary is None:
        logger.error("パターンB: トレードなし")
        return

    # --- パターンA 結果読み込み（最新の分足バックテスト） ---
    import glob
    a_files = sorted(glob.glob("backtest_results/minute_backtest_*.csv"))
    a_summary = None
    if a_files:
        df_a = pd.read_csv(a_files[-1], encoding='utf-8-sig')
        total = len(df_a)
        wins = (df_a['profit_loss'] > 0).sum()
        losses = (df_a['profit_loss'] < 0).sum()
        avg_win = df_a[df_a['profit_loss'] > 0]['profit_loss'].mean() if wins > 0 else 0
        avg_loss = df_a[df_a['profit_loss'] < 0]['profit_loss'].mean() if losses > 0 else 0
        cum = df_a['profit_loss'].cumsum()
        dd = (cum - cum.cummax()).min()
        mc = 0; c = 0
        for pl in df_a['profit_loss']:
            if pl < 0: c += 1; mc = max(mc, c)
            else: c = 0

        a_summary = {
            'trades': total,
            'wins': wins,
            'losses': losses,
            'win_rate': wins / total * 100,
            'total_pnl': df_a['profit_loss'].sum(),
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'r_ratio': abs(avg_win / avg_loss) if avg_loss != 0 else 0,
            'max_consecutive_loss': mc,
            'max_drawdown': dd,
            'avg_mfe': df_a['mfe_pct'].mean() if 'mfe_pct' in df_a.columns else 0,
            'avg_mae': df_a['mae_pct'].mean() if 'mae_pct' in df_a.columns else 0,
        }

    # --- 比較表出力 ---
    print()
    print("=" * 70)
    print("  パターンA vs パターンB 分足バックテスト比較")
    print("=" * 70)
    print()

    def fmt(v, f):
        if v is None:
            return "N/A"
        return f.format(v)

    rows = [
        ("トレード数",      "trades",               "{}回"),
        ("勝率",           "win_rate",              "{:.1f}%"),
        ("合計損益",        "total_pnl",             "{:+,.0f}円"),
        ("平均勝ち",        "avg_win",               "{:+,.0f}円"),
        ("平均負け",        "avg_loss",              "{:+,.0f}円"),
        ("R倍数",          "r_ratio",               "{:.2f}"),
        ("最大連敗",        "max_consecutive_loss",   "{}回"),
        ("最大DD",         "max_drawdown",           "{:+,.0f}円"),
        ("平均MFE",        "avg_mfe",               "{:+.2f}%"),
        ("平均MAE",        "avg_mae",               "{:+.2f}%"),
    ]

    header = f"{'':>16} {'パターンA(寄成)':>18} {'パターンB(場中)':>18}"
    print(header)
    print("-" * 55)

    for label, key, f in rows:
        a_val = fmt(a_summary.get(key) if a_summary else None, f)
        b_val = fmt(b_summary.get(key), f)
        print(f"{label:>16} {a_val:>18} {b_val:>18}")

    # パターンB固有指標
    print()
    print("--- パターンB固有指標 ---")
    print(f"  年利換算: {b_summary['annual_return']:+.1f}%")
    print(f"  期間: {b_summary['period_days']}日")

    # 決済理由内訳
    print()
    print("--- 決済理由内訳（パターンB） ---")
    for reason, count in sorted(b_summary['exit_reasons'].items(), key=lambda x: -x[1]):
        pct = count / b_summary['trades'] * 100
        print(f"  {reason}: {count}回 ({pct:.1f}%)")

    # エントリー時刻分布
    print()
    print("--- エントリー時刻分布（パターンB） ---")
    for time_bucket, count in sorted(b_summary['entry_time_dist'].items()):
        bar = "#" * count
        print(f"  {time_bucket}: {count:>3}回 {bar}")

    # MFE/MAE分布
    df_trades = pd.DataFrame(bt_b.trades)
    print()
    print("--- MFE分布（パターンB） ---")
    for pct in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        count = (df_trades['mfe_pct'] >= pct).sum()
        print(f"  +{pct}%以上到達: {count}回 ({count/len(df_trades)*100:.1f}%)")

    print()
    print("--- MAE分布（パターンB） ---")
    for pct in [-0.5, -1.0, -1.5, -2.0]:
        count = (df_trades['mae_pct'] <= pct).sum()
        print(f"  {pct}%以下到達: {count}回 ({count/len(df_trades)*100:.1f}%)")

    # 累積損益グラフ
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        fig, axes = plt.subplots(2, 1, figsize=(14, 10))

        # 累積損益
        dates_b = pd.to_datetime(df_trades['date'], format='%Y%m%d')
        cum_pnl_b = df_trades['profit_loss'].cumsum()

        ax1 = axes[0]
        ax1.plot(dates_b, cum_pnl_b, 'b-', linewidth=1.5, label='Pattern B')

        if a_files:
            df_a_plot = pd.read_csv(a_files[-1], encoding='utf-8-sig')
            dates_a = pd.to_datetime(df_a_plot['date'], format='%Y%m%d')
            cum_pnl_a = df_a_plot['profit_loss'].cumsum()
            ax1.plot(dates_a, cum_pnl_a, 'r-', linewidth=1.5, label='Pattern A', alpha=0.7)

        ax1.set_title('Cumulative P&L: Pattern A vs Pattern B', fontsize=14)
        ax1.set_ylabel('P&L (JPY)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.axhline(y=0, color='black', linewidth=0.5)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))

        # エントリー時刻ヒストグラム
        ax2 = axes[1]
        entry_minutes = df_trades['entry_time'].apply(
            lambda t: int(t.split(':')[0]) * 60 + int(t.split(':')[1]) if ':' in str(t) else 0
        )
        ax2.hist(entry_minutes, bins=range(570, 631, 5), color='steelblue', edgecolor='white')
        ax2.set_title('Pattern B: Entry Time Distribution', fontsize=14)
        ax2.set_xlabel('Time (minutes from midnight)')
        ax2.set_ylabel('Count')
        ticks = list(range(570, 631, 10))
        labels = [f"{m//60}:{m%60:02d}" for m in ticks]
        ax2.set_xticks(ticks)
        ax2.set_xticklabels(labels)
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        graph_path = f"backtest_results/pattern_b_backtest_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.png"
        plt.savefig(graph_path, dpi=150)
        plt.close()
        print(f"\nグラフ保存: {graph_path}")
    except Exception as e:
        logger.warning(f"グラフ生成失敗: {e}")

    print(f"\n結果CSV: {b_path}")


if __name__ == "__main__":
    run()
