"""
分足バックテスト実行スクリプト

Usage:
    python run_minute_backtest.py
"""
import sys
import pandas as pd
from loguru import logger
from src.backtest.minute_backtest import MinuteBacktest

# ログ設定
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level:<7} | {message}")


def run_comparison():
    """分足バックテスト実行 + 日足結果との比較"""

    # --- 分足バックテスト ---
    bt = MinuteBacktest(
        initial_capital=400_000,
        take_profit_pct=2.0,
        stop_loss_pct=-1.0,
        max_gap_pct=8.0,
        prev_day_surge_pct=5.0,
    )
    bt.run()
    minute_summary = bt.get_summary()
    result_path = bt.save_results()

    if minute_summary is None:
        logger.error("分足バックテスト: トレードなし")
        return

    # --- 日足バックテスト結果読み込み（最新） ---
    import glob
    daily_files = sorted(glob.glob("backtest_results/paper_trading_*.csv"))
    daily_summary = None
    if daily_files:
        df_daily = pd.read_csv(daily_files[-1], encoding='utf-8-sig')
        total = len(df_daily)
        wins = (df_daily['profit_loss'] > 0).sum()
        losses = (df_daily['profit_loss'] < 0).sum()
        avg_win = df_daily[df_daily['profit_loss'] > 0]['profit_loss'].mean() if wins > 0 else 0
        avg_loss = df_daily[df_daily['profit_loss'] < 0]['profit_loss'].mean() if losses > 0 else 0
        cum = df_daily['profit_loss'].cumsum()
        dd = (cum - cum.cummax()).min()
        mc = 0; c = 0
        for pl in df_daily['profit_loss']:
            if pl < 0: c += 1; mc = max(mc, c)
            else: c = 0

        daily_summary = {
            'trades': total,
            'wins': wins,
            'losses': losses,
            'win_rate': wins / total * 100,
            'total_pnl': df_daily['profit_loss'].sum(),
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'r_ratio': abs(avg_win / avg_loss) if avg_loss != 0 else 0,
            'max_consecutive_loss': mc,
            'max_drawdown': dd,
        }

    # --- 比較表出力 ---
    print()
    print("=" * 70)
    print("  バックテスト比較: 日足近似 vs 分足")
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
    ]

    header = f"{'':>16} {'日足近似':>18} {'分足':>18}"
    print(header)
    print("-" * 55)

    for label, key, f in rows:
        d_val = fmt(daily_summary.get(key) if daily_summary else None, f)
        m_val = fmt(minute_summary.get(key), f)
        print(f"{label:>16} {d_val:>18} {m_val:>18}")

    # 分足固有指標
    print()
    print("--- 分足バックテスト固有指標 ---")
    print(f"  平均MFE（最大含み益）: {minute_summary['avg_mfe']:+.2f}%")
    print(f"  平均MAE（最大含み損）: {minute_summary['avg_mae']:+.2f}%")
    print(f"  年利換算: {minute_summary['annual_return']:+.1f}%")
    print(f"  期間: {minute_summary['period_days']}日")
    print()

    # 決済理由内訳
    print("--- 決済理由内訳（分足） ---")
    for reason, count in sorted(minute_summary['exit_reasons'].items(), key=lambda x: -x[1]):
        pct = count / minute_summary['trades'] * 100
        print(f"  {reason}: {count}回 ({pct:.1f}%)")

    # MFE/MAE分布
    df_trades = pd.DataFrame(bt.trades)
    print()
    print("--- MFE分布（最大含み益） ---")
    for pct in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        count = (df_trades['mfe_pct'] >= pct).sum()
        print(f"  +{pct}%以上到達: {count}回 ({count/len(df_trades)*100:.1f}%)")

    print()
    print("--- MAE分布（最大含み損） ---")
    for pct in [-0.5, -1.0, -1.5, -2.0]:
        count = (df_trades['mae_pct'] <= pct).sum()
        print(f"  {pct}%以下到達: {count}回 ({count/len(df_trades)*100:.1f}%)")

    print()
    print(f"結果保存先: {result_path}")


if __name__ == "__main__":
    run_comparison()
