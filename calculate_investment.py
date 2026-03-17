import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

# 新バックテスト結果を読み込み（240営業日）
df = pd.read_csv('backtest_results/paper_trading_20260317_235151.csv', encoding='utf-8-sig')

print('=' * 80)
print('50万円運用シミュレーション（ETF除外・ボラティリティフィルタ適用後）')
print('=' * 80)
print()

# 基本統計
total_trades = len(df)
wins = len(df[df['profit_loss'] > 0])
losses = len(df[df['profit_loss'] < 0])
win_rate = (wins / total_trades * 100) if total_trades > 0 else 0

print('【バックテスト基本統計】')
print('-' * 80)
print(f'期間: 2026年1月5日 ～ 3月17日（{total_trades}営業日）')
print(f'総取引回数: {total_trades}回（1日1銘柄）')
print(f'勝ち: {wins}回 / 負け: {losses}回')
print(f'勝率: {win_rate:.1f}%')
print()

# 1株あたりの平均損益を計算
avg_profit_per_share = df['profit_loss'].mean()
print(f'1株あたり平均損益: {avg_profit_per_share:+.2f}円/日')
print()

# 50万円運用の場合の計算
print('【50万円運用シミュレーション】')
print('-' * 80)

# 各トレードで50万円分の株数を買った場合
df['shares'] = (500000 / df['entry_price']).astype(int)  # 購入可能株数（整数）
df['actual_investment'] = df['shares'] * df['entry_price']  # 実際の投資金額
df['actual_profit'] = df['shares'] * df['profit_loss']  # 実際の損益

# 統計
total_investment_avg = df['actual_investment'].mean()
total_profit = df['actual_profit'].sum()
avg_profit_per_trade = df['actual_profit'].mean()
final_capital = 500000 + total_profit

print(f'初期資金: 500,000円')
print(f'平均投資金額/日: {total_investment_avg:,.0f}円')
print()

print('【運用結果】')
print('-' * 80)
print(f'総損益: {total_profit:+,.0f}円')
print(f'平均損益/日: {avg_profit_per_trade:+,.0f}円')
print(f'最終資産: {final_capital:,.0f}円')
print(f'リターン: {(total_profit/500000*100):+.2f}%（{total_trades}営業日）')
print()

# 月別集計
print('【月別損益】')
print('-' * 80)
df['month'] = df['date'].astype(str).str[:6]
monthly = df.groupby('month').agg({
    'actual_profit': ['sum', 'mean', 'count'],
    'profit_loss': lambda x: (x > 0).sum()
})
monthly.columns = ['月間損益', '平均損益/日', '取引日数', '勝ち日数']
monthly['勝率%'] = (monthly['勝ち日数'] / monthly['取引日数'] * 100).round(1)

month_names = {
    '202601': '2026年1月',
    '202602': '2026年2月',
    '202603': '2026年3月'
}
monthly.index = monthly.index.map(lambda x: month_names.get(x, x))

print(monthly.to_string())
print()

# 最大勝ち/負けトレード
print('【ベスト/ワーストトレード】')
print('-' * 80)

best_trade = df.loc[df['actual_profit'].idxmax()]
worst_trade = df.loc[df['actual_profit'].idxmin()]

print(f'最大勝ちトレード:')
print(f'  {best_trade["date"]}: {best_trade["code"]} {best_trade["name"]}')
print(f'  {int(best_trade["shares"])}株 × {best_trade["entry_price"]:.0f}円 → {best_trade["exit_price"]:.0f}円')
print(f'  損益: +{best_trade["actual_profit"]:,.0f}円 ({best_trade["profit_loss_pct"]:+.2f}%)')
print()

print(f'最大負けトレード:')
print(f'  {worst_trade["date"]}: {worst_trade["code"]} {worst_trade["name"]}')
print(f'  {int(worst_trade["shares"])}株 × {worst_trade["entry_price"]:.0f}円 → {worst_trade["exit_price"]:.0f}円')
print(f'  損益: {worst_trade["actual_profit"]:,.0f}円 ({worst_trade["profit_loss_pct"]:+.2f}%)')
print()

# 累積損益グラフ（簡易版）
print('【累積損益推移（簡易グラフ）】')
print('-' * 80)
df = df.sort_values('date')
df['cumulative_profit'] = df['actual_profit'].cumsum()

# 10営業日ごとにサンプリング
sample_indices = list(range(0, len(df), 5)) + [len(df)-1]  # 5営業日ごと + 最終日
for i in sample_indices:
    row = df.iloc[i]
    date_str = str(row['date'])
    cumulative = row['cumulative_profit']
    bar_length = int(cumulative / 2000)  # スケール調整
    bar = '█' * abs(bar_length) if bar_length != 0 else ''
    sign = '+' if cumulative >= 0 else '-'

    print(f'{date_str}: {sign}{abs(cumulative):>6,.0f}円 {bar}')

print()
print('=' * 80)
print('【まとめ】')
print('=' * 80)
print(f'✅ 50万円 → {final_capital:,.0f}円（{total_trades}営業日で {total_profit:+,.0f}円）')
print(f'✅ 勝率 {win_rate:.1f}%、平均 {avg_profit_per_trade:+,.0f}円/日')
print(f'✅ 年率換算: 約{(total_profit/500000)/(total_trades/240)*100:.1f}% (240営業日/年と仮定)')
print()
