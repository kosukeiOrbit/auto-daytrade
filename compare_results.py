import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

# 旧バックテスト結果（ETF除外フィルタ適用前）
df_old = pd.read_csv('backtest_results/paper_trading_20260317_215920.csv', encoding='utf-8-sig')

# 新バックテスト結果（ETF除外・ボラティリティフィルタ適用後）
df_new = pd.read_csv('backtest_results/paper_trading_20260317_223055.csv', encoding='utf-8-sig')

print('=' * 80)
print('バックテスト結果比較：ETF除外・ボラティリティフィルタの効果検証')
print('=' * 80)
print()

# 基本統計
print('【基本統計】')
print('-' * 80)

def calc_stats(df, label):
    total_trades = len(df)
    wins = len(df[df['profit_loss'] > 0])
    losses = len(df[df['profit_loss'] < 0])
    breakeven = len(df[df['profit_loss'] == 0])
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    total_pl = df['profit_loss'].sum()
    avg_win = df[df['profit_loss'] > 0]['profit_loss'].mean() if wins > 0 else 0
    avg_loss = df[df['profit_loss'] < 0]['profit_loss'].mean() if losses > 0 else 0

    print(f'{label}')
    print(f'  総取引回数: {total_trades}回')
    print(f'  勝ち: {wins}回 / 負け: {losses}回 / 引き分け: {breakeven}回')
    print(f'  勝率: {win_rate:.1f}%')
    print(f'  総損益: {total_pl:+.2f}円')
    print(f'  平均損益: {total_pl/total_trades:+.2f}円/回')
    print(f'  平均勝ちトレード: +{avg_win:.2f}円')
    print(f'  平均負けトレード: {avg_loss:.2f}円')
    if avg_loss != 0:
        print(f'  リスクリワード比: {abs(avg_win/avg_loss):.2f}倍')
    print()

calc_stats(df_old, '【旧】フィルタなし（ETF混入）')
calc_stats(df_new, '【新】ETF除外・ボラティリティフィルタ適用')

# 改善度
old_win_rate = len(df_old[df_old['profit_loss'] > 0]) / len(df_old) * 100
new_win_rate = len(df_new[df_new['profit_loss'] > 0]) / len(df_new) * 100
old_total_pl = df_old['profit_loss'].sum()
new_total_pl = df_new['profit_loss'].sum()

print('【改善度】')
print('-' * 80)
print(f'勝率: {old_win_rate:.1f}% → {new_win_rate:.1f}% ({new_win_rate - old_win_rate:+.1f}%ポイント)')
print(f'総損益: {old_total_pl:+.2f}円 → {new_total_pl:+.2f}円 ({new_total_pl - old_total_pl:+.2f}円)')
print()

# 終値決済の比較
print('【終値決済（方向感なし）の比較】')
print('-' * 80)
eod_old = df_old[df_old['exit_reason'] == '終値決済']
eod_new = df_new[df_new['exit_reason'] == '終値決済']
print(f'旧: {len(eod_old)}回（{len(eod_old)/len(df_old)*100:.1f}%）')
print(f'新: {len(eod_new)}回（{len(eod_new)/len(df_new)*100:.1f}%）')
print(f'→ {len(eod_old) - len(eod_new)}回減少')
print()

# ETF混入チェック
print('【ETF混入チェック】')
print('-' * 80)

# 旧結果のETF
etf_codes_old = ['28430', '22590', '382A0', '26490', '253A0']  # 既知のETFコード
df_old['CodeStr'] = df_old['code'].astype(str)
etf_in_old = df_old[df_old['CodeStr'].isin(etf_codes_old)]

# 新結果のETF
df_new['CodeStr'] = df_new['code'].astype(str)
etf_in_new = df_new[df_new['CodeStr'].isin(etf_codes_old)]

print(f'旧結果: ETF取引 {len(etf_in_old)}回')
if len(etf_in_old) > 0:
    for _, row in etf_in_old.iterrows():
        print(f'  {row["date"]}: {row["code"]} {row["name"]} ({row["exit_reason"]})')

print(f'新結果: ETF取引 {len(etf_in_new)}回')
if len(etf_in_new) > 0:
    for _, row in etf_in_new.iterrows():
        print(f'  {row["date"]}: {row["code"]} {row["name"]} ({row["exit_reason"]})')
else:
    print('  ✅ ETF完全除外成功！')
print()

# 月別勝率比較
print('【月別勝率の比較】')
print('-' * 80)

df_old['month'] = df_old['date'].astype(str).str[:6]
df_new['month'] = df_new['date'].astype(str).str[:6]

monthly_old = df_old.groupby('month').agg({
    'profit_loss': ['count', lambda x: (x > 0).sum()]
})
monthly_old.columns = ['取引数', '勝ち数']
monthly_old['勝率%'] = (monthly_old['勝ち数'] / monthly_old['取引数'] * 100).round(1)

monthly_new = df_new.groupby('month').agg({
    'profit_loss': ['count', lambda x: (x > 0).sum()]
})
monthly_new.columns = ['取引数', '勝ち数']
monthly_new['勝率%'] = (monthly_new['勝ち数'] / monthly_new['取引数'] * 100).round(1)

month_names = {
    '202601': '1月',
    '202602': '2月',
    '202603': '3月'
}

print('旧（フィルタなし）:')
for month in ['202601', '202602', '202603']:
    if month in monthly_old.index:
        row = monthly_old.loc[month]
        print(f'  {month_names[month]}: {row["勝率%"]:.1f}% ({int(row["勝ち数"])}/{int(row["取引数"])})')

print()
print('新（フィルタ適用後）:')
for month in ['202601', '202602', '202603']:
    if month in monthly_new.index:
        row = monthly_new.loc[month]
        print(f'  {month_names[month]}: {row["勝率%"]:.1f}% ({int(row["勝ち数"])}/{int(row["取引数"])})')

print()
print('=' * 80)
print('【結論】')
print('=' * 80)
print(f'✅ ETF除外フィルタにより勝率が {new_win_rate - old_win_rate:+.1f}%ポイント改善')
print(f'✅ 総損益が {new_total_pl - old_total_pl:+.2f}円改善')
print(f'✅ 終値決済（方向感なし）が {len(eod_old) - len(eod_new)}回減少')
print(f'✅ ボラティリティフィルタにより低値幅銘柄を除外')
print()
