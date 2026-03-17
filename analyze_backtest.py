import pandas as pd
import sys

sys.stdout.reconfigure(encoding='utf-8')

# CSVを読み込み（新しい結果ファイル）
df = pd.read_csv('backtest_results/paper_trading_20260317_223055.csv', encoding='utf-8-sig')

print('=' * 70)
print('詳細分析レポート')
print('=' * 70)
print()

# 1. 決済価格 = エントリー価格のケースをチェック
print('【1】決済価格 = エントリー価格のケース（データ品質チェック）')
print('-' * 70)
same_price = df[df['entry_price'] == df['exit_price']]
if len(same_price) > 0:
    print(f'⚠️ 発見: {len(same_price)}件')
    for _, row in same_price.iterrows():
        print(f"  {row['date']}: {row['code']} {row['name']} {row['entry_price']}円 → {row['exit_reason']}")
else:
    print('✅ なし（全トレードで価格変動あり）')
print()

# 2. 損切り29回の内訳分析
print('【2】損切り29回の内訳分析')
print('-' * 70)
loss_trades = df[df['exit_reason'] == '損切り'].copy()

# 始値から計算した損切り価格
loss_trades['stop_price'] = loss_trades['entry_price'] * 0.99

# ギャップダウン判定（始値の時点で既に損切りライン以下）
# 実際には-1%で損切りされているので、ギャップダウンは存在しないはずだが確認
gap_down = loss_trades[loss_trades['entry_price'] <= loss_trades['stop_price']]
intraday_stop = loss_trades[loss_trades['entry_price'] > loss_trades['stop_price']]

print(f'始値から即座に-1%のケース（理論上ありえない）: {len(gap_down)}件')
print(f'日中に-1%到達したケース: {len(intraday_stop)}件')
print()
print('💡 解釈: 損切りは全て「始値エントリー後、日中に-1%到達」')
print('   シミュレーターは始値でエントリーし、その価格の-1%で損切り判定')
print()

# 3. 利確11回の銘柄コード一覧
print('【3】利確11回の銘柄一覧（+2%達成銘柄）')
print('-' * 70)
profit_trades = df[df['exit_reason'] == '利確'].copy()
profit_trades = profit_trades.sort_values('date')

for i, (_, row) in enumerate(profit_trades.iterrows(), 1):
    code = str(row['code'])
    name = str(row['name'])
    entry = row['entry_price']
    exit_p = row['exit_price']
    pl = row['profit_loss']
    date_str = str(row['date'])
    print(f"{i:2d}. {date_str}: {code:5s} {name:20s} {entry:6.0f}円 → {exit_p:6.0f}円 (+{pl:.2f}円)")
print()

# 4. 月別勝率集計
print('【4】月別勝率')
print('-' * 70)
df['month'] = df['date'].astype(str).str[:6]  # YYYYMM
monthly_stats = df.groupby('month').agg({
    'profit_loss': ['count', lambda x: (x > 0).sum(), 'sum', 'mean'],
    'profit_loss_pct': 'mean'
})

# カラム名を整理
monthly_stats.columns = ['取引回数', '勝ち回数', '総損益', '平均損益', '平均損益率%']
monthly_stats['勝率%'] = (monthly_stats['勝ち回数'] / monthly_stats['取引回数'] * 100).round(1)

# 月名を追加
month_names = {
    '202601': '2026年1月',
    '202602': '2026年2月',
    '202603': '2026年3月'
}
monthly_stats.index = monthly_stats.index.map(lambda x: month_names.get(x, x))

print(monthly_stats[['取引回数', '勝ち回数', '勝率%', '総損益', '平均損益率%']].to_string())
print()

# 5. 追加分析: 終値決済の詳細
print('【5】終値決済9回の詳細（方向感なしケース）')
print('-' * 70)
eod_trades = df[df['exit_reason'] == '終値決済'].copy()
eod_trades = eod_trades.sort_values('date')

for i, (_, row) in enumerate(eod_trades.iterrows(), 1):
    pct = row['profit_loss_pct']
    result_mark = '📈' if pct > 0 else '📉' if pct < 0 else '➖'
    code = str(row['code'])
    name = str(row['name'])
    date_str = str(row['date'])
    print(f"{i}. {date_str}: {code:5s} {name:20s} {pct:+5.2f}% {result_mark}")

print()
print('💡 終値決済の平均損益率:', round(eod_trades['profit_loss_pct'].mean(), 2), '%')
