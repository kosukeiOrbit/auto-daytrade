"""
利確/損切パラメータスイープ（高速版）

事前キャッシュ（minute_bars_cache.pkl）を使い、
パラメータの組み合わせを数秒で検証する。

Usage:
    python scripts/precompute_minute_cache.py  # 初回のみ（約1分）
    python run_param_sweep.py                  # パラメータスイープ（数秒）
"""
import os
import sys
import glob
import pickle
import time
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger

logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level:<7} | {message}")

CACHE_PATH = "data/cache/minute_bars_cache.pkl"
INITIAL = 400_000


def precompute_trade_entries(minute_cache, prev_day_surge_pct=5.0):
    """
    パラメータに依存しないエントリー候補を事前計算（1回だけ実行）

    Returns:
        list of (date_str, code, entry_price, bars_key)
    """
    # 日足キャッシュ
    price_cache = {}
    for f in sorted(glob.glob("data/cache/prices_*.pkl")):
        with open(f, 'rb') as fh:
            df = pickle.load(fh)
            if len(df) > 0:
                ds = os.path.basename(f).replace('prices_', '').replace('.pkl', '')
                price_cache[ds] = df

    files = sorted(glob.glob("data/candidates_*.csv"))
    files = [f for f in files if 'test' not in f.lower()]

    entries = []

    for file_path in files:
        filename = os.path.basename(file_path)
        date_str = filename.replace("candidates_", "").replace(".csv", "")
        try:
            trade_date = datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            continue

        df = pd.read_csv(file_path, encoding='utf-8-sig')
        if len(df) == 0:
            continue

        # 材料フィルタ
        if 'material_strength' in df.columns:
            df = df[df['material_strength'].isin(['強', '中'])]
        if len(df) == 0:
            continue

        # 前日騰落率フィルタ
        prev_dates = []
        for d in range(1, 8):
            ck = (trade_date - timedelta(days=d)).strftime('%Y%m%d')
            if ck in price_cache:
                prev_dates.append(price_cache[ck])
            if len(prev_dates) >= 2:
                break

        exclude_codes = set()
        if len(prev_dates) >= 2:
            for _, row in df.iterrows():
                code_raw = str(row['Code'])
                try:
                    code_5 = code_raw if len(code_raw) >= 5 else code_raw + '0'
                except:
                    continue
                m1 = prev_dates[0][prev_dates[0]['Code'].astype(str) == code_5]
                m2 = prev_dates[1][prev_dates[1]['Code'].astype(str) == code_5]
                if len(m1) > 0 and len(m2) > 0:
                    c1 = m1.iloc[0]['C']; c2 = m2.iloc[0]['C']
                    if c2 > 0 and (c1 - c2) / c2 * 100 >= prev_day_surge_pct:
                        exclude_codes.add(str(row['Code']))
            if exclude_codes:
                df = df[~df['Code'].astype(str).isin(exclude_codes)]

        if len(df) == 0:
            continue

        # 銘柄選択
        top = None
        if 'material_strength' in df.columns and 'VolumeSurgeRatio' in df.columns:
            strong = df[df['material_strength'] == '強']
            if len(strong) > 0:
                top = strong.sort_values('VolumeSurgeRatio', ascending=False).iloc[0]
            else:
                medium = df[df['material_strength'] == '中']
                if len(medium) > 0:
                    top = medium.sort_values('VolumeSurgeRatio', ascending=False).iloc[0]
        if top is None and 'VolumeSurgeRatio' in df.columns:
            top = df.sort_values('VolumeSurgeRatio', ascending=False).iloc[0]
        if top is None:
            continue

        code_raw = str(top['Code'])
        try:
            code_int = int(code_raw)
            code = str(code_int // 10) if code_int >= 10000 else code_raw
        except ValueError:
            code = code_raw

        # キャッシュに分足があるか
        key = (date_str, code)
        if key not in minute_cache:
            continue

        bars = minute_cache[key]
        if bars is None or len(bars) == 0:
            continue

        # エントリー価格: candidatesのOpen優先、なければ分足始値
        entry_price = top.get('Open') or top.get('O')
        if entry_price is None or pd.isna(entry_price) or entry_price <= 0:
            entry_price = bars.iloc[0]['O']
        if entry_price <= 0 or entry_price * 100 > INITIAL:
            continue

        # ギャップアップチェック（+8%以内）
        prev_close = top.get('Close') or top.get('C')
        if prev_close and prev_close > 0:
            gap_pct = (entry_price / prev_close - 1) * 100
            if gap_pct > 8.0:
                continue

        entries.append({
            'date_str': date_str,
            'code': code,
            'entry_price': entry_price,
            'cache_key': key,
        })

    return entries


def sweep_params(entries, minute_cache, tp, sl):
    """1パラメータでバックテスト実行（高速：エントリー候補は事前計算済み）"""
    trades = []
    for e in entries:
        bars = minute_cache[e['cache_key']]
        entry_price = e['entry_price']

        tp_price = entry_price * (1 + tp / 100)
        sl_price = entry_price * (1 + sl / 100)
        mfe = 0.0; mae = 0.0
        exit_price = None; reason = None

        # numpy配列で高速走査
        h_arr = bars['H'].values
        l_arr = bars['L'].values
        c_arr = bars['C'].values
        t_arr = bars['Time'].values

        for i in range(len(bars)):
            h = h_arr[i]; l = l_arr[i]; c = c_arr[i]; t = t_arr[i]
            if entry_price > 0:
                mfe = max(mfe, (h - entry_price) / entry_price * 100)
                mae = min(mae, (l - entry_price) / entry_price * 100)

            if l <= sl_price:
                exit_price = sl_price; reason = '損切り'; break
            if h >= tp_price:
                exit_price = tp_price; reason = '利確'; break
            if t >= '11:30' and t < '12:30' and c < entry_price:
                exit_price = c; reason = '前場引け'; break

        if exit_price is None:
            exit_price = c_arr[-1]; reason = '大引け'

        qty = int(INITIAL // (entry_price * 100)) * 100
        if qty <= 0: qty = 100

        trades.append({
            'profit_loss': (exit_price - entry_price) * qty,
            'mfe_pct': mfe, 'mae_pct': mae, 'reason': reason,
        })

    return trades


def summarize(trades):
    if not trades:
        return None
    pnls = [tr['profit_loss'] for tr in trades]
    n = len(pnls); w = sum(1 for p in pnls if p > 0)
    total = sum(pnls)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    avg_w = sum(wins)/len(wins) if wins else 0
    avg_l = sum(losses)/len(losses) if losses else 0
    r = abs(avg_w / avg_l) if avg_l != 0 else 0
    cum = 0; peak = 0; dd = 0
    for p in pnls:
        cum += p
        if cum > peak: peak = cum
        if cum - peak < dd: dd = cum - peak
    mc = 0; c = 0
    for p in pnls:
        if p < 0: c += 1; mc = max(mc, c)
        else: c = 0
    mfe = sum(tr['mfe_pct'] for tr in trades) / len(trades)
    mae = sum(tr['mae_pct'] for tr in trades) / len(trades)
    reasons = {}
    for tr in trades:
        reasons[tr['reason']] = reasons.get(tr['reason'], 0) + 1
    return {
        'trades': n, 'wins': w, 'wr': w/n*100,
        'total_pnl': total, 'avg_win': avg_w, 'avg_loss': avg_l,
        'r_ratio': r, 'max_dd': dd, 'max_cl': mc,
        'avg_mfe': mfe, 'avg_mae': mae, 'reasons': reasons,
    }


def main():
    t0 = time.time()

    # キャッシュロード
    with open(CACHE_PATH, 'rb') as f:
        minute_cache = pickle.load(f)
    logger.info(f"分足キャッシュ: {len(minute_cache)}エントリ ({time.time()-t0:.1f}秒)")

    # エントリー候補事前計算（1回だけ）
    t1 = time.time()
    entries = precompute_trade_entries(minute_cache)
    logger.info(f"エントリー候補: {len(entries)}件 ({time.time()-t1:.1f}秒)")

    # パラメータグリッド
    tp_values = [1.5, 2.0, 2.5, 3.0, 3.5, 4.0]
    sl_values = [-0.5, -0.75, -1.0, -1.25, -1.5, -2.0]

    print()
    print("=" * 105)
    print("  利確/損切パラメータスイープ（分足・キャッシュ使用）")
    print("=" * 105)
    print()

    results = []
    t2 = time.time()

    for tp in tp_values:
        for sl in sl_values:
            trades = sweep_params(entries, minute_cache, tp, sl)
            s = summarize(trades)
            if s:
                results.append({'tp': tp, 'sl': sl, **s})

    elapsed = time.time() - t2
    print(f"全{len(results)}パターン完了: {elapsed:.1f}秒")
    print()

    header = f"{'TP':>5} {'SL':>6} {'回数':>5} {'勝率':>6} {'合計損益':>12} {'平均勝':>9} {'平均負':>9} {'R倍数':>6} {'最大DD':>10} {'連敗':>4} {'MFE':>6} {'MAE':>7}"
    print(header)
    print("-" * 105)

    for r in sorted(results, key=lambda x: -x['total_pnl']):
        print(
            f"{r['tp']:>+4.1f}% {r['sl']:>+5.2f}% "
            f"{r['trades']:>5} {r['wr']:>5.1f}% "
            f"{r['total_pnl']:>+11,.0f}円 "
            f"{r['avg_win']:>+8,.0f}円 {r['avg_loss']:>+8,.0f}円 "
            f"{r['r_ratio']:>5.2f} "
            f"{r['max_dd']:>+9,.0f}円 "
            f"{r['max_cl']:>4} "
            f"{r['avg_mfe']:>+5.2f}% {r['avg_mae']:>+6.2f}%"
        )

    best = max(results, key=lambda x: x['total_pnl'])
    print()
    print(f"最良: TP={best['tp']:+.1f}% SL={best['sl']:+.2f}% → 損益{best['total_pnl']:+,.0f}円 勝率{best['wr']:.1f}% DD{best['max_dd']:+,.0f}円")

    total_time = time.time() - t0
    print(f"総実行時間: {total_time:.1f}秒")


if __name__ == "__main__":
    main()
