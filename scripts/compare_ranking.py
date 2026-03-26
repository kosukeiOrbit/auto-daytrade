"""
ランキングtype=6（売買高急増）vs type=14（売買代金急増）比較スクリプト
usage: python scripts/compare_ranking.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.kabu_client import KabuClient
from src.utils.config import Config

ETF_KEYWORDS = ['ETF', 'ETN', 'REIT', 'リート', 'ファンド', '日経', 'TOPIX', 'ダウ', 'S&P', 'ナスダック']

def is_etf(symbol, name):
    if symbol.startswith('1'):
        return True
    for kw in ETF_KEYWORDS:
        if kw in name:
            return True
    return False

def fetch_ranking(client, ranking_type, label, limit=30):
    print(f"\n{'='*60}")
    print(f"【{label}】type={ranking_type} 上位{limit}件")
    print(f"{'='*60}")
    print(f"{'順位':>4} {'コード':>6} {'銘柄名':<25} {'現在値':>8} {'売買代金':>12} {'ETF?':>5}")
    print('-'*70)

    results = client.get_ranking(ranking_type=ranking_type, exchange_division="ALL", limit=limit)

    etf_count = 0
    individual_count = 0
    for item in results:
        symbol = item.get('symbol', '')
        name = item.get('symbol_name', '')[:24]
        price = item.get('current_price', 0) or 0
        volume = item.get('trading_volume', 0) or 0
        # 売買代金（万円）= 出来高×現在値÷10000
        trading_value = price * volume * 1000 / 10000  # ランキングのvolumeは千株単位
        etf = is_etf(symbol, item.get('symbol_name', ''))
        if etf:
            etf_count += 1
        else:
            individual_count += 1

        marker = '★ETF' if etf else ''
        print(f"{item.get('rank', ''):>4} {symbol:>6} {name:<25} {price:>8,.0f} {trading_value:>10,.0f}万 {marker:>5}")

    print(f"\nETF/REIT: {etf_count}件 / 個別株: {individual_count}件")
    return results

def main():
    client = KabuClient()  # 本番ポート18080

    r6  = fetch_ranking(client, ranking_type=6,  label="売買高急増（現行）")
    r7 = fetch_ranking(client, ranking_type=7, label="売買代金急増（比較）")

    # 両方に入っている銘柄
    s6  = {item['symbol'] for item in r6}
    s7  = {item['symbol'] for item in r7}
    both = s6 & s7

    print(f"\n{'='*60}")
    print(f"【共通銘柄】両方のランキングに入っている銘柄: {len(both)}件")
    print(f"{'='*60}")
    for item in r6:
        if item['symbol'] in both:
            name = item.get('symbol_name', '')[:24]
            etf = is_etf(item['symbol'], item.get('symbol_name', ''))
            marker = '★ETF' if etf else ''
            print(f"  {item['symbol']} {name} {marker}")

if __name__ == '__main__':
    main()
