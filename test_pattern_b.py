"""
パターンB 単体検証スクリプト（ドライラン）

ランキングAPIのみ本番ポート（18080）から取得し、
board取得・発注テストは検証ポート（18081）で実行する。

使い方:
    python test_pattern_b.py

Ctrl+C で終了。
"""
import os
import sys
import time
from datetime import datetime, timedelta
from loguru import logger

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.utils.kabu_client import KabuClient
from src.utils.config import Config

# ログ設定
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level:<7} | {message}")

# ポート設定
TEST_API_URL = "http://localhost:18081/kabusapi"
TEST_API_PASSWORD = "***REMOVED***"
PROD_API_URL = "http://localhost:18080/kabusapi"
PROD_API_PASSWORD = "***REMOVED***"


def main():
    logger.info("=" * 60)
    logger.info("パターンB 単体検証（ドライラン）")
    logger.info(f"ランキング: {PROD_API_URL}（本番）")
    logger.info(f"board/発注: {TEST_API_URL}（検証）")
    logger.info("=" * 60)

    # 検証ポート用クライアント（board取得・発注テスト用）
    client = KabuClient()
    client.api_url = TEST_API_URL
    client.api_password = TEST_API_PASSWORD
    client.token = None
    client.token_expires_at = None

    try:
        token = client.get_token(force_refresh=True)
        logger.success(f"検証ポート トークン取得成功: {token[:20]}...")
    except Exception as e:
        logger.error(f"検証ポート トークン取得失敗: {e}")
        return

    # 本番ポート用クライアント（ランキング取得専用）
    prod_client = KabuClient()
    prod_client.api_url = PROD_API_URL
    prod_client.api_password = PROD_API_PASSWORD
    prod_client.token = None
    prod_client.token_expires_at = None

    try:
        prod_token = prod_client.get_token(force_refresh=True)
        logger.success(f"本番ポート トークン取得成功: {prod_token[:20]}...")
    except Exception as e:
        logger.error(f"本番ポート トークン取得失敗: {e}")
        return

    # 価格履歴（銘柄ごと）
    price_history = {}  # {symbol: [{'time': datetime, 'price': float, 'vwap': float}]}
    last_volume = {}    # {symbol: 前回累積出来高}（差分計算用）

    logger.info("\n1分ごとにランキング取得・エントリー判定を実行します（Ctrl+Cで終了）\n")

    iteration = 0
    try:
        while True:
            iteration += 1
            now = datetime.now()
            logger.info(f"[{now.strftime('%H:%M:%S')}] === ラウンド {iteration} ===")

            # 1. ランキング取得（本番ポートから）
            try:
                ranking = prod_client.get_ranking(ranking_type=6, exchange_division="ALL")
            except Exception as e:
                logger.error(f"ランキング取得エラー: {e}")
                time.sleep(60)
                continue

            if not ranking:
                logger.warning("ランキングデータなし")
                time.sleep(60)
                continue

            # ETFを除外して個別株上位10件を選択
            etf_keywords = ['ETF', 'ETN', '投信', '債券', 'リート', 'REIT',
                            'インデックス', 'ヘッジ', 'ブル', 'ベア', 'レバレッジ',
                            '先進国', '新興国', 'ナスダック', 'S&P', 'TOPIX']
            individual_stocks = []
            for item in ranking[:20]:
                sym = item['symbol']
                name = item.get('symbol_name', '')
                is_etf = any(kw in name for kw in etf_keywords)
                if not is_etf:
                    try:
                        code = int(sym)
                        if code < 1000 or code > 9999:
                            is_etf = True
                    except ValueError:
                        is_etf = True
                if not is_etf:
                    individual_stocks.append(item)
                if len(individual_stocks) >= 10:
                    break

            logger.info(f"ランキング取得: {len(ranking)}件 → 個別株{len(individual_stocks)}件")

            # 2. 各銘柄の /board 取得 → 価格履歴蓄積（本番ポートから取得）
            for item in individual_stocks:
                symbol = item['symbol']
                symbol_name = item['symbol_name']
                rapid_trade_pct = item.get('rapid_trade_pct', 0)

                try:
                    board = prod_client.get_symbol(symbol)
                    time.sleep(0.2)  # レート制限対策
                except Exception as e:
                    logger.debug(f"  {symbol} {symbol_name}: board取得失敗: {e}")
                    continue

                current_price = board.get('current_price')
                vwap = board.get('vwap')
                opening_price = board.get('opening_price')
                cumulative_volume = board.get('trading_volume') or 0

                # 累積出来高→差分変換
                prev_vol = last_volume.get(symbol, 0)
                delta_volume = cumulative_volume - prev_vol
                if delta_volume < 0:
                    delta_volume = cumulative_volume
                last_volume[symbol] = cumulative_volume

                if current_price is None or current_price <= 0:
                    logger.info(f"  {symbol} {symbol_name}: 現在値なし → スキップ")
                    continue

                # 価格履歴に追加
                if symbol not in price_history:
                    price_history[symbol] = []
                price_history[symbol].append({
                    'time': now,
                    'price': current_price,
                    'vwap': vwap,
                    'opening_price': opening_price,
                    'volume': delta_volume,
                    'rapid_trade_pct': rapid_trade_pct,
                })

                # 直近25分のみ保持（出来高平均計算用）
                cutoff = now - timedelta(minutes=25)
                price_history[symbol] = [
                    r for r in price_history[symbol] if r['time'] >= cutoff
                ]

                # 3. エントリー判定
                history = price_history[symbol]
                checks = []
                entry_ok = True

                # チェック1: VWAP（0.2%バッファ許容）
                if vwap is not None and vwap > 0:
                    if current_price >= vwap * 0.998:
                        checks.append(f"VWAP上✅（{current_price} >= {vwap:.0f}×0.998）")
                    else:
                        checks.append(f"VWAP下❌（{current_price} < {vwap:.0f}×0.998）")
                        entry_ok = False
                else:
                    checks.append("VWAP不明⚠️")

                # チェック2: 寄りからの上昇率
                if opening_price and opening_price > 0:
                    change = (current_price - opening_price) / opening_price * 100
                    if change <= 3.0:
                        checks.append(f"寄りから{change:+.1f}%✅")
                    else:
                        checks.append(f"寄りから{change:+.1f}%❌(+3%超)")
                        entry_ok = False
                else:
                    # 履歴の最初の価格で代用
                    if len(history) > 0:
                        first_price = history[0]['price']
                        if first_price and first_price > 0:
                            change = (current_price - first_price) / first_price * 100
                            checks.append(f"初回比{change:+.1f}%")

                # チェック3: 5本トレンド（3本以上切り上がり）
                if len(history) >= 5:
                    recent_5 = [r['price'] for r in history[-5:] if r['price'] is not None]
                    if len(recent_5) >= 5:
                        up_count = sum(1 for i in range(len(recent_5)-1) if recent_5[i+1] > recent_5[i])
                        if up_count >= 3:
                            checks.append(f"トレンド↑{up_count}/4✅")
                        else:
                            checks.append(f"トレンド↓{up_count}/4❌")
                            entry_ok = False
                    else:
                        checks.append("トレンド判定不可")
                        entry_ok = False
                else:
                    checks.append(f"履歴{len(history)}/5本")
                    entry_ok = False

                # チェック4: 出来高急増（RapidTradePercentage優先）
                latest_rapid = history[-1].get('rapid_trade_pct', 0) if history else 0
                if latest_rapid > 0:
                    if latest_rapid >= 100:
                        checks.append(f"RapidTrade{latest_rapid:.0f}%✅")
                    else:
                        checks.append(f"RapidTrade{latest_rapid:.0f}%❌")
                        entry_ok = False
                else:
                    # フォールバック：差分計算
                    volumes = [r['volume'] for r in history if r.get('volume', 0) > 0]
                    if len(volumes) >= 5:
                        avg_vol = sum(volumes[:-1]) / len(volumes[:-1])
                        latest_vol = volumes[-1]
                        if avg_vol > 0 and latest_vol >= avg_vol * 2.0:
                            checks.append(f"出来高{latest_vol:.0f}/{avg_vol:.0f}x2✅")
                        else:
                            ratio = latest_vol / avg_vol if avg_vol > 0 else 0
                            checks.append(f"出来高{ratio:.1f}x❌")
                            entry_ok = False
                    elif len(volumes) > 0:
                        checks.append(f"出来高{len(volumes)}/5本")
                        entry_ok = False

                # 結果出力
                check_str = " ".join(checks)
                result = "→ エントリー可🟢" if entry_ok else "→ スキップ"
                logger.info(
                    f"  {symbol} {symbol_name[:10]} "
                    f"現在値:{current_price:.0f} VWAP:{vwap or '?'} "
                    f"{check_str} {result}"
                )

            # ランキングから外れた銘柄の履歴を削除
            top_symbols = {item['symbol'] for item in individual_stocks}
            for symbol in list(price_history.keys()):
                if symbol not in top_symbols:
                    del price_history[symbol]

            logger.info("")
            time.sleep(60)

    except KeyboardInterrupt:
        logger.info("\nCtrl+C で終了しました")
        logger.info(f"合計ラウンド: {iteration}")


if __name__ == "__main__":
    try:
        Config.validate()
        main()
    except Exception as e:
        logger.error(f"実行エラー: {e}")
