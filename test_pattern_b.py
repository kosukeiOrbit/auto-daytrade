"""
パターンB 単体検証スクリプト（ドライラン）

kabuステーション検証ポート（18081）を使用して
パターンBのエントリーロジックを検証する。
実際の発注は一切行わない。

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

# 検証ポートで初期化（本番.envを変更しない）
TEST_API_URL = "http://localhost:18081/kabusapi"
TEST_API_PASSWORD = "***REMOVED***"


def main():
    logger.info("=" * 60)
    logger.info("パターンB 単体検証（ドライラン）")
    logger.info(f"検証ポート: {TEST_API_URL}")
    logger.info("=" * 60)

    # KabuClientを検証ポートで初期化
    # __init__後に即座に上書きし、トークンを強制再取得する
    client = KabuClient()
    client.api_url = TEST_API_URL
    client.api_password = TEST_API_PASSWORD
    client.token = None
    client.token_expires_at = None
    logger.info(f"API URL を検証ポートに上書き: {client.api_url}")

    # 検証ポートに対してトークンを取得（force_refreshでキャッシュ無視）
    try:
        token = client.get_token(force_refresh=True)
        logger.info(f"取得したトークンの検証: API URL = {client.api_url}")
        logger.success(f"トークン取得成功: {token[:20]}...")
    except Exception as e:
        logger.error(f"トークン取得失敗: {e}")
        logger.error("kabuステーション（検証ポート18081）が起動しているか確認してください")
        return

    # 価格履歴（銘柄ごと）
    price_history = {}  # {symbol: [{'time': datetime, 'price': float, 'vwap': float}]}

    logger.info("\n1分ごとにランキング取得・エントリー判定を実行します（Ctrl+Cで終了）\n")

    iteration = 0
    try:
        while True:
            iteration += 1
            now = datetime.now()
            logger.info(f"[{now.strftime('%H:%M:%S')}] === ラウンド {iteration} ===")

            # 1. ランキング取得
            try:
                ranking = client.get_ranking(ranking_type=6, exchange_division="ALL")
            except Exception as e:
                logger.error(f"ランキング取得エラー: {e}")
                time.sleep(60)
                continue

            if not ranking:
                logger.warning("ランキングデータなし")
                time.sleep(60)
                continue

            top10 = ranking[:10]
            logger.info(f"ランキング取得: {len(top10)}銘柄")

            # 2. 各銘柄の /board 取得 → 価格履歴蓄積
            for item in top10:
                symbol = item['symbol']
                symbol_name = item['symbol_name']

                try:
                    board = client.get_symbol(symbol)
                except Exception as e:
                    logger.debug(f"  {symbol} {symbol_name}: board取得失敗: {e}")
                    continue

                current_price = board.get('current_price')
                vwap = board.get('vwap')
                opening_price = board.get('opening_price')
                volume = board.get('trading_volume')

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
                })

                # 直近10分のみ保持
                cutoff = now - timedelta(minutes=10)
                price_history[symbol] = [
                    r for r in price_history[symbol] if r['time'] >= cutoff
                ]

                # 3. エントリー判定
                history = price_history[symbol]
                checks = []
                entry_ok = True

                # チェック1: VWAP
                if vwap is not None and vwap > 0:
                    if current_price > vwap:
                        checks.append(f"VWAP上✅")
                    else:
                        checks.append(f"VWAP下❌")
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

                # チェック3: 5本トレンド
                if len(history) >= 5:
                    recent_5 = [r['price'] for r in history[-5:] if r['price'] is not None]
                    if len(recent_5) >= 5:
                        is_uptrend = all(recent_5[i] <= recent_5[i + 1] for i in range(len(recent_5) - 1))
                        if is_uptrend:
                            checks.append(f"トレンド↑✅")
                        else:
                            checks.append(f"トレンド↓❌")
                            entry_ok = False
                    else:
                        checks.append("トレンド判定不可")
                        entry_ok = False
                else:
                    checks.append(f"履歴{len(history)}/5本")
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
            top_symbols = {item['symbol'] for item in top10}
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
