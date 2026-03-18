"""
銘柄情報API取得テスト
検証モードでどこまで情報が取得できるか確認
"""
from src.utils.kabu_client import KabuClient
from loguru import logger

client = KabuClient()

# テスト銘柄
test_symbols = [
    ("7203", "トヨタ自動車"),
    ("9984", "ソフトバンクグループ"),
    ("6758", "ソニーグループ"),
    ("1301", "極洋"),  # 小型株
]

print("=" * 80)
print("検証モード: 銘柄情報取得テスト")
print("=" * 80)
print()

for code, name in test_symbols:
    print(f"【{code}】{name}")
    print("-" * 80)

    try:
        symbol_info = client.get_symbol(code, exchange=1)

        print(f"  銘柄コード: {symbol_info['symbol']}")
        print(f"  銘柄名: {symbol_info['symbol_name']}")
        print(f"  現在値: {symbol_info['current_price']}")
        print(f"  買気配値: {symbol_info['bid_price']}")
        print(f"  売気配値: {symbol_info['ask_price']}")
        print(f"  出来高: {symbol_info['trading_volume']}")
        print(f"  値幅上限: {symbol_info['upper_limit']}")
        print(f"  値幅下限: {symbol_info['lower_limit']}")
        print()

    except Exception as e:
        logger.error(f"  エラー: {e}")
        print()

print("=" * 80)
print("テスト完了")
print("=" * 80)
