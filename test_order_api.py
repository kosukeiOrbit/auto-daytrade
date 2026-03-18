"""
注文APIテスト（検証モード）
エラーハンドリングも含めて確認
"""
from src.utils.kabu_client import KabuClient
from loguru import logger

client = KabuClient()

print("=" * 80)
print("検証モード: 注文APIテスト")
print("=" * 80)
print()

# テスト1: 成行買い注文（最小単位）
print("【テスト1】成行買い注文 (7203 トヨタ自動車 100株)")
print("-" * 80)

try:
    result = client.send_order(
        symbol="7203",
        exchange=1,
        side=2,  # 買
        qty=100,
        order_type=1,  # 成行
        price=0
    )

    print(f"  注文番号: {result['order_id']}")
    print(f"  結果コード: {result['result_code']}")
    print(f"  結果メッセージ: {result['result_msg']}")

    if result['result_code'] == 0:
        print("  ✅ 注文成功")
        order_id = result['order_id']

        # 注文取消テスト
        print()
        print("【テスト2】注文取消")
        print("-" * 80)

        try:
            cancel_result = client.cancel_order(order_id)
            print(f"  結果コード: {cancel_result['result_code']}")
            print(f"  結果メッセージ: {cancel_result['result_msg']}")

            if cancel_result['result_code'] == 0:
                print("  ✅ 取消成功")
            else:
                print("  ⚠️ 取消警告")

        except Exception as e:
            logger.error(f"  ❌ 取消エラー: {e}")

    else:
        print(f"  ⚠️ 注文警告: {result['result_msg']}")

except Exception as e:
    logger.error(f"  ❌ 注文エラー: {e}")

print()

# テスト3: 指値売り注文
print("【テスト3】指値売り注文 (7203 トヨタ自動車 100株 @ 3000円)")
print("-" * 80)

try:
    result = client.send_order(
        symbol="7203",
        exchange=1,
        side=1,  # 売
        qty=100,
        order_type=2,  # 指値
        price=3000
    )

    print(f"  注文番号: {result['order_id']}")
    print(f"  結果コード: {result['result_code']}")
    print(f"  結果メッセージ: {result['result_msg']}")

    if result['result_code'] == 0:
        print("  ✅ 注文成功")
    else:
        print(f"  ⚠️ 注文警告: {result['result_msg']}")

except Exception as e:
    logger.error(f"  ❌ 注文エラー: {e}")

print()

# テスト4: 逆指値注文
print("【テスト4】逆指値注文 (7203 トヨタ自動車 100株 2400円以下で成行売)")
print("-" * 80)

try:
    result = client.send_order(
        symbol="7203",
        exchange=1,
        side=1,  # 売
        qty=100,
        order_type=3,  # 逆指値
        price=0,  # 成行
        stop_price=2400  # トリガー価格
    )

    print(f"  注文番号: {result['order_id']}")
    print(f"  結果コード: {result['result_code']}")
    print(f"  結果メッセージ: {result['result_msg']}")

    if result['result_code'] == 0:
        print("  ✅ 注文成功")
    else:
        print(f"  ⚠️ 注文警告: {result['result_msg']}")

except Exception as e:
    logger.error(f"  ❌ 注文エラー: {e}")

print()
print("=" * 80)
print("テスト完了")
print("=" * 80)
