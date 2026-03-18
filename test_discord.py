"""
Discord通知テスト
"""
from src.utils.notifier import DiscordNotifier

notifier = DiscordNotifier()

print("=" * 80)
print("Discord通知テスト")
print("=" * 80)
print()

# テスト1: 通常メッセージ
print("【テスト1】通常メッセージ")
print("-" * 80)
notifier.send_message("✅ テストメッセージ: 自動売買システム動作確認")
print("  送信完了")
print()

# テスト2: エラー通知
print("【テスト2】エラー通知")
print("-" * 80)
notifier.send_error("❌ テストエラー: API接続失敗（これはテストです）")
print("  送信完了")
print()

# テスト3: トレード通知
print("【テスト3】トレード通知（エントリー）")
print("-" * 80)
notifier.send_trade_notification(
    action="エントリー",
    symbol="7203",
    price=2500,
    qty=300,
    stop_price=2475,
    target_price=2550
)
print("  送信完了")
print()

# テスト4: トレード通知（決済）
print("【テスト4】トレード通知（利確決済）")
print("-" * 80)
notifier.send_trade_notification(
    action="利確決済",
    symbol="7203",
    price=2550,
    qty=300
)
print("  送信完了")
print()

print("=" * 80)
print("Discord通知テスト完了")
print("=" * 80)
