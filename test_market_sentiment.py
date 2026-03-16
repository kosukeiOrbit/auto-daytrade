"""
地合いチェック機能のテスト
"""
from datetime import datetime, timedelta
from dateutil import tz
from src.utils.market_sentiment import MarketSentiment
import jpholiday

def get_previous_business_day(date):
    """直前の営業日を取得"""
    prev_date = date - timedelta(days=1)
    while prev_date.weekday() >= 5 or jpholiday.is_holiday(prev_date):
        prev_date -= timedelta(days=1)
    return prev_date

# 日本時間
jst = tz.gettz("Asia/Tokyo")
now = datetime.now(jst)
target_date = get_previous_business_day(now)

print(f"実行日時: {now.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"対象日（日本の前営業日）: {target_date.strftime('%Y-%m-%d')}")
print("-" * 60)

# 地合いチェック
sentiment = MarketSentiment()
us_market = sentiment.get_us_market_close(date=target_date)

if us_market:
    print(f"\n取得成功:")
    print(f"  対象日: {us_market['date']}")
    print(f"  NYダウ: {us_market['dow_close']:.2f} ({us_market['dow_change_pct']:+.2f}%)")
    print(f"  ナスダック: {us_market['nasdaq_close']:.2f} ({us_market['nasdaq_change_pct']:+.2f}%)")

    # 地合い判定
    market_status = sentiment.check_market_sentiment(
        us_market['dow_change_pct'],
        us_market['nasdaq_change_pct'],
        threshold=-2.0
    )
    print(f"\n地合い判定: {market_status}")
else:
    print("取得失敗")
