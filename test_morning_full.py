"""
朝スクリーニング全体フロー確認（J-Quants APIレート制限回避版）
"""
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
import jpholiday
import pandas as pd
from src.utils.market_sentiment import MarketSentiment

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
print(f"対象日（前営業日）: {target_date.strftime('%Y-%m-%d')}")
print("=" * 60)

# STEP 2: 地合いチェック
print("\nSTEP 2: 地合いチェック（NYダウ・ナスダック）")
print("=" * 60)

sentiment = MarketSentiment()
us_market = sentiment.get_us_market_close(date=target_date)

if us_market is None:
    print("[ERROR] 米国市場データ取得失敗")
else:
    print("[SUCCESS] 米国市場データ取得成功")
    print(f"   NYダウ: {us_market['dow_close']:.2f} ({us_market['dow_change_pct']:+.2f}%)")
    print(f"   ナスダック: {us_market['nasdaq_close']:.2f} ({us_market['nasdaq_change_pct']:+.2f}%)")

    market_status = sentiment.check_market_sentiment(
        us_market['dow_change_pct'],
        us_market['nasdaq_change_pct'],
        threshold=-2.0
    )

    print(f"\n地合い判定: {market_status}")
    if market_status == 'skip_all':
        print("  → 全スキップ（地合い悪化）")
    elif market_status == 'volume_only':
        print("  → 出来高急増銘柄のみ対象（地合いやや悪化）")
    else:
        print("  → 通常通り処理（地合い良好）")

print("\n" + "=" * 60)
print("地合いチェック確認完了")
print("=" * 60)
