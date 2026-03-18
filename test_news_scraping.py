"""
株探ニューススクレイピングのテスト
実際に取得しているニュース内容を確認（鮮度フィルタテスト）
"""
import sys
import io
from datetime import datetime
from dateutil import tz
from src.utils.news_scraper import NewsScraper

# UTF-8出力設定
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

scraper = NewsScraper()

# 問題の銘柄のニュースを直接取得
test_codes = [
    ('5888', 'DAIWA CYCLE'),
    ('7110', 'クラシコム'),
    ('3565', 'アセンテック'),
    ('2776', '新都ホールディングス')
]

# 3/18 朝7:00を想定
jst = tz.gettz('Asia/Tokyo')
test_date = datetime(2026, 3, 18, 7, 0, 0, tzinfo=jst)

print("=" * 80)
print("株探ニューススクレイピング内容確認（鮮度フィルタテスト）")
print(f"基準日時: {test_date.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"期待: 3/17 15:30以降のニュースのみ取得")
print("=" * 80)

for code, name in test_codes:
    print(f"\n{'='*80}")
    print(f"【{code} {name}】")
    print("=" * 80)

    # 会社名取得
    company_name = scraper.get_company_name(code)
    print(f"会社名: {company_name}")

    # ニュース取得（鮮度フィルタあり）
    news_text = scraper.get_stock_news(code, max_articles=5, reference_date=test_date)

    if news_text:
        print(f"\n取得したニュース（鮮度フィルタ適用後）:")
        print("-" * 80)
        print(news_text)
        print("-" * 80)
    else:
        print("ニュースなし（すべて古いニュースとして除外された可能性）")
