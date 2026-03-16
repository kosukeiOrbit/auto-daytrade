"""
株探ニューススクレイピング
"""
import requests
from bs4 import BeautifulSoup
from loguru import logger
import time


class NewsScraper:
    """株探ニューススクレイパー"""

    def __init__(self):
        """初期化"""
        self.base_url = "https://kabutan.jp"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    def get_stock_news(self, code, max_articles=5):
        """
        銘柄の最新ニュースを取得

        Args:
            code: 銘柄コード（文字列）
            max_articles: 取得する記事数（デフォルト5）

        Returns:
            str: ニューステキスト（改行区切り）
        """
        logger.info(f"{code}: 株探ニュース取得中...")

        try:
            # 株探のニュースページにアクセス
            url = f"{self.base_url}/stock/news?code={code}"

            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # ニュース記事を抽出（テーブル形式）
            # 株探は2024年以降、テーブル形式でニュースを表示
            tables = soup.find_all('table')

            # ニューステーブルを探す（通常4番目のテーブル）
            news_table = None
            for table in tables:
                rows = table.find_all('tr')
                # 10行以上あるテーブルをニューステーブルと判定
                if len(rows) >= 5:
                    news_table = table
                    break

            if not news_table:
                logger.warning(f"{code}: ニューステーブルが見つかりませんでした")
                return ""

            rows = news_table.find_all('tr')

            # 除外カテゴリ（銘柄固有でない一般的なマーケット情報）
            # テク: テクニカル分析（全市場）, 特集: マーケット特集, 注目: 注目銘柄ピックアップ, 市況: 市況コメント
            exclude_categories = ['テク', '特集', '注目', '市況']

            news_texts = []
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    # 1列目: 日付、2列目: カテゴリ、3列目以降: タイトル
                    date_text = cells[0].get_text(strip=True)
                    category_text = cells[1].get_text(strip=True)

                    # 除外カテゴリをスキップ
                    if category_text in exclude_categories:
                        continue

                    # タイトルを含むセルを探す（aタグを含むセル）
                    title_text = ""
                    for cell in cells[2:]:
                        link = cell.find('a')
                        if link:
                            title_text = link.get_text(strip=True)
                            break

                    if date_text and title_text:
                        news_texts.append(f"[{date_text}] {title_text}")

                        # 最大記事数に達したら終了
                        if len(news_texts) >= max_articles:
                            break

            combined_text = "\n".join(news_texts)

            if combined_text:
                logger.success(f"{code}: ニュース{len(news_texts)}件取得")
            else:
                logger.warning(f"{code}: ニューステキストなし")

            # レート制限対策（連続アクセス防止）
            time.sleep(0.5)

            return combined_text

        except Exception as e:
            logger.error(f"{code}: ニュース取得エラー: {e}")
            return ""

    def get_company_name(self, code):
        """
        銘柄コードから会社名を取得

        Args:
            code: 銘柄コード（文字列）

        Returns:
            str: 会社名
        """
        try:
            url = f"{self.base_url}/stock/?code={code}"

            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # 会社名を抽出
            title_elem = soup.find('h3')
            if title_elem:
                company_name = title_elem.get_text(strip=True)
                # 銘柄コードを除去
                company_name = company_name.replace(f"({code})", "").strip()
                return company_name

            return f"銘柄{code}"

        except Exception as e:
            logger.error(f"{code}: 会社名取得エラー: {e}")
            return f"銘柄{code}"
