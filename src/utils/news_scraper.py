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

            # ニュース記事を抽出
            news_list = soup.find('div', class_='news_list')
            if not news_list:
                logger.warning(f"{code}: ニュースが見つかりませんでした")
                return ""

            articles = news_list.find_all('li')[:max_articles]

            news_texts = []
            for article in articles:
                # 日付
                date_elem = article.find('span', class_='date')
                date_text = date_elem.get_text(strip=True) if date_elem else ""

                # タイトル
                title_elem = article.find('a')
                title_text = title_elem.get_text(strip=True) if title_elem else ""

                # 本文（プレビュー）
                content_elem = article.find('p', class_='news_content')
                content_text = content_elem.get_text(strip=True) if content_elem else ""

                if title_text:
                    news_texts.append(f"[{date_text}] {title_text}")
                    if content_text:
                        news_texts.append(content_text)

            combined_text = "\n".join(news_texts)

            if combined_text:
                logger.success(f"{code}: ニュース{len(articles)}件取得")
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
