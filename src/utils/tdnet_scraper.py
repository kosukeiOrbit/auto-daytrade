"""
TDnet引け後適時開示スクレイピング
"""
import requests
from datetime import datetime, timedelta
from dateutil import tz
from bs4 import BeautifulSoup
from loguru import logger
import time


class TDnetScraper:
    """TDnet適時開示スクレイパー"""

    def __init__(self):
        """初期化"""
        self.base_url = "https://www.release.tdnet.info/inbs"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    def get_after_hours_disclosures(self, target_date=None):
        """
        引け後の適時開示を取得（前日15:30〜当日6:00）

        Args:
            target_date: 対象日（datetime）。Noneの場合は今日

        Returns:
            list: [{
                'code': 銘柄コード,
                'company': 会社名,
                'title': 開示タイトル,
                'time': 開示時刻,
                'url': PDF URL
            }, ...]
        """
        if target_date is None:
            jst = tz.gettz("Asia/Tokyo")
            target_date = datetime.now(jst)

        # 前日15:30〜当日6:00の開示を取得
        prev_date = target_date - timedelta(days=1)

        logger.info(f"TDnet引け後開示取得: {prev_date.strftime('%Y-%m-%d')} 15:30 〜 {target_date.strftime('%Y-%m-%d')} 06:00")

        disclosures = []

        try:
            # TDnetの検索ページにアクセス
            # ※実際のTDnet APIは認証が必要なため、tdnet.infoを使用
            url = "https://www.release.tdnet.info/inbs/I_list_001_1F.html"

            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # テーブルから開示情報を抽出
            table = soup.find('table', class_='kjContents')
            if not table:
                logger.warning("TDnetのテーブルが見つかりませんでした")
                return []

            rows = table.find_all('tr')[1:]  # ヘッダー行をスキップ

            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 4:
                    continue

                # 時刻・銘柄コード・会社名・タイトル抽出
                time_str = cols[0].get_text(strip=True)
                code = cols[1].get_text(strip=True)
                company = cols[2].get_text(strip=True)
                title = cols[3].get_text(strip=True)

                # 開示時刻をパース
                try:
                    disclosure_time = datetime.strptime(
                        f"{target_date.strftime('%Y-%m-%d')} {time_str}",
                        '%Y-%m-%d %H:%M'
                    )
                except ValueError:
                    continue

                # 15:30〜翌朝6:00の範囲内かチェック
                after_hours_start = prev_date.replace(hour=15, minute=30, second=0)
                next_morning = target_date.replace(hour=6, minute=0, second=0)

                if not (after_hours_start <= disclosure_time <= next_morning):
                    continue

                # 対象となる開示種別かチェック
                if self._is_target_disclosure(title):
                    # PDF URLを取得
                    pdf_link = cols[3].find('a')
                    pdf_url = pdf_link['href'] if pdf_link else None

                    disclosures.append({
                        'code': code,
                        'company': company,
                        'title': title,
                        'time': disclosure_time.strftime('%H:%M'),
                        'url': pdf_url
                    })

                    logger.info(f"  {code} {company}: {title}")

            logger.success(f"引け後開示: {len(disclosures)}件")
            return disclosures

        except Exception as e:
            logger.error(f"TDnet取得エラー: {e}")
            return []

    def _is_target_disclosure(self, title):
        """
        対象となる開示種別かを判定

        Args:
            title: 開示タイトル

        Returns:
            bool: 対象の場合True
        """
        # 対象となるキーワード
        target_keywords = [
            '業績予想',
            '上方修正',
            '決算短信',
            '株式分割',
            '自己株式',
            '自社株買い',
            '株主優待'
        ]

        # 除外キーワード
        exclude_keywords = [
            '下方修正',
            '赤字',
            '損失',
            '延期',
            '訂正'
        ]

        # 除外チェック
        for keyword in exclude_keywords:
            if keyword in title:
                return False

        # 対象チェック
        for keyword in target_keywords:
            if keyword in title:
                return True

        return False

    def get_disclosure_codes(self, target_date=None):
        """
        引け後開示があった銘柄コードのリストを取得

        Args:
            target_date: 対象日

        Returns:
            list: 銘柄コードのリスト
        """
        disclosures = self.get_after_hours_disclosures(target_date)
        return list(set([d['code'] for d in disclosures]))
