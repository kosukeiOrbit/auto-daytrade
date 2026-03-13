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

    def _fetch_disclosures_for_date(self, date):
        """
        指定日の適時開示一覧を取得する

        Args:
            date: 対象日 (datetime)

        Returns:
            list: 開示情報のリスト（生データ）
        """
        date_str = date.strftime('%Y%m%d')
        url = f"{self.base_url}/I_list_001_{date_str}.html"

        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # テーブルから開示情報を抽出
            table = soup.find('table', class_='kjContents')
            if not table:
                logger.warning(f"TDnetのテーブルが見つかりませんでした: {date_str}")
                return []

            rows = table.find_all('tr')[1:]  # ヘッダー行をスキップ

            disclosures = []
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 4:
                    continue

                time_str = cols[0].get_text(strip=True)
                code = cols[1].get_text(strip=True)
                company = cols[2].get_text(strip=True)
                title = cols[3].get_text(strip=True)

                pdf_link = cols[3].find('a')
                pdf_url = pdf_link['href'] if pdf_link else ''

                try:
                    jst = tz.gettz('Asia/Tokyo')
                    disclosure_time = datetime.strptime(
                        f"{date.strftime('%Y-%m-%d')} {time_str}",
                        '%Y-%m-%d %H:%M'
                    ).replace(tzinfo=jst)
                except ValueError:
                    continue

                disclosures.append({
                    'code': code,
                    'company': company,
                    'title': title,
                    'time': time_str,
                    'datetime': disclosure_time,
                    'url': pdf_url,
                    'date': date_str,
                })

            return disclosures

        except Exception as e:
            logger.error(f"TDnetデータ取得エラー ({date_str}): {e}")
            return []

    def get_after_hours_disclosures(self, target_date=None):
        """
        引け後の適時開示を取得（前日15:30〜当日6:00）

        前日ページと当日ページの両方を取得してマージし、
        15:30〜翌6:00の範囲でフィルタする。

        Args:
            target_date: 対象日 (datetime)。Noneの場合は今日

        Returns:
            list: [{'code', 'company', 'title', 'time', 'url'}, ...]
        """
        jst = tz.gettz('Asia/Tokyo')
        if target_date is None:
            target_date = datetime.now(jst)

        # 前日を計算（土日をスキップして金曜日へ）
        prev_date = target_date - timedelta(days=1)
        while prev_date.weekday() >= 5:
            prev_date -= timedelta(days=1)

        # フィルタ範囲
        after_hours_start = prev_date.replace(hour=15, minute=30, second=0, tzinfo=jst)
        next_morning = target_date.replace(hour=6, minute=0, second=0, tzinfo=jst)

        logger.info(
            f"TDnet引け後開示取得: "
            f"{prev_date.strftime('%Y-%m-%d')} 15:30 〜 "
            f"{target_date.strftime('%Y-%m-%d')} 06:00"
        )

        # 前日ページと当日ページ両方を取得してマージ
        all_disclosures = []
        all_disclosures.extend(self._fetch_disclosures_for_date(prev_date))
        time.sleep(0.5)  # サーバー負荷対策
        all_disclosures.extend(self._fetch_disclosures_for_date(target_date))

        logger.info(f"取得した全開示件数（フィルタ前）: {len(all_disclosures)}件")

        # 時刻範囲 + ポジティブキーワードでフィルタ
        filtered = []
        for d in all_disclosures:
            dt = d.get('datetime')
            if dt is None:
                continue
            if after_hours_start <= dt <= next_morning:
                if self._is_target_disclosure(d['title']):
                    filtered.append(d)

        logger.info(f"引け後開示（ポジティブ）: {len(filtered)}件")
        return filtered

    def _is_target_disclosure(self, title):
        """
        対象となる開示かチェック

        Args:
            title: 開示タイトル

        Returns:
            bool: 対象の場合True
        """
        target_keywords = [
            '業績予想', '上方修正', '決算短信',
            '株式分割', '自己株式', '自社株買い', '株主優待',
        ]
        exclude_keywords = [
            '下方修正', '赤字', '損失', '延期', '訂正',
        ]

        for keyword in exclude_keywords:
            if keyword in title:
                return False
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
