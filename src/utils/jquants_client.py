"""
J-Quants API クライアント
"""
import jquantsapi
from datetime import datetime
from dateutil import tz
from loguru import logger
from .config import Config


class JQuantsClient:
    """J-Quants API クライアントのラッパー"""

    def __init__(self):
        """初期化"""
        Config.validate()
        self.client = jquantsapi.ClientV2(api_key=Config.JQUANTS_API_KEY)
        logger.info("J-Quants API クライアントを初期化しました")

    def test_connection(self):
        """
        API接続テスト

        Returns:
            bool: 接続成功時True
        """
        try:
            # 銘柄一覧を少数取得してテスト
            result = self.client.get_list()
            if result is not None and len(result) > 0:
                logger.success(f"J-Quants API接続成功 - {len(result)}銘柄取得")
                return True
            else:
                logger.error("J-Quants API接続失敗 - データが空です")
                return False
        except Exception as e:
            logger.error(f"J-Quants API接続エラー: {e}")
            return False

    def get_listed_info(self):
        """
        上場銘柄一覧を取得

        Returns:
            DataFrame: 銘柄一覧データ
        """
        try:
            logger.info("上場銘柄一覧を取得中...")
            df = self.client.get_list()
            logger.success(f"上場銘柄一覧を取得しました: {len(df)}銘柄")
            return df
        except Exception as e:
            logger.error(f"上場銘柄一覧取得エラー: {e}")
            raise

    def get_daily_quotes(self, code=None, date=None):
        """
        日次株価データを取得

        Args:
            code: 銘柄コード（省略時は全銘柄）
            date: 日付（datetime形式、省略時は今日）

        Returns:
            DataFrame: 株価データ
        """
        try:
            # dateが指定されていない場合は今日の日付を使用
            if date is None:
                date = datetime.now(tz.gettz("Asia/Tokyo"))
                logger.info(f"日付未指定のため今日の日付を使用: {date.strftime('%Y-%m-%d')}")

            logger.info(f"日次株価データを取得中... (code={code}, date={date.strftime('%Y-%m-%d') if date else None})")

            if code:
                # 特定銘柄の場合はget_eq_bars_dailyを使用
                df = self.client.get_eq_bars_daily(
                    code=code,
                    date_yyyymmdd=date.strftime('%Y%m%d')
                )
            else:
                # 全銘柄の場合はget_eq_bars_daily_rangeを使用
                df = self.client.get_eq_bars_daily_range(
                    start_dt=date,
                    end_dt=date
                )

            logger.success(f"日次株価データを取得しました: {len(df)}件")
            return df
        except Exception as e:
            logger.error(f"日次株価データ取得エラー: {e}")
            raise
