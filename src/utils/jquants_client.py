"""
J-Quants API クライアント
"""
import jquantsapi
from datetime import datetime
from dateutil import tz
from loguru import logger
from .config import Config
from .cache_manager import CacheManager


class JQuantsClient:
    """J-Quants API クライアントのラッパー"""

    def __init__(self, use_cache=True):
        """
        初期化

        Args:
            use_cache: キャッシュを使用するか (デフォルト: True)
        """
        Config.validate()
        self.client = jquantsapi.ClientV2(api_key=Config.JQUANTS_API_KEY)
        self.use_cache = use_cache
        self.cache_manager = CacheManager() if use_cache else None
        logger.info(f"J-Quants API クライアントを初期化しました (キャッシュ: {'有効' if use_cache else '無効'})")

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
        # キャッシュチェック
        if self.use_cache:
            cached_data = self.cache_manager.get_listed_info_cache()
            if cached_data is not None:
                logger.success(f"上場銘柄一覧をキャッシュから取得: {len(cached_data)}銘柄")
                return cached_data

        try:
            logger.info("上場銘柄一覧をAPIから取得中...")
            df = self.client.get_list()
            logger.success(f"上場銘柄一覧を取得しました: {len(df)}銘柄")

            # キャッシュに保存
            if self.use_cache:
                self.cache_manager.save_listed_info_cache(df)

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
        # dateが指定されていない場合は今日の日付を使用
        if date is None:
            date = datetime.now(tz.gettz("Asia/Tokyo"))
            logger.info(f"日付未指定のため今日の日付を使用: {date.strftime('%Y-%m-%d')}")

        # キャッシュチェック（全銘柄の場合のみ）
        if self.use_cache and code is None:
            cached_data = self.cache_manager.get_prices_cache(date)
            if cached_data is not None:
                logger.success(f"日次株価データをキャッシュから取得: {len(cached_data)}件")
                return cached_data

        try:
            logger.info(f"日次株価データをAPIから取得中... (code={code}, date={date.strftime('%Y-%m-%d')})")

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

            # キャッシュに保存（全銘柄の場合のみ）
            if self.use_cache and code is None:
                self.cache_manager.save_prices_cache(date, df)

            return df
        except Exception as e:
            logger.error(f"日次株価データ取得エラー: {e}")
            raise

    def get_statements(self):
        """
        財務情報を取得（発行済株式数を含む）

        優先順位:
            1. pickleキャッシュ（30日以内）
            2. ローカルCSVファイル（data/{YYYY}/fins_summary_*.csv.gz）
            3. J-Quants API（フォールバック）

        Returns:
            DataFrame: 財務情報データ
                - Code: 銘柄コード
                - ShOutFY: 期末発行済株式数
                - その他財務情報
        """
        import os
        import glob
        import pandas as pd
        from datetime import datetime, timedelta

        # 1. pickleキャッシュチェック
        if self.use_cache:
            cached_data = self.cache_manager.get_financial_cache()
            if cached_data is not None:
                logger.success(f"財務情報をキャッシュから取得: {len(cached_data)}件")
                return cached_data

        # 2. ローカルCSVファイルから読み込み
        now = datetime.now(tz.gettz("Asia/Tokyo"))
        local_dfs = []

        # 2a. 直近7日の日次ファイルを探す
        for days_back in range(7):
            dt = now - timedelta(days=days_back)
            filepath = f"data/{dt.year}/fins_summary_{dt.strftime('%Y%m%d')}.csv.gz"
            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath)
                    local_dfs.append(df)
                    logger.info(f"財務情報ローカルCSV読み込み: {filepath} ({len(df)}件)")
                except Exception as e:
                    logger.warning(f"ローカルCSV読み込み失敗: {filepath}: {e}")

        # 2b. 当月・前月の月次ファイルを探す
        for months_back in range(0, 13):
            dt = now - timedelta(days=months_back * 30)
            filepath = f"data/{dt.year}/fins_summary_{dt.strftime('%Y%m')}.csv.gz"
            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath)
                    local_dfs.append(df)
                    logger.info(f"財務情報ローカルCSV読み込み: {filepath} ({len(df)}件)")
                except Exception as e:
                    logger.warning(f"ローカルCSV読み込み失敗: {filepath}: {e}")

        if local_dfs:
            df_all = pd.concat(local_dfs, ignore_index=True)
            # 各銘柄の最新データのみ残す（DiscDateが最新のもの）
            if 'DiscDate' in df_all.columns:
                df_all['DiscDate'] = pd.to_datetime(df_all['DiscDate'], errors='coerce')
                df_all = df_all.sort_values('DiscDate').groupby('Code').tail(1).reset_index(drop=True)
            logger.success(f"財務情報をローカルCSVから取得: {len(df_all)}件 ({df_all['Code'].nunique()}銘柄)")

            # キャッシュに保存
            if self.use_cache:
                self.cache_manager.save_financial_cache(df_all)

            return df_all

        # 3. APIフォールバック
        try:
            logger.info("財務情報をAPIから取得中（直近1年分）...")

            end_dt = now
            start_dt = end_dt - timedelta(days=365)

            df = self.client.get_fin_summary_range(start_dt=start_dt, end_dt=end_dt)
            logger.success(f"財務情報を取得しました: {len(df)}件")

            # キャッシュに保存
            if self.use_cache:
                self.cache_manager.save_financial_cache(df)

            return df
        except Exception as e:
            logger.error(f"財務情報取得エラー: {e}")
            raise
