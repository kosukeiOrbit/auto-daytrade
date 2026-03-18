"""
APIデータキャッシュマネージャー
"""
import os
import pickle
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger
import pandas as pd


class CacheManager:
    """APIデータのキャッシュを管理するクラス"""

    CACHE_DIR = Path("data/cache")

    def __init__(self):
        """初期化"""
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _get_cache_path(self, cache_type: str, date_key: str) -> Path:
        """
        キャッシュファイルのパスを取得

        Args:
            cache_type: キャッシュタイプ (prices, financial, listed_info)
            date_key: 日付キー (YYYYMMDD, YYYYMM, etc.)

        Returns:
            Path: キャッシュファイルパス
        """
        return self.CACHE_DIR / f"{cache_type}_{date_key}.pkl"

    def save_cache(self, cache_type: str, date_key: str, data: any):
        """
        キャッシュを保存

        Args:
            cache_type: キャッシュタイプ
            date_key: 日付キー
            data: 保存するデータ
        """
        cache_path = self._get_cache_path(cache_type, date_key)
        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f)
            logger.info(f"キャッシュ保存: {cache_path}")
        except Exception as e:
            logger.error(f"キャッシュ保存失敗: {cache_path}, {e}")

    def load_cache(self, cache_type: str, date_key: str) -> any:
        """
        キャッシュを読み込み

        Args:
            cache_type: キャッシュタイプ
            date_key: 日付キー

        Returns:
            any: キャッシュデータ (存在しない場合None)
        """
        cache_path = self._get_cache_path(cache_type, date_key)
        if not cache_path.exists():
            return None

        try:
            with open(cache_path, 'rb') as f:
                data = pickle.load(f)
            logger.info(f"キャッシュ読み込み: {cache_path}")
            return data
        except Exception as e:
            logger.error(f"キャッシュ読み込み失敗: {cache_path}, {e}")
            return None

    def is_cache_valid(self, cache_type: str, date_key: str, max_age_days: int = None) -> bool:
        """
        キャッシュが有効かチェック

        Args:
            cache_type: キャッシュタイプ
            date_key: 日付キー
            max_age_days: 最大有効日数 (Noneの場合は存在のみチェック)

        Returns:
            bool: 有効ならTrue
        """
        cache_path = self._get_cache_path(cache_type, date_key)
        if not cache_path.exists():
            return False

        if max_age_days is None:
            return True

        # ファイルの更新日時をチェック
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age = datetime.now() - mtime
        return age.days < max_age_days

    def get_listed_info_cache(self) -> pd.DataFrame:
        """
        上場銘柄情報のキャッシュを取得 (7日間有効)

        Returns:
            pd.DataFrame: 上場銘柄情報 (キャッシュがない場合None)
        """
        # キャッシュキーは週単位 (YYYYWw形式)
        today = datetime.now()
        week_key = today.strftime("%Y%U")  # 年 + 週番号

        if self.is_cache_valid("listed_info", week_key, max_age_days=7):
            logger.info("上場銘柄情報: キャッシュを使用 (7日以内)")
            return self.load_cache("listed_info", week_key)

        return None

    def save_listed_info_cache(self, data: pd.DataFrame):
        """
        上場銘柄情報をキャッシュに保存

        Args:
            data: 上場銘柄情報
        """
        today = datetime.now()
        week_key = today.strftime("%Y%U")
        self.save_cache("listed_info", week_key, data)

    def get_financial_cache(self) -> pd.DataFrame:
        """
        財務情報のキャッシュを取得 (月次、30日間有効)

        Returns:
            pd.DataFrame: 財務情報 (キャッシュがない場合None)
        """
        # キャッシュキーは月単位
        today = datetime.now()
        month_key = today.strftime("%Y%m")

        if self.is_cache_valid("financial", month_key, max_age_days=30):
            logger.info("財務情報: キャッシュを使用 (30日以内)")
            return self.load_cache("financial", month_key)

        return None

    def save_financial_cache(self, data: pd.DataFrame):
        """
        財務情報をキャッシュに保存

        Args:
            data: 財務情報
        """
        today = datetime.now()
        month_key = today.strftime("%Y%m")
        self.save_cache("financial", month_key, data)

    def get_prices_cache(self, target_date: datetime) -> pd.DataFrame:
        """
        株価データのキャッシュを取得

        Args:
            target_date: 対象日

        Returns:
            pd.DataFrame: 株価データ (キャッシュがない場合None)
        """
        date_key = target_date.strftime("%Y%m%d")

        if self.is_cache_valid("prices", date_key):
            logger.info(f"株価データ ({date_key}): キャッシュを使用")
            return self.load_cache("prices", date_key)

        return None

    def save_prices_cache(self, target_date: datetime, data: pd.DataFrame):
        """
        株価データをキャッシュに保存

        Args:
            target_date: 対象日
            data: 株価データ
        """
        date_key = target_date.strftime("%Y%m%d")
        self.save_cache("prices", date_key, data)

    def get_prices_range_cache(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        期間内の株価データをキャッシュから取得して結合

        Args:
            start_date: 開始日
            end_date: 終了日

        Returns:
            pd.DataFrame: 期間内の株価データ (一部でもキャッシュがない場合None)
        """
        df_list = []
        current_date = start_date

        while current_date <= end_date:
            df_day = self.get_prices_cache(current_date)
            if df_day is None:
                logger.warning(f"株価データ ({current_date.strftime('%Y%m%d')}): キャッシュなし")
                return None

            df_list.append(df_day)
            current_date += timedelta(days=1)

        if len(df_list) == 0:
            return None

        logger.info(f"株価データ ({start_date.strftime('%Y%m%d')} 〜 {end_date.strftime('%Y%m%d')}): キャッシュから結合")
        return pd.concat(df_list, ignore_index=True)

    def cleanup_old_caches(self, days=90):
        """
        古いキャッシュを削除

        Args:
            days: この日数より古いキャッシュを削除
        """
        cutoff = datetime.now() - timedelta(days=days)
        deleted_count = 0

        for cache_file in self.CACHE_DIR.glob("*.pkl"):
            mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
            if mtime < cutoff:
                cache_file.unlink()
                deleted_count += 1
                logger.info(f"古いキャッシュを削除: {cache_file}")

        if deleted_count > 0:
            logger.info(f"合計 {deleted_count} 個の古いキャッシュを削除しました")
