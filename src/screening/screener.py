"""
銘柄スクリーニング機能
"""
import pandas as pd
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
from ..utils.jquants_client import JQuantsClient


class Screener:
    """銘柄スクリーニングクラス"""

    def __init__(self, budget=None):
        """
        初期化

        Args:
            budget: 買付余力（円）。Noneの場合は制限なし
        """
        self.client = JQuantsClient()
        self.budget = budget
        logger.info(f"スクリーナーを初期化しました (予算: {budget:,}円)" if budget else "スクリーナーを初期化しました (予算: 制限なし)")

    def get_candidates(
        self,
        min_price_change_rate=3.0,
        top_n_by_value=20,
        date=None
    ):
        """
        候補銘柄をスクリーニング

        Args:
            min_price_change_rate: 最低上昇率（%）デフォルト3%
            top_n_by_value: 売買代金上位N銘柄に絞り込み。デフォルト20
            date: 対象日（datetimeオブジェクト）。Noneの場合は最新

        Returns:
            DataFrame: 候補銘柄リスト
        """
        logger.info("=" * 60)
        logger.info("スクリーニング開始")
        logger.info(f"条件: 上昇率{min_price_change_rate}%以上, 売買代金上位{top_n_by_value}銘柄")
        if self.budget:
            logger.info(f"予算: {self.budget:,}円")
        logger.info("=" * 60)

        # 1. 全銘柄の日次株価データを取得（過去2日分）
        logger.info("\n[1/4] 全銘柄の株価データを取得中（過去2日分）...")

        # 対象日が指定されていない場合は今日を使用
        if date is None:
            jst = tz.gettz("Asia/Tokyo")
            date = datetime.now(jst)

        # 前日のデータも取得するため、3営業日前から取得
        start_date = date - timedelta(days=5)

        # 日付範囲で取得
        from ..utils.jquants_client import JQuantsClient
        df_prices = self.client.client.get_eq_bars_daily_range(
            start_dt=start_date,
            end_dt=date
        )

        logger.info(f"取得件数: {len(df_prices)}件")

        # 対象日のデータのみに絞り込む（前日比計算用に直前のデータは残す）
        df_prices['Date'] = pd.to_datetime(df_prices['Date'])
        target_date_str = date.strftime('%Y-%m-%d')
        logger.info(f"対象日: {target_date_str}")

        # 2. 前日比上昇率でフィルタ
        logger.info(f"\n[2/4] 前日比上昇率 {min_price_change_rate}% 以上でフィルタ中...")

        # 日付でソート
        df_prices = df_prices.sort_values(['Code', 'Date'])

        # 前日比上昇率を計算
        df_prices['PrevClose'] = df_prices.groupby('Code')['C'].shift(1)
        df_prices['ChangeRate'] = ((df_prices['C'] - df_prices['PrevClose']) / df_prices['PrevClose'] * 100)

        # 対象日のデータのみに絞り込み
        df_target = df_prices[df_prices['Date'] == target_date_str].copy()
        logger.info(f"対象日のデータ: {len(df_target)}件")

        # 前日比が計算できている（PrevCloseがNaNでない）データのみ
        df_target = df_target[df_target['PrevClose'].notna()]
        logger.info(f"前日比計算可能なデータ: {len(df_target)}件")

        # 上昇率フィルタ
        df_filtered = df_target[df_target['ChangeRate'] >= min_price_change_rate].copy()
        logger.info(f"該当銘柄: {len(df_filtered)}銘柄")

        if len(df_filtered) == 0:
            logger.warning("条件に合う銘柄が見つかりませんでした")
            return pd.DataFrame()

        # 3. 売買代金上位でフィルタ
        logger.info(f"\n[3/4] 売買代金上位 {top_n_by_value} 銘柄に絞り込み中...")

        # 売買代金 = 終値 × 出来高
        df_filtered['TradingValue'] = df_filtered['C'] * df_filtered['Vo']
        df_filtered = df_filtered.nlargest(top_n_by_value, 'TradingValue')
        logger.info(f"絞り込み後: {len(df_filtered)}銘柄")

        # 4. 予算内フィルタ（1単元=100株が買える銘柄）
        if self.budget:
            logger.info(f"\n[4/4] 予算内フィルタ (1単元100株 ≤ {self.budget:,}円)...")
            df_filtered['UnitPrice'] = df_filtered['C'] * 100  # 1単元=100株
            df_filtered = df_filtered[df_filtered['UnitPrice'] <= self.budget].copy()
            logger.info(f"予算内銘柄: {len(df_filtered)}銘柄")
        else:
            logger.info(f"\n[4/4] 予算フィルタはスキップ（予算制限なし）")

        # 結果を整形
        result = df_filtered[[
            'Code', 'Date', 'O', 'H', 'L', 'C', 'Vo',
            'ChangeRate', 'TradingValue'
        ]].copy()

        # 売買代金で降順ソート
        result = result.sort_values('TradingValue', ascending=False)
        result = result.reset_index(drop=True)

        logger.info("\n" + "=" * 60)
        logger.success(f"スクリーニング完了: {len(result)}銘柄")
        logger.info("=" * 60)

        return result

    def save_candidates(self, df, filepath="data/candidates.csv"):
        """
        候補銘柄をCSVファイルに保存

        Args:
            df: 候補銘柄データフレーム
            filepath: 保存先ファイルパス
        """
        try:
            # dataディレクトリが存在しない場合は作成
            import os
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.success(f"候補銘柄を保存しました: {filepath}")
        except Exception as e:
            logger.error(f"ファイル保存エラー: {e}")
            raise
