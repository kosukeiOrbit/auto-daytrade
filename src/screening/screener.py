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

        # 上場銘柄情報を取得（ETF・REIT除外用）
        try:
            self.listed_info = self.client.get_listed_info()
            logger.info(f"上場銘柄情報を取得しました: {len(self.listed_info)}銘柄")
        except Exception as e:
            logger.warning(f"上場銘柄情報の取得に失敗しました: {e}")
            self.listed_info = None

        # 財務情報を取得（発行済株式数取得用）
        try:
            self.statements = self.client.get_statements()
            logger.info(f"財務情報を取得しました: {len(self.statements)}件")
        except Exception as e:
            logger.warning(f"財務情報の取得に失敗しました: {e}")
            self.statements = None

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

    def get_volume_surge_candidates(
        self,
        surge_threshold=2.0,
        lookback_days=20,
        date=None
    ):
        """
        出来高急増銘柄をスクリーニング（新設計）

        Args:
            surge_threshold: 出来高急増倍率（デフォルト2.0 = 20日平均の2倍以上）
            lookback_days: 平均出来高計算期間（デフォルト20日）
            date: 対象日（datetimeオブジェクト）。Noneの場合は最新

        Returns:
            DataFrame: 候補銘柄リスト
        """
        logger.info("=" * 60)
        logger.info("出来高急増スクリーニング開始")
        logger.info(f"条件: {lookback_days}日平均の{surge_threshold}倍以上")
        if self.budget:
            logger.info(f"予算: {self.budget:,}円")
        logger.info("=" * 60)

        # 対象日が指定されていない場合は今日を使用
        if date is None:
            jst = tz.gettz("Asia/Tokyo")
            date = datetime.now(jst)

        # 過去データ取得（平均計算用に余裕を持って取得）
        start_date = date - timedelta(days=lookback_days + 10)

        logger.info(f"\n[1/5] 株価データ取得中（{start_date.strftime('%Y-%m-%d')} 〜 {date.strftime('%Y-%m-%d')}）...")
        try:
            df_prices = self.client.client.get_eq_bars_daily_range(
                start_dt=start_date,
                end_dt=date
            )
            logger.info(f"取得件数: {len(df_prices)}件")
        except Exception as e:
            logger.error(f"株価データ取得エラー: {e}")
            logger.error("API制限またはネットワークエラーの可能性があります")
            return pd.DataFrame()

        # 日付を変換
        df_prices['Date'] = pd.to_datetime(df_prices['Date'])
        target_date_str = date.strftime('%Y-%m-%d')
        logger.info(f"対象日: {target_date_str}")

        # 2. 各銘柄の20日平均出来高を計算
        logger.info(f"\n[2/5] {lookback_days}日平均出来高を計算中...")
        df_prices = df_prices.sort_values(['Code', 'Date'])

        # 20日平均出来高を計算（rolling）
        df_prices['AvgVolume'] = df_prices.groupby('Code')['Vo'].transform(
            lambda x: x.rolling(window=lookback_days, min_periods=lookback_days).mean()
        )

        # 対象日のデータのみ抽出
        df_target = df_prices[df_prices['Date'] == target_date_str].copy()
        logger.info(f"対象日のデータ: {len(df_target)}件")

        # 平均出来高が計算できている銘柄のみ
        df_target = df_target[df_target['AvgVolume'].notna()]
        logger.info(f"平均出来高計算可能: {len(df_target)}件")

        # 3. 出来高急増フィルタ
        logger.info(f"\n[3/7] 出来高急増フィルタ（{surge_threshold}倍以上）...")
        df_target['VolumeSurgeRatio'] = df_target['Vo'] / df_target['AvgVolume']
        df_filtered = df_target[df_target['VolumeSurgeRatio'] >= surge_threshold].copy()
        logger.info(f"出来高急増銘柄: {len(df_filtered)}件")

        if len(df_filtered) == 0:
            logger.warning("条件に合う銘柄が見つかりませんでした")
            return pd.DataFrame()

        # 3.5. 売買代金フィルタ（直近5日平均が3,000万円以上）
        logger.info(f"\n[3.5/7] 売買代金フィルタ（5日平均3,000万円以上）...")

        # 各銘柄の直近5日間の売買代金を計算
        df_prices['TradingValue'] = df_prices['C'] * df_prices['Vo']
        df_prices = df_prices.sort_values(['Code', 'Date'])

        # 5日平均売買代金を計算
        df_prices['AvgTradingValue5d'] = df_prices.groupby('Code')['TradingValue'].transform(
            lambda x: x.rolling(window=5, min_periods=5).mean()
        )

        # 対象日の5日平均売買代金を取得
        df_avg_trading = df_prices[df_prices['Date'] == target_date_str][['Code', 'AvgTradingValue5d']].copy()
        df_filtered = df_filtered.merge(df_avg_trading, on='Code', how='left')

        # 当日の売買代金も保持（表示用）
        df_filtered['TradingValue'] = df_filtered['C'] * df_filtered['Vo']

        before_count = len(df_filtered)
        df_filtered = df_filtered[df_filtered['AvgTradingValue5d'] >= 30_000_000].copy()
        excluded_count = before_count - len(df_filtered)
        logger.info(f"  除外: {excluded_count}件（薄商い株）, 残存: {len(df_filtered)}件")

        if len(df_filtered) == 0:
            logger.warning("売買代金フィルタ後、候補銘柄がなくなりました")
            return pd.DataFrame()

        # 4. 4桁コードフィルタ（ETF等の5桁コードを除外）
        # J-Quants APIのCodeは末尾0付き5桁整数で返される
        # 例: 1301 → 13010, 6501 → 65010
        # ETF等は末尾が0以外、または先頭4桁が範囲外（例: 48900, 17760）
        logger.info(f"\n[4/7] 4桁コードフィルタ（通常株式のみ）...")

        # Codeを数値に変換
        df_filtered['CodeNum'] = pd.to_numeric(df_filtered['Code'], errors='coerce')

        # フィルタ条件: 末尾が0 かつ 先頭4桁が1000-9999
        df_filtered = df_filtered[
            (df_filtered['CodeNum'] % 10 == 0) &           # 末尾が0
            (df_filtered['CodeNum'] // 10 >= 1000) &       # 先頭4桁が1000以上
            (df_filtered['CodeNum'] // 10 <= 9999)         # 先頭4桁が9999以下
        ].copy()

        # 4桁コードに変換（表示用）
        df_filtered['Code4'] = (df_filtered['CodeNum'] // 10).astype(int)
        df_filtered = df_filtered.drop(columns=['CodeNum'])

        logger.info(f"4桁コード銘柄: {len(df_filtered)}件")

        if len(df_filtered) == 0:
            logger.warning("4桁コード銘柄が見つかりませんでした")
            return pd.DataFrame()

        # 4.5. MktNmでETF・ETN・REIT除外（「その他」市場を除外）
        if self.listed_info is not None:
            logger.info(f"\n[4.5/7] ETF・ETN・REIT除外フィルタ...")

            # 上場銘柄情報とマージ（Code4を使用）
            # listed_infoのCodeは5桁（末尾0付き）なので、10で割って4桁に変換
            listed_info_4digit = self.listed_info.copy()
            listed_info_4digit['Code4'] = (pd.to_numeric(listed_info_4digit['Code'], errors='coerce') // 10).astype('Int64')

            # Code4でマージ（MktNmを取得）
            df_filtered = df_filtered.merge(
                listed_info_4digit[['Code4', 'MktNm']],
                on='Code4',
                how='left'
            )

            # MktNmが取得できた銘柄数を確認
            has_market_info = df_filtered['MktNm'].notna().sum()
            logger.info(f"  市場情報取得: {has_market_info}/{len(df_filtered)}件")

            # 除外前の件数を記録
            before_count = len(df_filtered)

            # 「その他」市場を除外（ETF・ETN・REIT等503銘柄）
            df_filtered = df_filtered[
                df_filtered['MktNm'] != 'その他'
            ].copy()

            excluded_count = before_count - len(df_filtered)
            logger.info(f"  ETF・ETN・REIT除外: {excluded_count}件除外, {len(df_filtered)}件残存")

            # MktNmカラムを削除（不要なため）
            df_filtered = df_filtered.drop(columns=['MktNm'], errors='ignore')

            if len(df_filtered) == 0:
                logger.warning("ETF・ETN・REIT除外後、候補銘柄がなくなりました")
                return pd.DataFrame()
        else:
            logger.warning(f"\n[4.5/7] 上場銘柄情報が取得できていないため、ETF・ETN・REIT除外をスキップ")

        # MA25は参考値として計算するが、フィルタには使用しない
        logger.info(f"\n25日移動平均を計算中（参考値）...")
        df_prices['MA25'] = df_prices.groupby('Code')['C'].transform(
            lambda x: x.rolling(window=25, min_periods=25).mean()
        )
        df_ma = df_prices[df_prices['Date'] == target_date_str][['Code', 'MA25']].copy()
        df_filtered = df_filtered.merge(df_ma, on='Code', how='left')
        logger.info(f"MA25計算完了")

        # 5. 時価総額フィルタ（50億円未満を除外）
        logger.info(f"\n[5/7] 時価総額フィルタ（50億円以上）...")
        # 時価総額（億円）= 終値 × 発行済株式数 / 100,000,000
        # ※ 発行済株式数は財務情報から取得（/fins/summary の ShOutFY フィールド）
        if self.statements is not None:
            # 財務情報から最新の発行済株式数を取得
            # 各銘柄の最新データのみを使用（DiscDate が最大のレコード）
            if 'DiscDate' in self.statements.columns:
                statements_latest = self.statements.sort_values('DiscDate').groupby('Code').tail(1).copy()
            else:
                # DiscDate がない場合は全データを使用（後方互換性）
                statements_latest = self.statements.copy()

            # 4桁コードに変換
            statements_latest['Code4'] = (pd.to_numeric(statements_latest['Code'], errors='coerce') // 10).astype('Int64')

            # 発行済株式数カラムが存在する場合はマージして除外
            # J-Quants V2 API /fins/summary の ShOutFY フィールド（期末発行済株式数）
            issued_shares_col = 'ShOutFY'
            if issued_shares_col in statements_latest.columns:
                # 発行済株式数を取得
                df_shares = statements_latest[['Code4', issued_shares_col]].copy()
                df_shares = df_shares.rename(columns={issued_shares_col: 'IssuedShares'})

                df_filtered = df_filtered.merge(
                    df_shares,
                    on='Code4',
                    how='left'
                )

                # 時価総額（億円）を計算
                df_filtered['MarketCap'] = (df_filtered['C'] * df_filtered['IssuedShares'] / 100_000_000).fillna(0)

                before_count = len(df_filtered)
                # 時価総額50億円未満を除外
                df_filtered = df_filtered[df_filtered['MarketCap'] >= 50].copy()
                excluded_count = before_count - len(df_filtered)
                logger.info(f"  除外: {excluded_count}件（小型株）, 残存: {len(df_filtered)}件")

                # IssuedShares列を削除
                df_filtered = df_filtered.drop(columns=['IssuedShares'], errors='ignore')

                if len(df_filtered) == 0:
                    logger.warning("時価総額フィルタ後、候補銘柄がなくなりました")
                    return pd.DataFrame()
            else:
                logger.warning(f"  発行済株式数情報がないため、時価総額フィルタをスキップ (列名: {issued_shares_col})")
                logger.warning(f"  利用可能なカラム: {sorted(statements_latest.columns.tolist())}")
                df_filtered['MarketCap'] = 0
        else:
            logger.warning("  財務情報がないため、時価総額フィルタをスキップ")
            df_filtered['MarketCap'] = 0

        # 6. 赤字銘柄フィルタ（財務データが必要 - 未実装）
        # TODO: J-Quants API /v1/fins/statements で財務データ取得後に実装
        # - 最終益が直近2期連続マイナスの銘柄を除外
        # - NetIncome（当期純利益）を確認
        logger.info(f"\n[6/7] 赤字銘柄フィルタ（未実装 - 財務データ取得が必要）...")
        logger.info("  スキップ: 財務データAPIアクセスが必要")

        # 7. 予算内フィルタ
        if self.budget:
            logger.info(f"\n[7/7] 予算内フィルタ (1単元100株 ≤ {self.budget:,}円)...")
            df_filtered['UnitPrice'] = df_filtered['C'] * 100
            df_filtered = df_filtered[df_filtered['UnitPrice'] <= self.budget].copy()
            logger.info(f"予算内銘柄: {len(df_filtered)}件")
        else:
            logger.info(f"\n[7/7] 予算フィルタはスキップ")

        # 結果を整形（出来高急増率で降順ソート）
        # Code4（4桁コード）をCodeとして使用
        result = df_filtered[[
            'Code4', 'Date', 'O', 'H', 'L', 'C', 'Vo',
            'AvgVolume', 'VolumeSurgeRatio', 'MA25', 'TradingValue',
            'AvgTradingValue5d', 'MarketCap'
        ]].copy()
        result = result.rename(columns={'Code4': 'Code'})
        result = result.sort_values('VolumeSurgeRatio', ascending=False)
        result = result.reset_index(drop=True)

        logger.info("\n" + "=" * 60)
        logger.success(f"出来高急増スクリーニング完了: {len(result)}銘柄")
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
