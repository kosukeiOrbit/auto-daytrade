"""
分足データを使った高精度バックテストエンジン

日足近似では判定できない「利確と損切どちらが先に到達したか」を
分足の時系列走査で正確にシミュレーションする。
"""
import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger


class MinuteDataLoader:
    """分足データのロードとキャッシュ"""

    def __init__(self, base_dir="equities_bars_minute"):
        self.base_dir = base_dir
        self._month_cache = {}  # (year, month) -> DataFrame

    def _find_gz_file(self, year, month):
        """指定年月のgzファイルパスを探す"""
        ym = f"{year}{month:02d}"
        patterns = [
            os.path.join(self.base_dir, str(year), f"equities_bars_minute_{ym}.csv.gz"),
            # 2025フォルダに202601が入っているケースに対応
            os.path.join(self.base_dir, str(year - 1), f"equities_bars_minute_{ym}.csv.gz"),
        ]
        # globでも探す
        for p in patterns:
            if os.path.exists(p):
                return p
        # 全ディレクトリから探す
        all_files = glob.glob(os.path.join(self.base_dir, "**", f"*_{ym}.csv.gz"), recursive=True)
        if all_files:
            return all_files[0]
        return None

    def _load_month(self, year, month):
        """1ヶ月分のデータをロード（キャッシュ付き）"""
        key = (year, month)
        if key in self._month_cache:
            return self._month_cache[key]

        gz_path = self._find_gz_file(year, month)
        if gz_path is None:
            logger.debug(f"分足データなし: {year}-{month:02d}")
            self._month_cache[key] = None
            return None

        logger.info(f"分足データ読み込み中: {gz_path}")
        df = pd.read_csv(
            gz_path,
            dtype={'Code': str},
            parse_dates=False,
        )
        # Code を4桁に正規化（数字のみの5桁コードは//10、英字混在はそのまま）
        df['Code4'] = df['Code'].apply(self._normalize_code)
        self._month_cache[key] = df
        logger.info(f"  {len(df):,}行ロード完了 ({df['Date'].nunique()}日)")
        return df

    @staticmethod
    def _normalize_code(code_str):
        """5桁数字コードを4桁に変換。英字混在コードはそのまま"""
        try:
            code_int = int(code_str)
            if code_int >= 10000:
                return str(code_int // 10)
            return code_str
        except ValueError:
            return code_str

    def get_minute_bars(self, code, date):
        """
        指定銘柄・指定日の分足データを取得

        Args:
            code: 4桁銘柄コード（str）
            date: datetime or str (YYYY-MM-DD or YYYYMMDD)

        Returns:
            DataFrame: 分足データ（時系列順）or None
        """
        if isinstance(date, str):
            if len(date) == 8:
                date = datetime.strptime(date, '%Y%m%d')
            else:
                date = datetime.strptime(date, '%Y-%m-%d')

        df_month = self._load_month(date.year, date.month)
        if df_month is None:
            return None

        date_str = date.strftime('%Y-%m-%d')
        code_str = str(code)

        mask = (df_month['Date'] == date_str) & (df_month['Code4'] == code_str)
        df_day = df_month[mask].copy()

        if len(df_day) == 0:
            return None

        df_day = df_day.sort_values('Time').reset_index(drop=True)
        return df_day

    def clear_cache(self):
        """メモリキャッシュをクリア"""
        self._month_cache.clear()

    def preload_month(self, year, month):
        """指定月を事前ロード"""
        self._load_month(year, month)


class MinuteBacktest:
    """分足ベースのバックテストエンジン"""

    def __init__(
        self,
        initial_capital=400_000,
        take_profit_pct=2.0,
        stop_loss_pct=-1.0,
        max_gap_pct=8.0,
        prev_day_surge_pct=5.0,
    ):
        self.initial_capital = initial_capital
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.max_gap_pct = max_gap_pct
        self.prev_day_surge_pct = prev_day_surge_pct
        self.loader = MinuteDataLoader()
        self.trades = []

    def simulate_trade_minute(self, code, entry_price, date):
        """
        分足ベースで1トレードをシミュレーション

        Args:
            code: 4桁銘柄コード
            entry_price: エントリー価格（始値）
            date: 取引日

        Returns:
            dict or None
        """
        bars = self.loader.get_minute_bars(code, date)
        if bars is None or len(bars) == 0:
            return None

        take_profit_price = entry_price * (1 + self.take_profit_pct / 100)
        stop_loss_price = entry_price * (1 + self.stop_loss_pct / 100)

        mfe_pct = 0.0
        mae_pct = 0.0
        exit_price = None
        exit_reason = None
        exit_time = None

        for _, bar in bars.iterrows():
            time_str = bar['Time']
            bar_h = bar['H']
            bar_l = bar['L']
            bar_c = bar['C']

            # MFE/MAE更新
            if entry_price > 0:
                high_pct = (bar_h - entry_price) / entry_price * 100
                low_pct = (bar_l - entry_price) / entry_price * 100
                mfe_pct = max(mfe_pct, high_pct)
                mae_pct = min(mae_pct, low_pct)

            # 利確・損切判定（同一バー内で両方到達する場合は損切優先）
            hit_stop = bar_l <= stop_loss_price
            hit_target = bar_h >= take_profit_price

            if hit_stop and hit_target:
                # 同一バーで両方到達 → 始値からどちらが近いかで判定
                # 保守的に損切優先
                exit_price = stop_loss_price
                exit_reason = "損切り"
                exit_time = time_str
                break
            elif hit_stop:
                exit_price = stop_loss_price
                exit_reason = "損切り"
                exit_time = time_str
                break
            elif hit_target:
                exit_price = take_profit_price
                exit_reason = "利確"
                exit_time = time_str
                break

            # 11:30の前場引け判定
            if time_str >= '11:30' and time_str < '12:30':
                current_pct = (bar_c - entry_price) / entry_price * 100
                if current_pct < 0:
                    exit_price = bar_c
                    exit_reason = "前場引け"
                    exit_time = time_str
                    break

        # 引けまで持ち越し
        if exit_price is None:
            last_bar = bars.iloc[-1]
            exit_price = last_bar['C']
            exit_reason = "大引け"
            exit_time = last_bar['Time']

        profit_loss = exit_price - entry_price
        profit_loss_pct = (profit_loss / entry_price * 100) if entry_price > 0 else 0

        return {
            'exit_price': exit_price,
            'profit_loss': profit_loss,
            'profit_loss_pct': profit_loss_pct,
            'exit_reason': exit_reason,
            'exit_time': exit_time,
            'mfe_pct': round(mfe_pct, 4),
            'mae_pct': round(mae_pct, 4),
        }

    def run(self):
        """
        全candidates CSVを読み込み、分足バックテストを実行
        """
        logger.info("=" * 60)
        logger.info("分足バックテスト開始")
        logger.info(f"利確: +{self.take_profit_pct}% / 損切: {self.stop_loss_pct}%")
        logger.info(f"初期資金: {self.initial_capital:,}円")
        logger.info("=" * 60)

        # candidates CSV読み込み
        files = sorted(glob.glob("data/candidates_*.csv"))
        files = [f for f in files if 'test' not in f.lower()]
        logger.info(f"候補CSV: {len(files)}ファイル")

        # 日足キャッシュ（前日騰落率チェック用）
        import pickle
        price_cache = {}
        for f in sorted(glob.glob("data/cache/prices_*.pkl")):
            with open(f, 'rb') as fh:
                df = pickle.load(fh)
                if len(df) > 0:
                    date_str = os.path.basename(f).replace('prices_', '').replace('.pkl', '')
                    price_cache[date_str] = df

        prev_month = None

        for file_path in files:
            filename = os.path.basename(file_path)
            date_str = filename.replace("candidates_", "").replace(".csv", "")

            try:
                trade_date = datetime.strptime(date_str, '%Y%m%d')
            except ValueError:
                continue

            # 月が変わったらキャッシュクリア（メモリ節約）
            current_month = (trade_date.year, trade_date.month)
            if prev_month is not None and current_month != prev_month:
                # 前月のキャッシュを解放（現在月は残す）
                old_keys = [k for k in self.loader._month_cache if k != current_month]
                for k in old_keys:
                    del self.loader._month_cache[k]
            prev_month = current_month

            # CSV読み込み
            candidates_df = pd.read_csv(file_path, encoding='utf-8-sig')
            if len(candidates_df) == 0:
                continue

            # フィルタ適用
            candidates_df = self._apply_filters(candidates_df, trade_date, price_cache)
            if len(candidates_df) == 0:
                continue

            # 1日1銘柄: material_strength優先で選択
            top = self._select_top_candidate(candidates_df)
            if top is None:
                continue

            code_raw = str(top['Code'])
            # 5桁数字コード（例: 40170）を4桁（4017）に変換
            try:
                code_int = int(code_raw)
                code = str(code_int // 10) if code_int >= 10000 else code_raw
            except ValueError:
                code = code_raw
            name = str(top.get('Name', '') or '')

            # エントリー価格チェック
            entry_price = top.get('Open') or top.get('O')
            if entry_price is None or pd.isna(entry_price) or entry_price <= 0:
                # 分足の09:00始値を使用
                bars = self.loader.get_minute_bars(code, trade_date)
                if bars is not None and len(bars) > 0:
                    entry_price = bars.iloc[0]['O']
                else:
                    continue

            # 予算チェック
            unit_price = entry_price * 100
            if unit_price > self.initial_capital:
                continue

            # ギャップアップチェック
            prev_close = top.get('Close') or top.get('C')
            if prev_close and prev_close > 0:
                gap_pct = (entry_price / prev_close - 1) * 100
                if gap_pct > self.max_gap_pct:
                    continue
            else:
                gap_pct = None

            # 分足シミュレーション
            result = self.simulate_trade_minute(code, entry_price, trade_date)
            if result is None:
                logger.debug(f"{date_str} {code}: 分足データなし")
                continue

            qty = int(self.initial_capital // (entry_price * 100)) * 100
            if qty <= 0:
                qty = 100

            trade_record = {
                'date': date_str,
                'code': code,
                'name': name,
                'entry_price': entry_price,
                'exit_price': result['exit_price'],
                'qty': qty,
                'profit_loss': result['profit_loss'] * qty,
                'profit_loss_pct': result['profit_loss_pct'],
                'exit_reason': result['exit_reason'],
                'exit_time': result['exit_time'],
                'mfe_pct': result['mfe_pct'],
                'mae_pct': result['mae_pct'],
                'material_strength': top.get('material_strength', ''),
                'volume_surge': top.get('VolumeSurgeRatio', 0),
                'gap_pct': gap_pct,
            }
            self.trades.append(trade_record)

            pnl = trade_record['profit_loss']
            logger.info(
                f"{date_str} {code} {name[:10]}: "
                f"{entry_price:.0f}→{result['exit_price']:.0f} "
                f"({result['exit_reason']}) "
                f"{pnl:+,.0f}円 "
                f"MFE:{result['mfe_pct']:+.2f}% MAE:{result['mae_pct']:+.2f}%"
            )

        logger.info(f"\n完了: {len(self.trades)}トレード")
        return self.trades

    def _apply_filters(self, df, trade_date, price_cache):
        """候補銘柄にフィルタ適用"""
        # 材料フィルタ
        if 'material_strength' in df.columns:
            df = df[df['material_strength'].isin(['強', '中'])].copy()

        if len(df) == 0:
            return df

        # 前日騰落率フィルタ（+5%以上除外）
        date_key = trade_date.strftime('%Y%m%d')
        # 前日・前々日のデータから計算
        prev_dates = []
        for d in range(1, 8):
            check = trade_date - timedelta(days=d)
            ck = check.strftime('%Y%m%d')
            if ck in price_cache:
                prev_dates.append((ck, price_cache[ck]))
            if len(prev_dates) >= 2:
                break

        if len(prev_dates) >= 2:
            df_prev1 = prev_dates[0][1]  # 前日
            df_prev2 = prev_dates[1][1]  # 前々日

            exclude_codes = set()
            for _, row in df.iterrows():
                code = str(row['Code'])
                code_5 = code + '0' if len(code) == 4 else code
                # 前日終値
                m1 = df_prev1[df_prev1['Code'].astype(str) == code_5]
                m2 = df_prev2[df_prev2['Code'].astype(str) == code_5]
                if len(m1) > 0 and len(m2) > 0:
                    c1 = m1.iloc[0]['C']
                    c2 = m2.iloc[0]['C']
                    if c2 > 0:
                        change = (c1 - c2) / c2 * 100
                        if change >= self.prev_day_surge_pct:
                            exclude_codes.add(code)

            if exclude_codes:
                df = df[~df['Code'].astype(str).isin(exclude_codes)].copy()

        return df

    def _select_top_candidate(self, df):
        """1日1銘柄: material_strength優先で選択"""
        if 'material_strength' in df.columns and 'VolumeSurgeRatio' in df.columns:
            strong = df[df['material_strength'] == '強']
            if len(strong) > 0:
                return strong.sort_values('VolumeSurgeRatio', ascending=False).iloc[0]
            medium = df[df['material_strength'] == '中']
            if len(medium) > 0:
                return medium.sort_values('VolumeSurgeRatio', ascending=False).iloc[0]

        if 'VolumeSurgeRatio' in df.columns:
            return df.sort_values('VolumeSurgeRatio', ascending=False).iloc[0]

        return df.iloc[0] if len(df) > 0 else None

    def get_summary(self):
        """バックテスト結果のサマリーを返す"""
        if not self.trades:
            return None

        df = pd.DataFrame(self.trades)
        total = len(df)
        wins = len(df[df['profit_loss'] > 0])
        losses = len(df[df['profit_loss'] < 0])
        win_rate = wins / total * 100

        total_pnl = df['profit_loss'].sum()
        avg_win = df[df['profit_loss'] > 0]['profit_loss'].mean() if wins > 0 else 0
        avg_loss = df[df['profit_loss'] < 0]['profit_loss'].mean() if losses > 0 else 0
        r_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

        # 最大連敗
        max_cl = 0
        cl = 0
        for pl in df['profit_loss']:
            if pl < 0:
                cl += 1
                max_cl = max(max_cl, cl)
            else:
                cl = 0

        # 最大DD
        cum = df['profit_loss'].cumsum()
        dd = (cum - cum.cummax()).min()

        # 年利換算
        days = (datetime.strptime(df['date'].iloc[-1], '%Y%m%d') -
                datetime.strptime(df['date'].iloc[0], '%Y%m%d')).days
        years = days / 365 if days > 0 else 1
        annual_return = (total_pnl / self.initial_capital * 100) / years

        # MFE/MAE統計
        avg_mfe = df['mfe_pct'].mean()
        avg_mae = df['mae_pct'].mean()

        # 決済理由内訳
        reasons = df['exit_reason'].value_counts().to_dict()

        return {
            'trades': total,
            'wins': wins,
            'losses': losses,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'annual_return': annual_return,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'r_ratio': r_ratio,
            'max_consecutive_loss': max_cl,
            'max_drawdown': dd,
            'avg_mfe': avg_mfe,
            'avg_mae': avg_mae,
            'exit_reasons': reasons,
            'period_days': days,
        }

    def save_results(self, filepath=None):
        """結果をCSVに保存"""
        if not self.trades:
            return None
        if filepath is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = f"backtest_results/minute_backtest_{timestamp}.csv"
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        df = pd.DataFrame(self.trades)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"結果保存: {filepath}")
        return filepath
