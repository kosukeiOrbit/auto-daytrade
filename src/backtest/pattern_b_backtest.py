"""
パターンB 分足バックテストエンジン

9:30〜10:30の場中動意検出→エントリー→決済を
分足データでシミュレーションする。

【バックテストの設計上の制約】

本バックテストは candidates_*.csv（前日スクリーニング済み銘柄）を
パターンBの走査対象として使用している。

本番のパターンBとの乖離：

■ 過大評価の要因（バックテストが有利に出る）
- candidates内の銘柄は前日に出来高急増・材料ありでフィルタ済みのため
  翌日も動きやすい銘柄が多く、条件を満たしやすい
- 本番ランキングAPIは上位10銘柄のみだが、
  バックテストはcandidates全銘柄（数十〜100銘柄）を走査している

■ 過小評価の要因（本番が有利に出る可能性）
- 本番ではcandidates外の銘柄もランキングに入りうるため
  エントリー機会が増える可能性がある

■ 現実的な代替案が困難な理由
- 全銘柄の分足走査は1ヶ月約1000万行×全銘柄で非現実的
- 現設計を維持しつつ、結果は参考値として扱うこと

→ バックテスト結果は過大評価気味の可能性があるため、
  本番稼働後の実績と比較して乖離を検証すること。
"""
import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from .minute_backtest import MinuteDataLoader


class PatternBBacktest:
    """パターンB 分足バックテスト"""

    def __init__(
        self,
        initial_capital=400_000,
        take_profit_pct=2.0,
        stop_loss_pct=-1.0,
        max_gap_from_open_pct=3.0,
        volume_surge_ratio=2.0,
        volume_lookback=20,
        trend_min_up=3,
        trend_window=5,
    ):
        self.initial_capital = initial_capital
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.max_gap_from_open_pct = max_gap_from_open_pct
        self.volume_surge_ratio = volume_surge_ratio
        self.volume_lookback = volume_lookback
        self.trend_min_up = trend_min_up
        self.trend_window = trend_window
        self.loader = MinuteDataLoader()
        self.trades = []

    def _calc_vwap(self, bars):
        """当日9:00からの累積VWAPを計算"""
        cumulative_va = bars['Va'].cumsum()
        cumulative_vo = bars['Vo'].cumsum()
        vwap = cumulative_va / cumulative_vo.replace(0, np.nan)
        return vwap

    def _check_entry(self, bars, idx, opening_price):
        """
        指定インデックスの分足でエントリー条件を判定

        Returns:
            True/False
        """
        if idx < self.trend_window:
            return False

        bar = bars.iloc[idx]
        current_price = bar['C']
        current_volume = bar['Vo']
        time_str = bar['Time']

        # 時間帯チェック（9:30〜10:30）
        if time_str < '09:30' or time_str > '10:30':
            return False

        # 1. 出来高急増: 直近N分平均の倍率以上
        start_idx = max(0, idx - self.volume_lookback)
        avg_vol = bars.iloc[start_idx:idx]['Vo'].mean()
        if avg_vol <= 0 or current_volume < avg_vol * self.volume_surge_ratio:
            return False

        # 2. 現在値 > VWAP
        vwap_series = self._calc_vwap(bars.iloc[:idx + 1])
        current_vwap = vwap_series.iloc[-1]
        if pd.isna(current_vwap) or current_price <= current_vwap:
            return False

        # 3. 寄り付きから+3%以内
        if opening_price > 0:
            gap_pct = (current_price / opening_price - 1) * 100
            if gap_pct > self.max_gap_from_open_pct or gap_pct < 0:
                return False

        # 4. 直近5本の上昇トレンド（3本以上切り上がり）
        recent = bars.iloc[idx - self.trend_window + 1:idx + 1]
        closes = recent['C'].values
        up_count = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i - 1])
        if up_count < self.trend_min_up:
            return False

        return True

    def _simulate_exit(self, bars, entry_idx, entry_price):
        """
        エントリー後の決済シミュレーション

        Returns:
            dict: 決済結果
        """
        take_profit_price = entry_price * (1 + self.take_profit_pct / 100)
        stop_loss_price = entry_price * (1 + self.stop_loss_pct / 100)

        mfe_pct = 0.0
        mae_pct = 0.0

        for i in range(entry_idx + 1, len(bars)):
            bar = bars.iloc[i]
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

            # 利確・損切判定
            hit_stop = bar_l <= stop_loss_price
            hit_target = bar_h >= take_profit_price

            if hit_stop and hit_target:
                return {
                    'exit_price': stop_loss_price,
                    'exit_reason': '損切り',
                    'exit_time': time_str,
                    'mfe_pct': round(mfe_pct, 4),
                    'mae_pct': round(mae_pct, 4),
                }
            elif hit_stop:
                return {
                    'exit_price': stop_loss_price,
                    'exit_reason': '損切り',
                    'exit_time': time_str,
                    'mfe_pct': round(mfe_pct, 4),
                    'mae_pct': round(mae_pct, 4),
                }
            elif hit_target:
                return {
                    'exit_price': take_profit_price,
                    'exit_reason': '利確',
                    'exit_time': time_str,
                    'mfe_pct': round(mfe_pct, 4),
                    'mae_pct': round(mae_pct, 4),
                }

            # 前場引け
            if time_str >= '11:30' and time_str < '12:30':
                current_pct = (bar_c - entry_price) / entry_price * 100
                if current_pct < 0:
                    return {
                        'exit_price': bar_c,
                        'exit_reason': '前場引け',
                        'exit_time': time_str,
                        'mfe_pct': round(mfe_pct, 4),
                        'mae_pct': round(mae_pct, 4),
                    }

        # 大引け
        last_bar = bars.iloc[-1]
        return {
            'exit_price': last_bar['C'],
            'exit_reason': '大引け',
            'exit_time': last_bar['Time'],
            'mfe_pct': round(mfe_pct, 4),
            'mae_pct': round(mae_pct, 4),
        }

    def run(self):
        """
        パターンBバックテスト実行（複数トレード対応）

        1日に複数回トレード可能。ポジション保有中は新規エントリーしない。
        決済後は即座に次のエントリー候補を探す。
        """
        logger.info("=" * 60)
        logger.info("パターンB 分足バックテスト開始（複数トレード対応）")
        logger.info(f"利確: +{self.take_profit_pct}% / 損切: {self.stop_loss_pct}%")
        logger.info(f"エントリー窓: 9:30〜10:30")
        logger.info(f"条件: VWAP上 & 寄り+{self.max_gap_from_open_pct}%以内 & "
                     f"出来高{self.volume_surge_ratio}倍 & {self.trend_min_up}/{self.trend_window}本上昇")
        logger.info(f"初期資金: {self.initial_capital:,}円")
        logger.info("=" * 60)

        files = sorted(glob.glob("data/candidates_*.csv"))
        files = [f for f in files if 'test' not in f.lower()]
        logger.info(f"候補CSV: {len(files)}ファイル")

        prev_month = None

        for file_path in files:
            filename = os.path.basename(file_path)
            date_str = filename.replace("candidates_", "").replace(".csv", "")

            try:
                trade_date = datetime.strptime(date_str, '%Y%m%d')
            except ValueError:
                continue

            # 月が変わったらキャッシュクリア
            current_month = (trade_date.year, trade_date.month)
            if prev_month is not None and current_month != prev_month:
                old_keys = [k for k in self.loader._month_cache if k != current_month]
                for k in old_keys:
                    del self.loader._month_cache[k]
            prev_month = current_month

            # CSV読み込み
            candidates_df = pd.read_csv(file_path, encoding='utf-8-sig')
            if len(candidates_df) == 0:
                continue

            # 材料フィルタ
            if 'material_strength' in candidates_df.columns:
                candidates_df = candidates_df[
                    candidates_df['material_strength'].isin(['強', '中'])
                ]
            if len(candidates_df) == 0:
                continue

            # 銘柄コードを4桁に変換、分足データをプリロード
            code_bars = {}  # code -> (bars, opening_price)
            code_names = {}
            for _, row in candidates_df.iterrows():
                code_raw = str(row['Code'])
                try:
                    code_int = int(code_raw)
                    code = str(code_int // 10) if code_int >= 10000 else code_raw
                except ValueError:
                    code = code_raw

                if code in code_bars:
                    continue

                bars = self.loader.get_minute_bars(code, trade_date)
                if bars is None or len(bars) < self.trend_window + 5:
                    continue

                opening_price = bars.iloc[0]['O']
                if opening_price <= 0 or opening_price * 100 > self.initial_capital:
                    continue

                code_bars[code] = (bars, opening_price)
                name = str(row.get('Name', '') or '')
                code_names[code] = name if name != 'nan' else ''

            if not code_bars:
                continue

            # 1日の複数トレードシミュレーション
            day_trades = self._simulate_day(date_str, code_bars, code_names)
            self.trades.extend(day_trades)

            for t in day_trades:
                logger.info(
                    f"{t['date']} {t['code']} @{t['entry_time']}: "
                    f"{t['entry_price']:.0f}→{t['exit_price']:.0f} "
                    f"({t['exit_reason']}) "
                    f"{t['profit_loss']:+,.0f}円 "
                    f"MFE:{t['mfe_pct']:+.2f}% MAE:{t['mae_pct']:+.2f}%"
                )

        logger.info(f"\n完了: {len(self.trades)}トレード")
        return self.trades

    def _simulate_day(self, date_str, code_bars, code_names):
        """
        1日分の複数トレードをシミュレーション

        ポジションなし→エントリー条件走査→エントリー→決済→再走査のループ
        """
        trades = []
        traded_codes = set()  # 同一銘柄の再エントリーは禁止
        position = None  # 現在のポジション（None=なし）
        position_exit_time = '09:00'  # 前回決済時刻（これ以降から次を探す）

        # 全候補のエントリー可能ポイントを事前計算
        entry_candidates = []
        for code, (bars, opening_price) in code_bars.items():
            for idx in range(self.trend_window, len(bars)):
                time_str = bars.iloc[idx]['Time']
                if time_str < '09:30':
                    continue
                if time_str > '10:30':
                    break
                if self._check_entry(bars, idx, opening_price):
                    entry_candidates.append({
                        'code': code,
                        'bars': bars,
                        'idx': idx,
                        'time': time_str,
                        'price': bars.iloc[idx]['C'],
                        'opening_price': opening_price,
                    })
                    break  # 各銘柄の最初のエントリーポイントのみ

        # 時刻順にソート
        entry_candidates.sort(key=lambda x: x['time'])

        for cand in entry_candidates:
            code = cand['code']

            # 同一銘柄は再エントリーしない
            if code in traded_codes:
                continue

            # 前回決済前のエントリーはスキップ
            if cand['time'] <= position_exit_time:
                continue

            entry_price = cand['price']
            entry_idx = cand['idx']
            bars = cand['bars']
            opening_price = cand['opening_price']

            # 決済シミュレーション
            result = self._simulate_exit(bars, entry_idx, entry_price)

            qty = int(self.initial_capital // (entry_price * 100)) * 100
            if qty <= 0:
                qty = 100

            gap_pct = (entry_price / opening_price - 1) * 100

            trade_record = {
                'date': date_str,
                'code': code,
                'name': code_names.get(code, ''),
                'entry_price': entry_price,
                'exit_price': result['exit_price'],
                'qty': qty,
                'profit_loss': (result['exit_price'] - entry_price) * qty,
                'profit_loss_pct': (result['exit_price'] - entry_price) / entry_price * 100,
                'exit_reason': result['exit_reason'],
                'entry_time': cand['time'],
                'exit_time': result['exit_time'],
                'mfe_pct': result['mfe_pct'],
                'mae_pct': result['mae_pct'],
                'gap_from_open_pct': round(gap_pct, 2),
            }
            trades.append(trade_record)
            traded_codes.add(code)

            # 決済時刻を更新（次のエントリーはこれ以降）
            position_exit_time = result['exit_time']

            # 10:30以降に決済した場合は新規エントリー不可
            if position_exit_time > '10:30':
                break

        return trades

    def get_summary(self):
        """バックテスト結果のサマリー"""
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

        # エントリー時刻分布
        entry_time_dist = df['entry_time'].apply(
            lambda t: t[:4] + '0' if len(t) >= 4 else t  # 10分単位にまとめる
        ).value_counts().sort_index().to_dict()

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
            'entry_time_dist': entry_time_dist,
            'period_days': days,
        }

    def save_results(self, filepath=None):
        """結果をCSVに保存"""
        if not self.trades:
            return None
        if filepath is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = f"backtest_results/pattern_b_backtest_{timestamp}.csv"
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        df = pd.DataFrame(self.trades)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"結果保存: {filepath}")
        return filepath
