"""
ペーパートレードシミュレーター
candidates_YYYYMMDD.csv と J-Quants 株価データを使用して
trade_executor.py のロジックを再現

実行方法:
    python src/backtest/paper_trading_simulator.py
"""
import os
import glob
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from src.utils.jquants_client import JQuantsClient
from src.utils.notifier import DiscordNotifier


class PaperTradingSimulator:
    """ペーパートレードシミュレーター"""

    def __init__(self):
        """初期化"""
        self.jquants = JQuantsClient()
        self.notifier = DiscordNotifier()
        self.trades = []  # 個別トレード結果

        logger.info("ペーパートレードシミュレーター初期化")

    def load_candidate_files(self):
        """
        data/ フォルダから全 candidates_YYYYMMDD.csv を検索

        Returns:
            list: [(date: str, file_path: str), ...]
        """
        # data/とresults/の両方を検索
        pattern1 = os.path.join("data", "candidates_*.csv")
        pattern2 = os.path.join("results", "candidates_*.csv")
        files = glob.glob(pattern1) + glob.glob(pattern2)

        # test用のファイルは除外
        files = [f for f in files if 'test' not in f.lower()]

        if len(files) == 0:
            logger.warning("候補銘柄CSVが見つかりません")
            return []

        # ファイル名から日付を抽出してソート
        file_dates = []
        for file_path in files:
            filename = os.path.basename(file_path)
            # candidates_20250115.csv → 20250115
            date_str = filename.replace("candidates_", "").replace(".csv", "")
            file_dates.append((date_str, file_path))

        file_dates.sort()
        logger.info(f"候補銘柄CSV: {len(file_dates)}ファイル検出")

        return file_dates

    def simulate_trade(self, code, entry_price, ohlcv):
        """
        1銘柄1日のトレードシミュレーション

        Args:
            code: 銘柄コード
            entry_price: エントリー価格（始値）
            ohlcv: 当日のOHLCVデータ（Series）

        Returns:
            dict: {
                'exit_price': float,
                'profit_loss': float,
                'profit_loss_pct': float,
                'exit_reason': str
            } or None
        """
        if ohlcv is None or pd.isna(ohlcv['High']) or pd.isna(ohlcv['Low']):
            logger.warning(f"{code}: OHLCVデータ不足")
            return None

        high = ohlcv['High']
        low = ohlcv['Low']
        close = ohlcv['Close']

        # 損切り・利確ライン（デイトレルール）
        stop_price = entry_price * 0.99   # -1%
        target_price = entry_price * 1.02  # +2%

        # シミュレーションロジック
        # 1. 安値が損切りラインに到達 → 損切り優先
        if low <= stop_price:
            exit_price = stop_price
            exit_reason = "損切り"
        # 2. 高値が利確ラインに到達 → 利確
        elif high >= target_price:
            exit_price = target_price
            exit_reason = "利確"
        # 3. どちらも発生しない → 終値で決済
        else:
            exit_price = close
            exit_reason = "終値決済"

        profit_loss = exit_price - entry_price
        profit_loss_pct = (profit_loss / entry_price) * 100

        return {
            'exit_price': exit_price,
            'profit_loss': profit_loss,
            'profit_loss_pct': profit_loss_pct,
            'exit_reason': exit_reason
        }

    def run_simulation(self):
        """
        全候補銘柄CSVに対してシミュレーション実行
        """
        logger.info("=" * 60)
        logger.info("ペーパートレードシミュレーション開始")
        logger.info("=" * 60)

        # 候補銘柄CSV読み込み
        file_dates = self.load_candidate_files()

        if len(file_dates) == 0:
            logger.error("シミュレーション対象がありません")
            return

        # 各CSVに対してシミュレーション
        for date_str, file_path in file_dates:
            logger.info(f"\n処理中: {date_str} ({file_path})")

            try:
                # CSV読み込み
                candidates_df = pd.read_csv(file_path, encoding='utf-8-sig')

                if len(candidates_df) == 0:
                    logger.warning(f"{date_str}: 候補銘柄なし")
                    continue

                logger.info(f"{date_str}: 候補銘柄 {len(candidates_df)}件")

                # 1日1銘柄集中: material_strength優先で選択
                top_candidate = None
                selection_reason = ""

                if 'material_strength' in candidates_df.columns and 'VolumeSurgeRatio' in candidates_df.columns:
                    # 優先順位1: material_strength == '強' の中でVolumeSurgeRatio最大
                    strong_df = candidates_df[candidates_df['material_strength'] == '強']
                    if len(strong_df) > 0:
                        strong_df = strong_df.sort_values('VolumeSurgeRatio', ascending=False)
                        top_candidate = strong_df.iloc[0]
                        selection_reason = f"材料強 (VolumeSurgeRatio={top_candidate.get('VolumeSurgeRatio', 0):.1f}倍)"
                    else:
                        # 優先順位2: material_strength == '中' の中でVolumeSurgeRatio最大
                        medium_df = candidates_df[candidates_df['material_strength'] == '中']
                        if len(medium_df) > 0:
                            medium_df = medium_df.sort_values('VolumeSurgeRatio', ascending=False)
                            top_candidate = medium_df.iloc[0]
                            selection_reason = f"材料中 (VolumeSurgeRatio={top_candidate.get('VolumeSurgeRatio', 0):.1f}倍)"
                        else:
                            # 優先順位3: VolumeSurgeRatio最大
                            candidates_df = candidates_df.sort_values('VolumeSurgeRatio', ascending=False)
                            top_candidate = candidates_df.iloc[0]
                            selection_reason = f"VolumeSurgeRatio最大 ({top_candidate.get('VolumeSurgeRatio', 0):.1f}倍)"
                elif 'VolumeSurgeRatio' in candidates_df.columns:
                    # material_strengthカラムがない場合: VolumeSurgeRatio最大
                    candidates_df = candidates_df.sort_values('VolumeSurgeRatio', ascending=False)
                    top_candidate = candidates_df.iloc[0]
                    selection_reason = f"VolumeSurgeRatio最大 ({top_candidate.get('VolumeSurgeRatio', 0):.1f}倍・材料情報なし)"
                else:
                    # どちらもない場合は最初の銘柄
                    top_candidate = candidates_df.iloc[0]
                    selection_reason = "最初の銘柄（カラムなし）"
                    logger.warning(f"{date_str}: VolumeSurgeRatioカラムなし")

                logger.info(f"{date_str}: 最上位銘柄を選択 ({selection_reason})")

                # 日付を datetime に変換
                trade_date = datetime.strptime(date_str, '%Y%m%d')

                # 選択した1銘柄のみシミュレーション
                code = str(top_candidate['Code'])
                name = top_candidate.get('Name', '')

                # 当日の株価データ取得
                ohlcv = self.get_daily_ohlcv(code, trade_date)

                if ohlcv is None:
                    logger.warning(f"{code} {name}: 株価データ取得失敗")
                    continue

                # エントリー価格（始値）
                entry_price = ohlcv['Open']

                if pd.isna(entry_price) or entry_price <= 0:
                    logger.warning(f"{code} {name}: 始値データなし")
                    continue

                # トレードシミュレーション
                result = self.simulate_trade(code, entry_price, ohlcv)

                if result is None:
                    continue

                # トレード記録
                trade_record = {
                    'date': date_str,
                    'code': code,
                    'name': name,
                    'entry_price': entry_price,
                    'exit_price': result['exit_price'],
                    'profit_loss': result['profit_loss'],
                    'profit_loss_pct': result['profit_loss_pct'],
                    'exit_reason': result['exit_reason']
                }

                self.trades.append(trade_record)

                logger.info(
                    f"✅ {code} {name}: "
                    f"始値={entry_price:.0f}円 → {result['exit_reason']}={result['exit_price']:.0f}円 "
                    f"({result['profit_loss_pct']:+.2f}%)"
                )

            except Exception as e:
                logger.error(f"{date_str}: シミュレーションエラー: {e}")
                continue

        logger.info("=" * 60)
        logger.info("シミュレーション完了")
        logger.info("=" * 60)

    def get_daily_ohlcv(self, code, date):
        """
        指定日の日足OHLCVデータを取得

        Args:
            code: 銘柄コード
            date: 日付（datetime）

        Returns:
            pd.Series: OHLCV or None
        """
        try:
            # J-Quants APIから当日の株価データを取得
            df = self.jquants.get_daily_quotes(code=code, date=date)

            if df is None or len(df) == 0:
                return None

            # 最初の行を返す（通常1銘柄1日分）
            row = df.iloc[0]

            # J-Quants APIのカラム名: O, H, L, C, Vo
            result = pd.Series({
                'Open': row.get('O'),
                'High': row.get('H'),
                'Low': row.get('L'),
                'Close': row.get('C'),
                'Volume': row.get('Vo')
            })

            return result

        except Exception as e:
            logger.debug(f"{code}: 株価データ取得エラー: {e}")
            return None

    def calculate_metrics(self):
        """
        パフォーマンス指標を計算

        Returns:
            dict: 各種メトリクス
        """
        if len(self.trades) == 0:
            logger.warning("トレードデータなし")
            return None

        df = pd.DataFrame(self.trades)

        # 基本統計
        total_trades = len(df)
        wins = len(df[df['profit_loss'] > 0])
        losses = len(df[df['profit_loss'] < 0])
        breakeven = len(df[df['profit_loss'] == 0])

        win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0

        # 損益統計
        total_profit_loss = df['profit_loss'].sum()
        avg_profit_loss = df['profit_loss'].mean()
        avg_profit_loss_pct = df['profit_loss_pct'].mean()

        # R倍数（平均利益 / 平均損失の絶対値）
        avg_win = df[df['profit_loss'] > 0]['profit_loss'].mean() if wins > 0 else 0
        avg_loss = df[df['profit_loss'] < 0]['profit_loss'].mean() if losses > 0 else 0
        r_multiple = abs(avg_win / avg_loss) if avg_loss != 0 else 0

        # 最大連敗
        max_consecutive_losses = self._calculate_max_consecutive_losses(df)

        # 最大ドローダウン
        max_drawdown = self._calculate_max_drawdown(df)

        metrics = {
            'total_trades': total_trades,
            'wins': wins,
            'losses': losses,
            'breakeven': breakeven,
            'win_rate': win_rate,
            'total_profit_loss': total_profit_loss,
            'avg_profit_loss': avg_profit_loss,
            'avg_profit_loss_pct': avg_profit_loss_pct,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'r_multiple': r_multiple,
            'max_consecutive_losses': max_consecutive_losses,
            'max_drawdown': max_drawdown
        }

        return metrics

    def _calculate_max_consecutive_losses(self, df):
        """
        最大連敗数を計算

        Args:
            df: トレード結果DataFrame

        Returns:
            int: 最大連敗数
        """
        max_streak = 0
        current_streak = 0

        for profit_loss in df['profit_loss']:
            if profit_loss < 0:
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0

        return max_streak

    def _calculate_max_drawdown(self, df):
        """
        最大ドローダウンを計算

        Args:
            df: トレード結果DataFrame

        Returns:
            float: 最大ドローダウン（円）
        """
        cumulative_pnl = df['profit_loss'].cumsum()
        running_max = cumulative_pnl.cummax()
        drawdown = cumulative_pnl - running_max
        max_drawdown = drawdown.min()

        return max_drawdown

    def save_results(self):
        """
        結果をCSVファイルに保存
        """
        if len(self.trades) == 0:
            logger.warning("保存するトレードデータがありません")
            return

        # ファイル名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"paper_trading_{timestamp}.csv"
        csv_path = os.path.join("backtest_results", csv_filename)

        # DataFrame作成
        df = pd.DataFrame(self.trades)

        # CSV保存
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.success(f"結果を保存しました: {csv_path}")

        return csv_path

    def send_discord_notification(self, metrics):
        """
        Discord通知を送信

        Args:
            metrics: パフォーマンス指標（dict）
        """
        if metrics is None:
            self.notifier.send_message("⚠️ ペーパートレードシミュレーション結果なし")
            return

        # メッセージ構築
        content = f"📊 **ペーパートレードシミュレーション結果**\n\n"

        content += f"**基本統計**\n"
        content += f"総トレード数: {metrics['total_trades']}回\n"
        content += f"勝ち: {metrics['wins']}回 / 負け: {metrics['losses']}回 / 引分: {metrics['breakeven']}回\n"
        content += f"勝率: {metrics['win_rate']:.1f}%\n\n"

        content += f"**損益統計**\n"
        content += f"合計損益: {metrics['total_profit_loss']:,.0f}円\n"
        content += f"平均損益: {metrics['avg_profit_loss']:,.0f}円 ({metrics['avg_profit_loss_pct']:+.2f}%)\n"
        content += f"平均利益: {metrics['avg_win']:,.0f}円\n"
        content += f"平均損失: {metrics['avg_loss']:,.0f}円\n"
        content += f"R倍数: {metrics['r_multiple']:.2f}\n\n"

        content += f"**リスク指標**\n"
        content += f"最大連敗: {metrics['max_consecutive_losses']}回\n"
        content += f"最大ドローダウン: {metrics['max_drawdown']:,.0f}円\n"

        self.notifier.send_message(content)
        logger.success("Discord通知を送信しました")


def main():
    """メイン処理"""
    simulator = PaperTradingSimulator()

    # シミュレーション実行
    simulator.run_simulation()

    # メトリクス計算
    metrics = simulator.calculate_metrics()

    if metrics:
        logger.info("\n" + "=" * 60)
        logger.info("シミュレーション結果サマリー")
        logger.info("=" * 60)
        logger.info(f"総トレード数: {metrics['total_trades']}回")
        logger.info(f"勝率: {metrics['win_rate']:.1f}%")
        logger.info(f"合計損益: {metrics['total_profit_loss']:,.0f}円")
        logger.info(f"平均損益: {metrics['avg_profit_loss']:,.0f}円 ({metrics['avg_profit_loss_pct']:+.2f}%)")
        logger.info(f"R倍数: {metrics['r_multiple']:.2f}")
        logger.info(f"最大連敗: {metrics['max_consecutive_losses']}回")
        logger.info(f"最大ドローダウン: {metrics['max_drawdown']:,.0f}円")
        logger.info("=" * 60)

    # 結果保存
    csv_path = simulator.save_results()

    # Discord通知
    simulator.send_discord_notification(metrics)

    logger.success("ペーパートレードシミュレーション完了")


if __name__ == "__main__":
    main()
