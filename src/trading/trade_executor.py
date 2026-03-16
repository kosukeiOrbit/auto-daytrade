"""
自動売買エグゼキューター
candidates_YYYYMMDD.csv を読み込み、エントリー判定・注文実行
"""
import os
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from src.utils.kabu_client import KabuClient
from src.utils.notifier import DiscordNotifier


class TradeExecutor:
    """自動売買エグゼキューター"""

    def __init__(self, budget=800000, max_daily_loss_rate=0.03, max_consecutive_losses=3):
        """
        初期化

        Args:
            budget: 1銘柄あたりの予算（デフォルト80万円）
            max_daily_loss_rate: 日次最大損失率（デフォルト3%）
            max_consecutive_losses: 最大連敗回数（デフォルト3回）
        """
        self.kabu_client = KabuClient()
        self.notifier = DiscordNotifier()
        self.budget = budget
        self.max_daily_loss_rate = max_daily_loss_rate
        self.max_consecutive_losses = max_consecutive_losses

        # トレード状態管理
        self.daily_profit_loss = 0.0  # 日次損益
        self.consecutive_losses = 0   # 連敗カウント
        self.active_positions = {}    # アクティブなポジション

        logger.info(f"TradeExecutor初期化: 予算={budget:,}円, 最大損失率={max_daily_loss_rate*100}%, 最大連敗={max_consecutive_losses}回")

    def load_candidates(self, date=None):
        """
        スクリーニング候補銘柄CSVを読み込む

        Args:
            date: 対象日（datetime）。Noneの場合は今日

        Returns:
            pd.DataFrame: 候補銘柄データ
        """
        if date is None:
            date = datetime.now()

        csv_filename = f"candidates_{date.strftime('%Y%m%d')}.csv"
        csv_path = os.path.join("results", csv_filename)

        if not os.path.exists(csv_path):
            logger.warning(f"候補銘柄CSVが見つかりません: {csv_path}")
            return pd.DataFrame()

        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            logger.info(f"候補銘柄CSV読み込み成功: {len(df)}銘柄")
            return df
        except Exception as e:
            logger.error(f"候補銘柄CSV読み込みエラー: {e}")
            return pd.DataFrame()

    def check_entry_signal(self, symbol):
        """
        エントリーシグナルをチェック

        Args:
            symbol: 銘柄コード

        Returns:
            bool: エントリー可能な場合True
        """
        try:
            # 銘柄情報を取得
            symbol_info = self.kabu_client.get_symbol(symbol)

            # 現在値が取得できない場合はスキップ
            if symbol_info['current_price'] is None or symbol_info['current_price'] <= 0:
                logger.warning(f"{symbol}: 現在値が取得できません")
                return False

            # 気配値チェック（売気配が存在するか）
            if symbol_info['ask_price'] is None or symbol_info['ask_price'] <= 0:
                logger.warning(f"{symbol}: 売気配がありません")
                return False

            logger.info(f"{symbol}: エントリー可能")
            return True

        except Exception as e:
            logger.error(f"{symbol}: エントリーシグナルチェックエラー: {e}")
            return False

    def check_safety_conditions(self):
        """
        安全装置チェック

        Returns:
            tuple: (is_safe: bool, reason: str)
        """
        # 日次最大損失チェック
        wallet = self.kabu_client.get_wallet_cash()
        cash_balance = wallet['stock_account_wallet']

        if cash_balance is None:
            logger.warning("買付余力がnull（検証環境）のため、安全装置チェックをスキップ")
            return True, ""

        # 日次損失が最大損失率を超えた場合
        max_daily_loss = cash_balance * self.max_daily_loss_rate
        if self.daily_profit_loss < -max_daily_loss:
            reason = f"日次最大損失到達: {self.daily_profit_loss:,.0f}円 (限度: {-max_daily_loss:,.0f}円)"
            logger.warning(reason)
            return False, reason

        # 連敗チェック
        if self.consecutive_losses >= self.max_consecutive_losses:
            reason = f"最大連敗到達: {self.consecutive_losses}回連続損切り"
            logger.warning(reason)
            return False, reason

        return True, ""

    def calculate_position_size(self, current_price):
        """
        ポジションサイズを計算

        Args:
            current_price: 現在株価

        Returns:
            int: 購入株数（単元株数の倍数）
        """
        # 1銘柄あたりの予算で購入できる株数
        qty = int(self.budget / current_price)

        # 単元株（100株）の倍数に調整
        qty = (qty // 100) * 100

        if qty < 100:
            logger.warning(f"予算不足: 現在値={current_price}円では100株未満")
            return 0

        logger.info(f"ポジションサイズ: {qty}株 (現在値={current_price}円)")
        return qty

    def entry_with_stop_and_target(self, symbol, exchange=1):
        """
        エントリー + 逆指値・利確注文セット

        Args:
            symbol: 銘柄コード
            exchange: 市場コード（デフォルト1=東証）

        Returns:
            dict: {
                'entry_order_id': str,
                'stop_order_id': str,
                'target_order_id': str,
                'entry_price': float,
                'qty': int
            } or None
        """
        try:
            # 銘柄情報取得
            symbol_info = self.kabu_client.get_symbol(symbol, exchange)
            current_price = symbol_info['ask_price']  # 売気配値でエントリー

            # ポジションサイズ計算
            qty = self.calculate_position_size(current_price)
            if qty == 0:
                logger.warning(f"{symbol}: ポジションサイズが0のためスキップ")
                return None

            # エントリー（成行買い）
            logger.info(f"{symbol}: エントリー注文 {qty}株 @ 成行")
            entry_result = self.kabu_client.send_order(
                symbol=symbol,
                exchange=exchange,
                side=2,  # 2=買
                qty=qty,
                order_type=1,  # 1=成行
                price=0
            )

            if entry_result['result_code'] != 0:
                logger.error(f"{symbol}: エントリー注文失敗")
                return None

            entry_order_id = entry_result['order_id']
            logger.success(f"{symbol}: エントリー注文成功 注文番号={entry_order_id}")

            # 損切り価格（-1%）デイトレルール
            stop_price = int(current_price * 0.99)

            # 利確価格（+2%）デイトレルール（R倍数2.0）
            target_price = int(current_price * 1.02)

            # 逆指値（損切り）注文
            logger.info(f"{symbol}: 逆指値注文 {qty}株 @ {stop_price}円以下で成行売")
            stop_result = self.kabu_client.send_order(
                symbol=symbol,
                exchange=exchange,
                side=1,  # 1=売
                qty=qty,
                order_type=3,  # 3=逆指値
                price=0,  # 成行
                stop_price=stop_price
            )

            stop_order_id = stop_result['order_id'] if stop_result['result_code'] == 0 else None

            # 指値（利確）注文
            logger.info(f"{symbol}: 指値注文 {qty}株 @ {target_price}円で売")
            target_result = self.kabu_client.send_order(
                symbol=symbol,
                exchange=exchange,
                side=1,  # 1=売
                qty=qty,
                order_type=2,  # 2=指値
                price=target_price
            )

            target_order_id = target_result['order_id'] if target_result['result_code'] == 0 else None

            # ポジション情報を保存
            position_info = {
                'entry_order_id': entry_order_id,
                'stop_order_id': stop_order_id,
                'target_order_id': target_order_id,
                'entry_price': current_price,
                'qty': qty,
                'stop_price': stop_price,
                'target_price': target_price,
                'entry_time': datetime.now()
            }

            self.active_positions[symbol] = position_info

            # Discord通知
            self.notifier.send_trade_notification(
                action="エントリー",
                symbol=symbol,
                price=current_price,
                qty=qty,
                stop_price=stop_price,
                target_price=target_price
            )

            logger.success(f"{symbol}: エントリー完了 数量={qty}株 価格={current_price}円 損切={stop_price}円 利確={target_price}円")

            return position_info

        except Exception as e:
            logger.error(f"{symbol}: エントリーエラー: {e}")
            # エラー通知
            self.notifier.send_error(f"エントリーエラー: {symbol} - {str(e)[:100]}")
            return None

    def execute_daily_trading(self):
        """
        日次自動売買実行
        候補銘柄CSVを読み込み、エントリー判定・注文実行
        """
        logger.info("=" * 60)
        logger.info("日次自動売買開始")
        logger.info("=" * 60)

        # 安全装置チェック
        is_safe, reason = self.check_safety_conditions()
        if not is_safe:
            logger.warning(f"安全装置発動: {reason}")
            self.notifier.send_error(f"自動売買停止: {reason}")
            return

        # 候補銘柄読み込み
        candidates_df = self.load_candidates()

        if len(candidates_df) == 0:
            logger.info("候補銘柄がありません")
            return

        logger.info(f"候補銘柄: {len(candidates_df)}銘柄")

        # 各候補銘柄に対してエントリー判定
        entry_count = 0
        for idx, row in candidates_df.iterrows():
            symbol = str(row['code'])

            logger.info(f"\n[{idx+1}/{len(candidates_df)}] {symbol} {row['name']}")

            # エントリーシグナルチェック
            if not self.check_entry_signal(symbol):
                logger.info(f"{symbol}: エントリー条件不適合")
                continue

            # エントリー実行
            position = self.entry_with_stop_and_target(symbol)

            if position:
                entry_count += 1
                logger.success(f"{symbol}: エントリー成功")
            else:
                logger.warning(f"{symbol}: エントリー失敗")

        logger.info("=" * 60)
        logger.info(f"日次自動売買完了: エントリー {entry_count}銘柄")
        logger.info("=" * 60)

        # 完了通知
        self.notifier.send_message(
            f"自動売買完了\nエントリー: {entry_count}銘柄\n候補: {len(candidates_df)}銘柄"
        )

    def force_exit_losing_positions_midday(self):
        """
        11:30（前場引け）の含み損ポジション強制成行決済
        """
        now = datetime.now()
        current_time = now.time()

        # 11:30チェック（11:25-11:35の間に実行）
        if not (now.replace(hour=11, minute=25, second=0) <= now <= now.replace(hour=11, minute=35, second=0)):
            logger.debug("前場引け時刻外のため、含み損決済スキップ")
            return

        logger.info("=" * 60)
        logger.info("前場引け: 含み損ポジション強制決済チェック")
        logger.info("=" * 60)

        try:
            positions = self.kabu_client.get_positions()

            for pos in positions:
                symbol = pos['symbol']
                profit_loss = pos['profit_loss']
                qty = pos['qty']

                # 含み損の場合のみ決済
                if profit_loss < 0:
                    logger.warning(f"{symbol}: 含み損 {profit_loss:,.0f}円 → 前場引け強制決済")

                    # 成行売り注文
                    self.kabu_client.send_order(
                        symbol=symbol,
                        exchange=1,
                        side=1,  # 1=売
                        qty=qty,
                        order_type=1,  # 1=成行
                        price=0
                    )

                    # Discord通知
                    self.notifier.send_trade_notification(
                        action="前場引け強制決済",
                        symbol=symbol,
                        price=pos['current_price'],
                        qty=qty
                    )

                    logger.success(f"{symbol}: 前場引け強制決済完了")

        except Exception as e:
            logger.error(f"前場引け強制決済エラー: {e}")

    def force_exit_all_positions_eod(self):
        """
        15:20（大引け10分前）の全ポジション強制決済
        """
        now = datetime.now()

        # 15:20チェック（15:15-15:25の間に実行）
        if not (now.replace(hour=15, minute=15, second=0) <= now <= now.replace(hour=15, minute=25, second=0)):
            logger.debug("大引け前時刻外のため、全決済スキップ")
            return

        logger.info("=" * 60)
        logger.info("大引け10分前: 全ポジション強制決済")
        logger.info("=" * 60)

        try:
            positions = self.kabu_client.get_positions()

            if len(positions) == 0:
                logger.info("決済対象ポジションなし")
                return

            for pos in positions:
                symbol = pos['symbol']
                profit_loss = pos['profit_loss']
                qty = pos['qty']

                logger.info(f"{symbol}: 損益={profit_loss:,.0f}円 → 大引け前強制決済")

                # 成行売り注文
                self.kabu_client.send_order(
                    symbol=symbol,
                    exchange=1,
                    side=1,  # 1=売
                    qty=qty,
                    order_type=1,  # 1=成行
                    price=0
                )

                # Discord通知
                self.notifier.send_trade_notification(
                    action="大引け前強制決済",
                    symbol=symbol,
                    price=pos['current_price'],
                    qty=qty
                )

                logger.success(f"{symbol}: 大引け前強制決済完了")

            # 完了通知
            self.notifier.send_message(f"大引け前強制決済完了: {len(positions)}銘柄")

        except Exception as e:
            logger.error(f"大引け前強制決済エラー: {e}")

    def monitor_positions(self):
        """
        保有ポジションを監視
        決済済みポジションの損益を集計
        """
        try:
            positions = self.kabu_client.get_positions()

            logger.info(f"保有ポジション: {len(positions)}件")

            for pos in positions:
                symbol = pos['symbol']
                profit_loss = pos['profit_loss']
                profit_loss_rate = pos['profit_loss_rate']

                logger.info(f"{symbol}: 損益={profit_loss:,.0f}円 ({profit_loss_rate:+.2f}%)")

                # 決済済みの場合（ポジションがアクティブリストにあるが保有ポジションに存在しない）
                # → 損益を集計し、連敗カウントを更新

        except Exception as e:
            logger.error(f"ポジション監視エラー: {e}")
