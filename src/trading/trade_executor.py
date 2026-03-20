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
from src.utils.jquants_client import JQuantsClient


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
        self.jquants_client = JQuantsClient()
        self.budget = budget
        self.max_daily_loss_rate = max_daily_loss_rate
        self.max_consecutive_losses = max_consecutive_losses

        # トレード状態管理
        self.daily_profit_loss = 0.0  # 日次損益
        self.consecutive_losses = 0   # 連敗カウント
        self.active_positions = {}    # アクティブなポジション

        # パターンB: 銘柄ごとの直近価格履歴（5分足組み立て用）
        self.pattern_b_price_history = {}  # {symbol: [{'time': datetime, 'price': float, 'volume': int, 'vwap': float}]}

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
        csv_path = os.path.join("data", csv_filename)

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

    def apply_filters(self, candidates_df, trade_date):
        """
        候補銘柄にフィルタを適用

        Args:
            candidates_df: 候補銘柄DataFrame
            trade_date: 取引日（datetime）

        Returns:
            DataFrame: フィルタ後の候補銘柄
        """
        initial_count = len(candidates_df)
        logger.info(f"フィルタ適用前: {initial_count}件")

        # フィルタ1: material_strength フィルタ（'強'または'中'のみ）
        if 'material_strength' in candidates_df.columns:
            before_count = len(candidates_df)
            candidates_df = candidates_df[
                (candidates_df['material_strength'] == '強') |
                (candidates_df['material_strength'] == '中')
            ]
            filtered_count = before_count - len(candidates_df)
            if filtered_count > 0:
                logger.info(f"フィルタ1 (材料強度): {filtered_count}件除外 (残り{len(candidates_df)}件)")

        # フィルタ2: 前日ストップ高除外（前日終値が前々日比+25%以上）
        if 'Code' in candidates_df.columns:
            filtered_codes = []
            for idx, row in candidates_df.iterrows():
                code = str(row['Code'])
                if self.is_previous_day_limit_up(code, trade_date):
                    filtered_codes.append(code)
                    logger.info(f"フィルタ2 (前日ストップ高): {code} を除外")

            if len(filtered_codes) > 0:
                candidates_df = candidates_df[~candidates_df['Code'].astype(str).isin(filtered_codes)]
                logger.info(f"フィルタ2 (前日ストップ高除外): {len(filtered_codes)}件除外 (残り{len(candidates_df)}件)")

        logger.info(f"フィルタ適用後: {len(candidates_df)}件 (除外: {initial_count - len(candidates_df)}件)")
        return candidates_df

    def is_previous_day_limit_up(self, code, trade_date):
        """
        前日がストップ高だったか判定

        Args:
            code: 銘柄コード
            trade_date: 取引日（datetime）

        Returns:
            bool: True=前日ストップ高（除外すべき）
        """
        try:
            # 過去7日分を取得して前日・前々日を抽出
            start_date = trade_date - timedelta(days=7)
            df = self.jquants_client.get_daily_quotes(code=code, date=start_date)

            if df is None or len(df) < 2:
                # データ不足の場合はフィルタしない（保守的）
                return False

            # 日付でソート（新しい順）
            df = df.sort_values('Date', ascending=False)

            # 取引日より前のデータのみ抽出
            df = df[df['Date'] < trade_date.strftime('%Y-%m-%d')]

            # 前日と前々日の終値を取得
            if len(df) >= 2:
                prev_close_1 = df.iloc[0]['C']  # 前日終値
                prev_close_2 = df.iloc[1]['C']  # 前々日終値

                if pd.notna(prev_close_1) and pd.notna(prev_close_2) and prev_close_2 > 0:
                    # 前日の上昇率を計算
                    prev_day_change_pct = ((prev_close_1 - prev_close_2) / prev_close_2) * 100

                    # +25%以上ならストップ高と判定
                    if prev_day_change_pct >= 25.0:
                        logger.debug(f"{code}: 前日ストップ高検出 (+{prev_day_change_pct:.1f}%)")
                        return True

            return False

        except Exception as e:
            logger.debug(f"{code}: 前日データ取得エラー: {e}")
            # エラー時はフィルタしない（保守的）
            return False

    def check_entry_signal(self, symbol, pre_open=False):
        """
        エントリーシグナルをチェック

        Args:
            symbol: 銘柄コード
            pre_open: True=寄前エントリー（現在値チェックスキップ）

        Returns:
            bool: エントリー可能な場合True
        """
        # 寄前（8:45）の寄成注文は現在値不要
        if pre_open:
            logger.info(f"{symbol}: 寄前エントリー（現在値チェックスキップ）")
            return True

        try:
            # 銘柄情報を取得（パターンB等、場中エントリー用）
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
        ポジションサイズを計算（買付余力に応じて動的に調整）

        Args:
            current_price: 現在株価

        Returns:
            int: 購入株数（単元株数の倍数）
        """
        # 買付余力を取得
        try:
            wallet = self.kabu_client.get_wallet_cash()
            available_cash = wallet['stock_account_wallet']

            # 検証環境の場合（nullの場合）は固定予算を使用
            if available_cash is None:
                usable_budget = self.budget
                logger.warning(f"検証環境のため固定予算を使用: {self.budget:,}円")
            else:
                # 本番環境: 買付余力と予算の小さい方を使用
                usable_budget = min(available_cash, self.budget)
                logger.info(f"買付余力: {available_cash:,}円, 使用予算: {usable_budget:,}円")

        except Exception as e:
            # API取得失敗時は固定予算を使用
            logger.warning(f"買付余力取得エラー、固定予算を使用: {e}")
            usable_budget = self.budget

        # 1銘柄あたりの予算で購入できる株数
        qty = int(usable_budget / current_price)

        # 単元株（100株）の倍数に調整
        qty = (qty // 100) * 100

        if qty < 100:
            logger.warning(f"予算不足: 現在値={current_price}円では100株未満")
            return 0

        logger.info(f"ポジションサイズ: {qty}株 (現在値={current_price}円, 予算={usable_budget:,}円)")
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

            # エントリー（寄成・前場買い）
            logger.info(f"{symbol}: エントリー注文 {qty}株 @ 寄成（前場）")
            entry_result = self.kabu_client.send_order(
                symbol=symbol,
                exchange=exchange,
                side=2,  # 2=買
                qty=qty,
                order_type=13,  # 13=寄成・前場（FrontOrderType直接指定）
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

            # エントリー時のVWAP・始値を取得（分析用）
            entry_vwap = symbol_info.get('vwap')
            entry_opening = symbol_info.get('opening_price')
            entry_vwap_ratio = ((current_price / entry_vwap) - 1) * 100 if entry_vwap and entry_vwap > 0 else None
            entry_gap_pct = ((current_price / entry_opening) - 1) * 100 if entry_opening and entry_opening > 0 else None

            # ポジション情報を保存
            position_info = {
                'entry_order_id': entry_order_id,
                'stop_order_id': stop_order_id,
                'target_order_id': target_order_id,
                'entry_price': current_price,
                'qty': qty,
                'stop_price': stop_price,
                'target_price': target_price,
                'entry_time': datetime.now(),
                'material_strength': '',
                'material_type': '',
                'volume_surge': 0.0,
                'entry_pattern': 'A',
                'mfe_pct': 0.0,
                'mae_pct': 0.0,
                'entry_vwap_ratio': round(entry_vwap_ratio, 4) if entry_vwap_ratio is not None else None,
                'entry_gap_pct': round(entry_gap_pct, 4) if entry_gap_pct is not None else None,
                'opening_gap_pct': None,  # execute_daily_tradingで上書き
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
        日次自動売買実行（1日1銘柄集中）
        候補銘柄CSVを読み込み、最優先銘柄にエントリー
        """
        logger.info("=" * 60)
        logger.info("日次自動売買開始（1日1銘柄集中）")
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

        # フィルタ適用
        trade_date = datetime.now()
        candidates_df = self.apply_filters(candidates_df, trade_date)

        if len(candidates_df) == 0:
            logger.info("フィルタ後の候補銘柄がありません")
            return

        # 優先順位でソート
        # 1. material_strength: '強' > '中'
        # 2. 同一強度内では VolumeSurgeRatio 降順
        strength_order = {'強': 0, '中': 1}
        candidates_df['strength_rank'] = candidates_df['material_strength'].map(strength_order)
        candidates_df = candidates_df.sort_values(
            by=['strength_rank', 'VolumeSurgeRatio'],
            ascending=[True, False]
        )
        candidates_df = candidates_df.drop(columns=['strength_rank'])

        logger.info("優先順位ソート完了:")
        for idx, row in candidates_df.head(5).iterrows():
            logger.info(f"  {idx+1}. {row['Code']}: {row['material_strength']} 出来高{row['VolumeSurgeRatio']:.2f}倍")

        # 1日1銘柄エントリー（最優先銘柄のみ）
        entry_count = 0
        for idx, row in candidates_df.iterrows():
            symbol = str(row['Code'])

            # 10:30以降はエントリーしない
            now = datetime.now()
            if now.time() > datetime.strptime("10:30", "%H:%M").time():
                logger.info("エントリー期限（10:30）を過ぎたためスキップ")
                break

            logger.info(f"\n[エントリー候補] {symbol} ({row['material_strength']}, 出来高{row['VolumeSurgeRatio']:.2f}倍)")

            # エントリーシグナルチェック（寄成注文のため現在値チェックスキップ）
            if not self.check_entry_signal(symbol, pre_open=True):
                logger.info(f"{symbol}: エントリー条件不適合 → 次の候補へ")
                continue

            # エントリー実行
            position = self.entry_with_stop_and_target(symbol)

            if position:
                # candidate情報をposition_infoに付与
                position['material_strength'] = row.get('material_strength', '')
                position['material_type'] = row.get('material_type', '')
                position['volume_surge'] = row.get('VolumeSurgeRatio', 0.0)

                # opening_gap_pct: エントリー価格 ÷ 前日終値 - 1（%）
                prev_close = row.get('C') or row.get('Close') or 0
                entry_price = position.get('entry_price', 0)
                if prev_close and prev_close > 0 and entry_price > 0:
                    position['opening_gap_pct'] = round((entry_price / prev_close - 1) * 100, 4)
                else:
                    position['opening_gap_pct'] = None

                entry_count += 1
                logger.success(f"{symbol}: エントリー成功 → 1日1銘柄ルールにより終了")
                break  # 1銘柄エントリーしたら終了
            else:
                logger.warning(f"{symbol}: エントリー失敗 → 次の候補へ")

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

                    # トレード履歴保存
                    entry_price = self.active_positions.get(symbol, {}).get('entry_price', 0)
                    self.save_trade_history(symbol, entry_price, pos['current_price'], qty, '前場強制')

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

                # トレード履歴保存
                entry_price = self.active_positions.get(symbol, {}).get('entry_price', 0)
                exit_reason = '大引強制'
                self.save_trade_history(symbol, entry_price, pos['current_price'], qty, exit_reason)

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

    def save_trade_history(self, symbol, entry_price, exit_price, qty, exit_reason):
        """
        トレード結果をCSVに追記保存

        Args:
            symbol: 銘柄コード
            entry_price: エントリー価格
            exit_price: 決済価格
            qty: 株数
            exit_reason: 決済理由（利確/損切/前場強制/大引強制）
        """
        try:
            filepath = "data/trade_history.csv"
            os.makedirs("data", exist_ok=True)

            profit_loss = (exit_price - entry_price) * qty
            profit_pct = (exit_price - entry_price) / entry_price * 100 if entry_price > 0 else 0

            # active_positionsからcandidate情報を取得
            pos_info = self.active_positions.get(symbol, {})
            material_strength = pos_info.get('material_strength', '')
            material_type = pos_info.get('material_type', '')
            volume_surge = pos_info.get('volume_surge', 0.0)

            entry_time_obj = pos_info.get('entry_time')
            entry_time_str = entry_time_obj.strftime('%H:%M') if entry_time_obj else ''
            entry_pattern = pos_info.get('entry_pattern', 'A')

            # 決済時刻・保有時間
            exit_time_now = datetime.now()
            exit_time_str = exit_time_now.strftime('%H:%M')
            hold_minutes = int((exit_time_now - entry_time_obj).total_seconds() / 60) if entry_time_obj else 0

            # MFE/MAE
            mfe_pct = round(pos_info.get('mfe_pct', 0), 4)
            mae_pct = round(pos_info.get('mae_pct', 0), 4)

            # VWAP比・寄り乖離率・寄りギャップ
            entry_vwap_ratio = pos_info.get('entry_vwap_ratio', '')
            entry_gap_pct = pos_info.get('entry_gap_pct', '')
            opening_gap_pct = pos_info.get('opening_gap_pct', '')

            record = {
                'date': datetime.now().strftime('%Y%m%d'),
                'code': symbol,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'qty': qty,
                'profit_loss': round(profit_loss, 1),
                'profit_pct': round(profit_pct, 4),
                'exit_reason': exit_reason,
                'material_strength': material_strength,
                'material_type': material_type,
                'volume_surge': round(volume_surge, 2),
                'entry_time': entry_time_str,
                'entry_pattern': entry_pattern,
                'exit_time': exit_time_str,
                'hold_minutes': hold_minutes,
                'mfe_pct': mfe_pct,
                'mae_pct': mae_pct,
                'entry_vwap_ratio': entry_vwap_ratio if entry_vwap_ratio is not None else '',
                'entry_gap_pct': entry_gap_pct if entry_gap_pct is not None else '',
                'opening_gap_pct': opening_gap_pct if opening_gap_pct is not None else '',
            }

            df_new = pd.DataFrame([record])
            expected_columns = list(record.keys())

            # ファイルが存在すれば追記（ヘッダー不一致時は再書き込み）
            if os.path.exists(filepath):
                df_existing = pd.read_csv(filepath, encoding='utf-8-sig')
                if list(df_existing.columns) != expected_columns:
                    # カラム変更があった場合：既存データにカラム追加して再保存
                    for col in expected_columns:
                        if col not in df_existing.columns:
                            df_existing[col] = ''
                    df_existing = df_existing[expected_columns]
                    df_all = pd.concat([df_existing, df_new], ignore_index=True)
                    df_all.to_csv(filepath, index=False, encoding='utf-8-sig')
                else:
                    df_new.to_csv(filepath, mode='a', header=False, index=False, encoding='utf-8-sig')
            else:
                df_new.to_csv(filepath, index=False, encoding='utf-8-sig')

            logger.info(f"トレード履歴保存: {symbol} {exit_reason} {profit_loss:+,.0f}円 → {filepath}")

        except Exception as e:
            logger.error(f"トレード履歴保存エラー: {e}")

    def monitor_positions(self):
        """
        保有ポジションを監視
        決済済みポジション（逆指値/指値で自動約定）を検知してトレード履歴に保存
        """
        try:
            positions = self.kabu_client.get_positions()
            current_symbols = {pos['symbol'] for pos in positions}

            logger.info(f"保有ポジション: {len(positions)}件")

            for pos in positions:
                symbol = pos['symbol']
                profit_loss = pos['profit_loss']
                profit_loss_rate = pos['profit_loss_rate']
                logger.info(f"{symbol}: 損益={profit_loss:,.0f}円 ({profit_loss_rate:+.2f}%)")

                # MFE/MAE更新（active_positionsに存在する場合）
                if symbol in self.active_positions:
                    pos_info = self.active_positions[symbol]
                    entry_price = pos_info.get('entry_price', 0)
                    current_price = pos.get('current_price', 0)
                    if entry_price > 0 and current_price and current_price > 0:
                        current_pct = (current_price - entry_price) / entry_price * 100
                        pos_info['mfe_pct'] = max(pos_info.get('mfe_pct', 0), current_pct)
                        pos_info['mae_pct'] = min(pos_info.get('mae_pct', 0), current_pct)

            # 決済済みポジション検知（active_positionsにあるがAPIのポジションにない）
            closed_symbols = []
            for symbol, pos_info in list(self.active_positions.items()):
                if symbol not in current_symbols:
                    # 自動決済された（逆指値or指値が約定）
                    entry_price = pos_info.get('entry_price', 0)
                    qty = pos_info.get('qty', 0)
                    stop_price = pos_info.get('stop_price', 0)
                    target_price = pos_info.get('target_price', 0)

                    # 決済理由の推定（損切りか利確か）
                    # 最新の約定価格が取れない場合、stop/targetとの距離で判定
                    # → 保守的にstop_priceで損切りと仮定し、後で実際の約定価格で補正
                    exit_reason = '損切り'
                    exit_price = stop_price

                    # 利確の可能性チェック（target_priceに近い場合）
                    if target_price > 0 and entry_price > 0:
                        # 利確ラインは+2%、損切りは-1%
                        # target到達の方が有利なので利確と判定
                        if profit_loss_rate is not None and profit_loss_rate > 0:
                            exit_reason = '利確'
                            exit_price = target_price

                    self.save_trade_history(symbol, entry_price, exit_price, qty, exit_reason)
                    closed_symbols.append(symbol)
                    logger.info(f"{symbol}: 自動決済検知 → {exit_reason} トレード履歴保存")

            # 決済済みポジションをactive_positionsから削除
            for symbol in closed_symbols:
                del self.active_positions[symbol]

        except Exception as e:
            logger.error(f"ポジション監視エラー: {e}")

    # ========== パターンB エントリーロジック ==========

    def scan_pattern_b_candidates(self):
        """
        パターンB: 売買高急増ランキングから候補銘柄を検知し、
        価格履歴を蓄積する。

        Returns:
            list: ランキング上位10銘柄のシンボルリスト
        """
        try:
            ranking = self.kabu_client.get_ranking(ranking_type=6, exchange_division="ALL")

            if not ranking:
                return []

            # 上位10銘柄のみ監視
            top_symbols = []
            for item in ranking[:10]:
                symbol = item['symbol']
                top_symbols.append(symbol)

                # /board で詳細情報取得
                try:
                    board = self.kabu_client.get_symbol(symbol)
                    price_record = {
                        'time': datetime.now(),
                        'price': board.get('current_price'),
                        'volume': board.get('trading_volume'),
                        'vwap': board.get('vwap'),
                        'opening_price': board.get('opening_price'),
                    }

                    # 価格履歴に追加（銘柄ごと）
                    if symbol not in self.pattern_b_price_history:
                        self.pattern_b_price_history[symbol] = []
                    self.pattern_b_price_history[symbol].append(price_record)

                    # 直近10分のデータのみ保持（古いデータを削除）
                    cutoff = datetime.now() - timedelta(minutes=10)
                    self.pattern_b_price_history[symbol] = [
                        r for r in self.pattern_b_price_history[symbol]
                        if r['time'] >= cutoff
                    ]

                except Exception as e:
                    logger.debug(f"パターンB {symbol}: board取得失敗: {e}")

            # ランキングから外れた銘柄の履歴を削除
            for symbol in list(self.pattern_b_price_history.keys()):
                if symbol not in top_symbols:
                    del self.pattern_b_price_history[symbol]

            return top_symbols

        except Exception as e:
            logger.error(f"パターンBスキャンエラー: {e}")
            return []

    def check_pattern_b_entry(self, symbol):
        """
        パターンBのエントリー条件をチェック

        条件:
        1. 現在値がVWAPより上
        2. 現在値が寄り付き値から+3%以内
        3. 直近5本の価格が上昇トレンド（終値切り上がり）
        4. パターンAで既にポジションを持っていない

        Args:
            symbol: 銘柄コード

        Returns:
            bool: エントリー可能ならTrue
        """
        # ポジション保有中はスキップ（1銘柄制限）
        if len(self.active_positions) > 0:
            return False

        # 価格履歴が5本以上必要
        history = self.pattern_b_price_history.get(symbol, [])
        if len(history) < 5:
            return False

        latest = history[-1]
        current_price = latest['price']
        vwap = latest.get('vwap')

        if current_price is None or current_price <= 0:
            return False

        # 条件1: 現在値がVWAPより上
        if vwap is not None and vwap > 0:
            if current_price <= vwap:
                logger.debug(f"パターンB {symbol}: VWAP割れ（現在値{current_price} <= VWAP{vwap}）")
                return False

        # 条件2: 現在値が寄り付き値から+3%以内
        # /board の OpeningPrice を使用（取得できなければ履歴の最初の価格で代用）
        opening_price = latest.get('opening_price')
        if opening_price is None or opening_price <= 0:
            opening_price = history[0]['price']
        if opening_price and opening_price > 0:
            change_from_open = (current_price - opening_price) / opening_price * 100
            if change_from_open > 3.0:
                logger.debug(f"パターンB {symbol}: 高値掴み防止（寄りから+{change_from_open:.1f}%）")
                return False

        # 条件3: 直近5本が上昇トレンド（価格が切り上がっている）
        recent_5 = history[-5:]
        prices = [r['price'] for r in recent_5 if r['price'] is not None]
        if len(prices) < 5:
            return False

        is_uptrend = all(prices[i] <= prices[i + 1] for i in range(len(prices) - 1))
        if not is_uptrend:
            logger.debug(f"パターンB {symbol}: 上昇トレンドなし")
            return False

        logger.info(f"パターンB {symbol}: エントリー条件充足（価格{current_price}, VWAP{vwap}, 5本上昇）")
        return True

    def execute_pattern_b_entry(self, symbol):
        """
        パターンBのエントリーを実行（成行注文 + 逆指値 + 利確指値）

        Args:
            symbol: 銘柄コード

        Returns:
            dict: position_info or None
        """
        try:
            board = self.kabu_client.get_symbol(symbol)
            current_price = board['current_price']

            if current_price is None or current_price <= 0:
                logger.warning(f"パターンB {symbol}: 現在値取得失敗")
                return None

            qty = self.calculate_position_size(current_price)
            if qty == 0:
                logger.warning(f"パターンB {symbol}: ポジションサイズが0")
                return None

            # 成行注文（場中）
            logger.info(f"パターンB {symbol}: 成行エントリー {qty}株 @ {current_price}円")
            entry_result = self.kabu_client.send_order(
                symbol=symbol,
                exchange=1,
                side=2,  # 買
                qty=qty,
                order_type=1,  # 成行
                price=0
            )

            if entry_result['result_code'] != 0:
                logger.error(f"パターンB {symbol}: エントリー注文失敗")
                return None

            entry_order_id = entry_result['order_id']

            # 損切り -1%
            stop_price = int(current_price * 0.99)
            stop_result = self.kabu_client.send_order(
                symbol=symbol, exchange=1, side=1, qty=qty,
                order_type=3, price=0, stop_price=stop_price
            )
            stop_order_id = stop_result['order_id'] if stop_result['result_code'] == 0 else None

            # 利確 +2%
            target_price = int(current_price * 1.02)
            target_result = self.kabu_client.send_order(
                symbol=symbol, exchange=1, side=1, qty=qty,
                order_type=2, price=target_price
            )
            target_order_id = target_result['order_id'] if target_result['result_code'] == 0 else None

            # エントリー時のVWAP・始値を取得（分析用）
            entry_vwap = board.get('vwap')
            entry_opening = board.get('opening_price')
            entry_vwap_ratio = ((current_price / entry_vwap) - 1) * 100 if entry_vwap and entry_vwap > 0 else None
            entry_gap_pct = ((current_price / entry_opening) - 1) * 100 if entry_opening and entry_opening > 0 else None

            position_info = {
                'entry_order_id': entry_order_id,
                'stop_order_id': stop_order_id,
                'target_order_id': target_order_id,
                'entry_price': current_price,
                'qty': qty,
                'stop_price': stop_price,
                'target_price': target_price,
                'entry_time': datetime.now(),
                'material_strength': '',
                'material_type': '',
                'volume_surge': 0.0,
                'entry_pattern': 'B',
                'mfe_pct': 0.0,
                'mae_pct': 0.0,
                'entry_vwap_ratio': round(entry_vwap_ratio, 4) if entry_vwap_ratio is not None else None,
                'entry_gap_pct': round(entry_gap_pct, 4) if entry_gap_pct is not None else None,
                'opening_gap_pct': round(entry_gap_pct, 4) if entry_gap_pct is not None else None,
            }

            self.active_positions[symbol] = position_info

            self.notifier.send_trade_notification(
                action="パターンBエントリー",
                symbol=symbol,
                price=current_price,
                qty=qty,
                stop_price=stop_price,
                target_price=target_price
            )

            logger.success(f"パターンB {symbol}: エントリー完了 {qty}株 @ {current_price}円")
            return position_info

        except Exception as e:
            logger.error(f"パターンB {symbol}: エントリーエラー: {e}")
            return None
