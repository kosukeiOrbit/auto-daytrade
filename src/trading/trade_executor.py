"""
自動売買エグゼキューター
candidates_YYYYMMDD.csv を読み込み、エントリー判定・注文実行
"""
import os
import time
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

        # ポジション管理定数
        self.max_positions_a = 2       # パターンA最大保有数
        self.max_positions_b = 3       # パターンB最大保有数
        self.max_positions_total = 3   # 合計最大保有数
        self.max_entry_amount = 400_000  # 1銘柄あたりの投資上限（40万円）

        # トレード状態管理
        self.daily_profit_loss = 0.0  # 日次損益（決済のたびに更新）
        self.consecutive_losses = 0   # 連敗カウント（決済のたびに更新）
        self.last_trade_date = None   # 日次損益リセット用
        self.active_positions = {}    # アクティブなポジション

        # パターンB: 銘柄ごとの直近価格履歴（5分足組み立て用）
        self.pattern_b_price_history = {}  # {symbol: [{'time': datetime, 'price': float, 'volume': int, 'vwap': float}]}
        self.pattern_b_last_volume = {}    # {symbol: 前回の累積出来高}（差分計算用）
        self.entry_blacklist = set()       # エントリー失敗した銘柄（当日中は再挑戦しない）
        self.pattern_b_candidate_symbols = []  # candidates_*.csvから読み込んだ優先監視銘柄

        # 財務データ（発行済株式数）をロード（時価総額フィルター用）
        self.issued_shares_dict = {}  # {code_4digit: issued_shares}
        try:
            jquants = self.jquants_client
            statements = jquants.get_statements()
            if statements is not None and len(statements) > 0:
                for _, row in statements.iterrows():
                    code = str(row.get('Code', ''))
                    if len(code) == 5:
                        code = code[:-1]
                    shares = row.get('ShOutFY')
                    if shares and pd.notna(shares):
                        self.issued_shares_dict[code] = float(shares)
                logger.info(f"財務データロード完了: {len(self.issued_shares_dict)}銘柄の発行済株式数を取得")
        except Exception as e:
            logger.warning(f"財務データロード失敗（時価総額フィルターをスキップ）: {e}")

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
            wallet = self.kabu_client.get_wallet_margin()
            available = wallet['margin_account_wallet']

            if available is None:
                usable_budget = self.budget
                logger.warning("信用余力取得失敗、固定予算を使用")
            else:
                # 1銘柄あたりの上限：信用余力÷最大ポジション数 と max_entry_amount の小さい方
                per_position = available / self.max_positions_total
                usable_budget = min(per_position, self.max_entry_amount, self.budget)
                logger.info(f"信用余力: {available:,.0f}円 → 1銘柄予算: {usable_budget:,.0f}円")

        except Exception as e:
            logger.warning(f"信用余力取得エラー、固定予算を使用: {e}")
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

    def entry_with_stop_and_target(self, symbol, exchange=9, entry_pattern='B'):
        """
        エントリー + 逆指値・利確注文セット

        Args:
            symbol: 銘柄コード
            exchange: 市場コード（デフォルト9=SOR）
            entry_pattern: 'A'=寄り前成行, 'B'=場中指値+1%

        Returns:
            dict: position_info or None
        """
        try:
            # 登録銘柄を全解除（レジスト数上限50対策）
            self.kabu_client.unregister_all()

            # 銘柄情報取得（板情報はExchange=1で取得）
            symbol_info = self.kabu_client.get_symbol(symbol, exchange=1)
            current_price = symbol_info['ask_price'] or symbol_info['current_price']

            # 寄り前はask_priceがNullの場合、前日終値（current_price）を使用
            if current_price is None or current_price <= 0:
                current_price = symbol_info.get('current_price') or 0
            if current_price is None or current_price <= 0:
                logger.warning(f"{symbol}: 現在値取得失敗")
                return None

            # ポジションサイズ計算
            qty = self.calculate_position_size(current_price)
            if qty == 0:
                logger.warning(f"{symbol}: ポジションサイズが0のためスキップ")
                return None

            # エントリー注文（パターンA/Bで分岐、どちらも指値）
            if entry_pattern == 'A':
                # パターンA: 指値+5%（寄り前に出して寄り付きで約定を狙う）
                # SOR成行は値幅上限で余力拘束されるため指値を使用
                limit_price = int(current_price * 1.05)
                logger.info(f"{symbol}: エントリー注文 {qty}株 @ 指値{limit_price}円（前日終値{current_price}+5%・パターンA）")
            else:
                # パターンB: 指値+1%（場中の即約定狙い）
                limit_price = int(current_price * 1.01)
                logger.info(f"{symbol}: エントリー注文 {qty}株 @ 指値{limit_price}円（現在値{current_price}+1%・パターンB）")

            entry_result = self.kabu_client.send_order(
                symbol=symbol,
                exchange=exchange,
                side=2,
                qty=qty,
                order_type=2,  # 指値
                price=limit_price
            )

            if entry_result['result_code'] != 0:
                logger.error(f"{symbol}: エントリー注文失敗")
                return None

            entry_order_id = entry_result['order_id']
            logger.success(f"{symbol}: エントリー注文成功 注文番号={entry_order_id}")

            # 約定確認（パターンA: 寄り付き待機+最大1分, パターンB: 最大30秒）
            if entry_pattern == 'A':
                now = datetime.now()
                market_open = now.replace(hour=9, minute=0, second=30, microsecond=0)
                wait_seconds = (market_open - now).total_seconds()
                if wait_seconds > 0:
                    logger.info(f"{symbol}: 寄り付き待機 {wait_seconds:.0f}秒")
                    time.sleep(wait_seconds)
                else:
                    logger.info(f"{symbol}: 既に9:00:30を過ぎているため待機スキップ")
                max_tries = 12  # 5秒×12回=最大1分
            else:
                max_tries = 6   # 5秒×6回=最大30秒

            actual_entry_price = None
            for _ in range(max_tries):
                time.sleep(5)
                try:
                    orders = self.kabu_client.get_orders(symbol=symbol)
                    matched = next(
                        (o for o in orders
                         if o['order_id'] == entry_order_id
                         and o['state'] == 5
                         and (o.get('cum_qty') or 0) >= qty),
                        None
                    )
                    if matched:
                        actual_entry_price = matched.get('exec_price') or matched.get('price')
                        logger.success(f"{symbol}: 約定確認OK（注文ID={entry_order_id} 約定価格={actual_entry_price}円 数量={matched.get('cum_qty')}株）")
                        break
                except Exception as e:
                    logger.warning(f"{symbol}: 約定確認エラー（get_orders失敗）: {e}")
                    # フォールバック：get_positions()で確認
                    try:
                        positions = self.kabu_client.get_positions()
                        filled_pos = next(
                            (p for p in positions if p['symbol'] == symbol and (p.get('qty') or 0) > 0),
                            None
                        )
                        if filled_pos:
                            actual_entry_price = filled_pos.get('price')
                            logger.success(f"{symbol}: 約定確認OK（フォールバック 約定価格={actual_entry_price}円）")
                            break
                    except Exception as e2:
                        logger.warning(f"{symbol}: フォールバック確認エラー: {e2}")

            if actual_entry_price is None:
                logger.warning(f"{symbol}: 約定タイムアウト → 注文キャンセル")
                try:
                    self.kabu_client.cancel_order(entry_order_id)
                except Exception as cancel_e:
                    logger.error(f"{symbol}: キャンセル失敗: {cancel_e}")
                return None

            # 損切り・利確を実際の約定価格基準で計算
            # 逆指値はAPI非対応（SOR+信用で「即座に発動」エラー多発）のため
            # 損切り・利確ともmonitor_positions()の監視型で実行
            stop_price = int(actual_entry_price * 0.99)
            # 利確価格（パターンによって変える）
            if entry_pattern == 'A':
                target_price = int(actual_entry_price * 1.02)   # A: +2.0%
                target_pct = '+2.0%'
            else:
                target_price = int(actual_entry_price * 1.015)  # B: +1.5%（MFE分析より）
                target_pct = '+1.5%'

            stop_order_id = None  # 監視型損切り（逆指値API不使用）
            target_order_id = None  # 監視型利確

            logger.info(f"{symbol}: 損切={stop_price}円(-1%) 利確={target_price}円({target_pct}) ※監視型（3秒ポーリング）")

            # エントリー時のVWAP・始値を取得（分析用）
            entry_vwap = symbol_info.get('vwap')
            entry_opening = symbol_info.get('opening_price')
            entry_vwap_ratio = ((actual_entry_price / entry_vwap) - 1) * 100 if entry_vwap and entry_vwap > 0 else None
            entry_gap_pct = ((actual_entry_price / entry_opening) - 1) * 100 if entry_opening and entry_opening > 0 else None

            # ポジション情報を保存
            position_info = {
                'entry_order_id': entry_order_id,
                'stop_order_id': stop_order_id,
                'target_order_id': target_order_id,
                'entry_price': actual_entry_price,
                'qty': qty,
                'stop_price': stop_price,
                'target_price': target_price,
                'entry_time': datetime.now(),
                'material_strength': '',
                'material_type': '',
                'volume_surge': 0.0,
                'entry_pattern': entry_pattern,
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
            # エントリー失敗銘柄を当日ブラックリストに追加（再挑戦防止）
            self.entry_blacklist.add(symbol)
            logger.info(f"{symbol}: ブラックリストに追加（当日中は再エントリーしない）")
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

        # 9:05以降はパターンAスキップ（寄り付き後は成行注文で意図しない価格で約定するリスク）
        now = datetime.now()
        if now.hour > 9 or (now.hour == 9 and now.minute >= 5):
            logger.warning(f"パターンA: 9:05以降のため本日のエントリーをスキップ（現在時刻: {now.strftime('%H:%M')}）")
            return

        # 日次損益リセット（日付が変わった場合）
        today = datetime.now().date()
        if self.last_trade_date != today:
            self.daily_profit_loss = 0.0
            self.last_trade_date = today
            logger.info(f"日次損益リセット（連敗カウント={self.consecutive_losses}回は継続）")

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

        # TOB・MBO銘柄を除外（material_summaryにキーワードが含まれる場合）
        # morning_screening.pyで除外済みだが、2重チェックとして実施
        tob_keywords = ['TOB', 'MBO', '公開買付', '株式交換', '完全子会社化', '非公開化',
                    '買収防衛', 'スクイーズアウト', '株式併合', '上場廃止']
        if 'material_summary' in candidates_df.columns:
            tob_mask = candidates_df['material_summary'].astype(str).apply(
                lambda s: any(kw in s for kw in tob_keywords)
            )
            excluded_tob = candidates_df[tob_mask]
            if len(excluded_tob) > 0:
                for _, row in excluded_tob.iterrows():
                    logger.info(f"TOB・MBO銘柄除外: {row.get('Code', '')} {row.get('material_summary', '')}")
            candidates_df = candidates_df[~tob_mask]

        if len(candidates_df) == 0:
            logger.info("TOB・MBO除外後の候補銘柄がありません")
            return

        # 優先順位でソート
        # 1. material_strength: '強' > '中'
        # 2. 同一強度内では TradingValue（売買代金絶対値）降順
        strength_order = {'強': 0, '中': 1}
        candidates_df['strength_rank'] = candidates_df['material_strength'].map(strength_order)
        candidates_df = candidates_df.sort_values(
            by=['strength_rank', 'TradingValue'],
            ascending=[True, False]
        )
        candidates_df = candidates_df.drop(columns=['strength_rank'])

        logger.info("優先順位ソート完了:")
        for idx, row in candidates_df.head(5).iterrows():
            logger.info(f"  {idx+1}. {row['Code']}: {row['material_strength']} 売買代金{row['TradingValue']/1e8:.1f}億円 出来高{row['VolumeSurgeRatio']:.2f}倍")

        # ====== フェーズ1: 注文発注（寄り前に全銘柄分を一括発注）======
        logger.info("=" * 60)
        logger.info("フェーズ1: 注文発注")
        logger.info("=" * 60)

        pending_orders = []  # [(symbol, entry_order_id, qty, current_price, row)]
        order_count = 0
        for idx, row in candidates_df.iterrows():
            if order_count >= self.max_positions_a:
                logger.info(f"パターンA注文上限到達（{order_count}/{self.max_positions_a}）")
                break

            symbol = str(row['Code'])

            # 9:05以降はスキップ
            now = datetime.now()
            if now.hour > 9 or (now.hour == 9 and now.minute >= 5):
                logger.warning(f"パターンA: 9:05以降のため本日のエントリーをスキップ（現在時刻: {now.strftime('%H:%M')}）")
                break

            logger.info(f"\n[エントリー候補] {symbol} ({row['material_strength']}, 売買代金{row['TradingValue']/1e8:.1f}億円)")

            # エントリーシグナルチェック
            if not self.check_entry_signal(symbol, pre_open=True):
                logger.info(f"{symbol}: エントリー条件不適合 → 次の候補へ")
                continue

            # ブラックリストチェック
            if symbol in self.entry_blacklist:
                logger.info(f"{symbol}: ブラックリスト → スキップ")
                continue

            # 銘柄情報取得・注文発注
            try:
                self.kabu_client.unregister_all()
                symbol_info = self.kabu_client.get_symbol(symbol, exchange=1)
                current_price = symbol_info.get('ask_price') or symbol_info.get('current_price') or 0
                if current_price <= 0:
                    logger.warning(f"{symbol}: 現在値取得失敗")
                    continue

                qty = self.calculate_position_size(current_price)
                if qty == 0:
                    continue

                limit_price = int(current_price * 1.05)
                logger.info(f"{symbol}: 注文発注 {qty}株 @ 指値{limit_price}円（前日終値{current_price}+5%）")
                entry_result = self.kabu_client.send_order(
                    symbol=symbol, exchange=9, side=2, qty=qty,
                    order_type=2, price=limit_price
                )
                if entry_result['result_code'] != 0:
                    logger.error(f"{symbol}: 注文失敗")
                    self.entry_blacklist.add(symbol)
                    continue

                entry_order_id = entry_result['order_id']
                logger.success(f"{symbol}: 注文成功 ID={entry_order_id}")
                pending_orders.append((symbol, entry_order_id, qty, current_price, row))
                order_count += 1

            except Exception as e:
                logger.error(f"{symbol}: 注文エラー: {e}")
                self.entry_blacklist.add(symbol)
                self.notifier.send_error(f"エントリーエラー: {symbol} - {str(e)[:100]}")

        if not pending_orders:
            logger.info("注文なし")
            logger.info("=" * 60)
            logger.info("日次自動売買完了: エントリー 0銘柄")
            logger.info("=" * 60)
            self.notifier.send_message(f"自動売買完了\nエントリー: 0銘柄\n候補: {len(candidates_df)}銘柄")
            return

        # ====== フェーズ2: 寄り付き待機 ======
        now = datetime.now()
        market_open = now.replace(hour=9, minute=0, second=30, microsecond=0)
        wait_seconds = (market_open - now).total_seconds()
        if wait_seconds > 0:
            logger.info(f"寄り付き待機 {wait_seconds:.0f}秒（{len(pending_orders)}銘柄の注文済み）")
            time.sleep(wait_seconds)
        else:
            logger.info("既に9:00:30を過ぎているため待機スキップ")

        # ====== フェーズ3: 約定確認・損切り/利確設定 ======
        logger.info("=" * 60)
        logger.info("フェーズ3: 約定確認")
        logger.info("=" * 60)

        entry_count = 0
        for symbol, entry_order_id, qty, current_price, row in pending_orders:
            # 約定確認（5秒×12回=最大1分）
            actual_entry_price = None
            for _ in range(12):
                time.sleep(5)
                try:
                    orders = self.kabu_client.get_orders(symbol=symbol)
                    matched = next(
                        (o for o in orders
                         if o['order_id'] == entry_order_id
                         and o['state'] == 5
                         and (o.get('cum_qty') or 0) >= qty),
                        None
                    )
                    if matched:
                        actual_entry_price = matched.get('exec_price') or matched.get('price')
                        logger.success(f"{symbol}: 約定確認OK（約定価格={actual_entry_price}円）")
                        break
                except Exception as e:
                    logger.warning(f"{symbol}: get_orders失敗: {e}")

            if actual_entry_price is None:
                logger.warning(f"{symbol}: 約定タイムアウト → 注文キャンセル")
                try:
                    self.kabu_client.cancel_order(entry_order_id)
                except Exception as e:
                    logger.error(f"{symbol}: キャンセル失敗: {e}")
                continue

            # 損切り・利確を約定価格基準で設定
            stop_price = int(actual_entry_price * 0.99)
            target_price = int(actual_entry_price * 1.02)

            symbol_info = self.kabu_client.get_symbol(symbol, exchange=1)
            entry_vwap = symbol_info.get('vwap')
            entry_opening = symbol_info.get('opening_price')
            entry_vwap_ratio = ((actual_entry_price / entry_vwap) - 1) * 100 if entry_vwap and entry_vwap > 0 else None
            entry_gap_pct = ((actual_entry_price / entry_opening) - 1) * 100 if entry_opening and entry_opening > 0 else None

            position_info = {
                'entry_order_id': entry_order_id,
                'stop_order_id': None,
                'target_order_id': None,
                'entry_price': actual_entry_price,
                'qty': qty,
                'stop_price': stop_price,
                'target_price': target_price,
                'entry_time': datetime.now(),
                'material_strength': row.get('material_strength', ''),
                'material_type': row.get('material_type', ''),
                'volume_surge': row.get('VolumeSurgeRatio', 0.0),
                'entry_pattern': 'A',
                'mfe_pct': 0.0,
                'mae_pct': 0.0,
                'entry_vwap_ratio': round(entry_vwap_ratio, 4) if entry_vwap_ratio is not None else None,
                'entry_gap_pct': round(entry_gap_pct, 4) if entry_gap_pct is not None else None,
                'opening_gap_pct': None,
            }

            # opening_gap_pct
            prev_close = row.get('C') or row.get('Close') or 0
            if prev_close and prev_close > 0 and actual_entry_price > 0:
                position_info['opening_gap_pct'] = round((actual_entry_price / prev_close - 1) * 100, 4)

            self.active_positions[symbol] = position_info

            self.notifier.send_trade_notification(
                action="エントリー",
                symbol=symbol,
                price=actual_entry_price,
                qty=qty,
                stop_price=stop_price,
                target_price=target_price
            )

            logger.success(f"{symbol}: エントリー完了 {qty}株@{actual_entry_price}円 損切={stop_price} 利確={target_price}（監視型3秒）")
            entry_count += 1

        logger.info("=" * 60)
        logger.info(f"日次自動売買完了: エントリー {entry_count}銘柄")
        logger.info("=" * 60)

        self.notifier.send_message(
            f"自動売買完了\nエントリー: {entry_count}銘柄\n候補: {len(candidates_df)}銘柄"
        )

    def _get_actual_exit_info(self, symbol, pos_info):
        """注文履歴から実際の約定価格と決済理由を取得（注文ID絞り込み付き）"""
        entry_price = pos_info.get('entry_price', 0)
        stop_price = pos_info.get('stop_price', 0)
        target_price = pos_info.get('target_price', 0)

        # 注文IDで絞り込み
        target_ids = {
            pos_info.get('stop_order_id'),
            pos_info.get('target_order_id'),
        } - {None}

        try:
            orders = self.kabu_client.get_orders(symbol=symbol)

            # 注文IDが一致する約定済み注文を探す
            if target_ids:
                matched = [
                    o for o in orders
                    if o.get('order_id') in target_ids
                    and o.get('state') == 5
                    and o.get('exec_price')
                ]
                if matched:
                    actual_price = matched[0]['exec_price']
                    if actual_price >= entry_price:
                        return actual_price, '利確'
                    else:
                        return actual_price, '損切り'

            # 注文IDで特定できなかった場合、売り・約定済み注文から探す
            for order in orders:
                if order.get('side') == '1' and order.get('state') == 5 and order.get('exec_price'):
                    actual_price = order['exec_price']
                    logger.warning(f"{symbol}: 注文IDで約定を特定できず。売り約定注文から推定。")
                    if actual_price >= entry_price:
                        return actual_price, '利確'
                    else:
                        return actual_price, '損切り'

        except Exception as e:
            logger.warning(f"{symbol}: 注文履歴取得失敗、推測価格を使用: {e}")

        # フォールバック: 推測ロジック
        logger.warning(f"{symbol}: 推測価格使用（注文履歴から約定価格を取得できず）")
        if target_price > 0 and entry_price > 0:
            return target_price, '利確'
        return stop_price, '損切り'

    def _cancel_existing_orders(self, symbol, pos_info):
        """既存の逆指値・利確指値注文をキャンセル（強制決済前に呼ぶ）"""
        for order_key, label in [('stop_order_id', '逆指値'), ('target_order_id', '利確指値')]:
            order_id = pos_info.get(order_key)
            if order_id:
                try:
                    result = self.kabu_client.cancel_order(order_id)
                    logger.info(f"{symbol}: {label}注文キャンセル (ID={order_id})")
                except Exception as e:
                    logger.warning(f"{symbol}: {label}キャンセル失敗: {e}")
                    self.notifier.send_error(f"⚠️ {symbol}: {label}キャンセル失敗: {e}")

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
                qty = pos.get('qty') or 0
                if qty <= 0:
                    continue  # 0株（決済済み残骸）はスキップ
                profit_loss = pos.get('profit_loss') or 0

                # 含み損の場合のみ決済
                if profit_loss < 0:
                    logger.warning(f"{symbol}: 含み損 {profit_loss:,.0f}円 → 前場引け強制決済")

                    # 既存の逆指値・利確指値をキャンセル
                    pos_info = self.active_positions.get(symbol, {})
                    self._cancel_existing_orders(symbol, pos_info)

                    # 成行売り注文
                    result = self.kabu_client.send_order(
                        symbol=symbol,
                        exchange=9,
                        side=1,  # 1=売
                        qty=qty,
                        order_type=1,  # 1=成行
                        price=0
                    )

                    # result_codeチェック
                    if result.get('result_code') != 0:
                        logger.error(f"{symbol}: 前場強制決済注文失敗 result_code={result.get('result_code')}")
                        self.notifier.send_error(f"⚠️ {symbol}: 前場強制決済失敗！手動確認を推奨")
                        continue

                    # トレード履歴保存
                    entry_price = pos_info.get('entry_price', 0)
                    self.save_trade_history(symbol, entry_price, pos['current_price'], qty, '前場強制')

                    # active_positionsから削除
                    if symbol in self.active_positions:
                        del self.active_positions[symbol]
                        logger.info(f"{symbol}: active_positionsから削除（前場強制決済完了）")

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
                qty = pos.get('qty') or 0
                if qty <= 0:
                    continue  # 0株（決済済み残骸）はスキップ
                profit_loss = pos.get('profit_loss') or 0

                logger.info(f"{symbol}: 損益={profit_loss:,.0f}円 → 大引け前強制決済")

                # 既存の逆指値・利確指値をキャンセル
                pos_info = self.active_positions.get(symbol, {})
                self._cancel_existing_orders(symbol, pos_info)

                # 成行売り注文
                result = self.kabu_client.send_order(
                    symbol=symbol,
                    exchange=9,
                    side=1,  # 1=売
                    qty=qty,
                    order_type=1,  # 1=成行
                    price=0
                )

                # result_codeチェック
                if result.get('result_code') != 0:
                    logger.error(f"{symbol}: 大引強制決済注文失敗 result_code={result.get('result_code')}")
                    self.notifier.send_error(f"⚠️ {symbol}: 大引強制決済失敗！手動確認を推奨")
                    continue

                # トレード履歴保存
                entry_price = pos_info.get('entry_price', 0)
                exit_reason = '大引強制'
                self.save_trade_history(symbol, entry_price, pos['current_price'], qty, exit_reason)

                # active_positionsから削除
                if symbol in self.active_positions:
                    del self.active_positions[symbol]
                    logger.info(f"{symbol}: active_positionsから削除（大引強制決済完了）")

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

            # 安全装置カウンター更新
            self.daily_profit_loss += profit_loss
            if profit_loss > 0:
                self.consecutive_losses = 0
            else:
                self.consecutive_losses += 1
            logger.info(f"安全装置更新: 日次損益={self.daily_profit_loss:+,.0f}円, 連敗={self.consecutive_losses}回")

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

    def generate_daily_report(self, opening_wallet=None, closing_wallet=None, take_profit_pct=2.0, stop_loss_pct=1.0):
        """
        本日のトレード結果を集計してレポートを生成

        Args:
            opening_wallet: 開場時の買付余力
            closing_wallet: 終了時の買付余力
            take_profit_pct: 利確設定（%）
            stop_loss_pct: 損切設定（%）

        Returns:
            dict: 日次レポートデータ
        """
        today_str = datetime.now().strftime('%Y%m%d')
        filepath = "data/trade_history.csv"

        trades = []
        if os.path.exists(filepath):
            df = pd.read_csv(filepath, encoding='utf-8-sig')
            df_today = df[df['date'].astype(str) == today_str]

            for _, row in df_today.iterrows():
                code = str(row.get('code', ''))

                # 銘柄名取得
                symbol_name = ''
                try:
                    board = self.kabu_client.get_symbol(code)
                    symbol_name = board.get('symbol_name', '') or ''
                except Exception:
                    pass

                trades.append({
                    'code': code,
                    'symbol_name': symbol_name,
                    'profit_loss': float(row.get('profit_loss', 0)),
                    'profit_pct': float(row.get('profit_pct', 0)),
                    'exit_reason': str(row.get('exit_reason', '')),
                    'material_strength': str(row.get('material_strength', '')),
                    'material_type': str(row.get('material_type', '')),
                    'volume_surge': float(row.get('volume_surge', 0)),
                    'entry_time': str(row.get('entry_time', '')),
                    'exit_time': str(row.get('exit_time', '')),
                    'hold_minutes': int(row.get('hold_minutes', 0)),
                    'mfe_pct': float(row.get('mfe_pct', 0)),
                    'mae_pct': float(row.get('mae_pct', 0)),
                    'entry_vwap_ratio': row.get('entry_vwap_ratio', ''),
                    'entry_pattern': str(row.get('entry_pattern', 'A')),
                })

        trade_count = len(trades)
        win_count = sum(1 for t in trades if t['profit_loss'] > 0)
        lose_count = sum(1 for t in trades if t['profit_loss'] <= 0)
        total_pnl = sum(t['profit_loss'] for t in trades)
        win_rate = win_count / trade_count * 100 if trade_count > 0 else 0

        wallet_diff = None
        if opening_wallet is not None and closing_wallet is not None:
            wallet_diff = closing_wallet - opening_wallet

        report = {
            'date': today_str,
            'trade_count': trade_count,
            'win_count': win_count,
            'lose_count': lose_count,
            'win_rate': round(win_rate, 1),
            'total_pnl': round(total_pnl, 1),
            'opening_wallet': opening_wallet,
            'closing_wallet': closing_wallet,
            'wallet_diff': wallet_diff,
            'take_profit_pct': take_profit_pct,
            'stop_loss_pct': stop_loss_pct,
            'trades': trades,
        }

        logger.info(f"日次レポート生成: {trade_count}件, 損益{total_pnl:+,.0f}円")
        return report

    def monitor_positions(self):
        """
        保有ポジションを監視
        決済済みポジション（逆指値/指値で自動約定）を検知してトレード履歴に保存

        安全対策：
        - get_positions()が空を返した場合、active_positionsに保有中なら
          APIエラーの可能性としてスキップ（誤キャンセル防止）
        - ポジション消失時はget_orders()で約定確認してから処理
        """
        try:
            positions = self.kabu_client.get_positions()
            current_symbols = {pos['symbol'] for pos in positions}

            # get_positions()が空だがactive_positionsに保有中 → APIエラーの可能性
            if len(positions) == 0 and len(self.active_positions) > 0:
                logger.warning(
                    "get_positions()が空を返しましたが、"
                    f"active_positionsに{len(self.active_positions)}銘柄保有中。"
                    "APIエラーの可能性があるためスキップします。"
                )
                self.notifier.send_error(
                    "⚠️ get_positions()が予期しない結果を返しました。手動確認を推奨します。"
                )
                return

            logger.info(f"保有ポジション: {len(positions)}件")

            # 実際に保有中の銘柄を集める（0株残骸を除外）
            active_symbols = set()
            for pos in positions:
                symbol = pos['symbol']
                qty = pos.get('qty', 0) or 0
                if qty <= 0:
                    # 0株（決済済み残骸）→ 同銘柄にqty>0の建玉がなければ決済検知
                    if symbol in self.active_positions:
                        # 同銘柄でqty>0の建玉が他にあるか確認
                        has_live_position = any(
                            p['symbol'] == symbol and (p.get('qty', 0) or 0) > 0
                            for p in positions
                        )
                        if has_live_position:
                            # 0株残骸だが生きた建玉がある → スキップ（誤検知防止）
                            continue
                        pos_info = self.active_positions[symbol]
                        entry_price = pos_info.get('entry_price', 0)
                        exit_price = pos.get('current_price') or pos.get('price') or entry_price
                        exit_qty = pos_info.get('qty', 0)
                        pnl = (exit_price - entry_price) * exit_qty if entry_price and exit_price else 0
                        pnl_pct = (exit_price / entry_price - 1) * 100 if entry_price and exit_price and entry_price > 0 else 0
                        logger.info(f"{symbol}: 逆指値約定検知（0株・全建玉決済済み）→ 損益{pnl:+,.0f}円（{pnl_pct:+.1f}%）")
                        self.save_trade_history(symbol, entry_price, exit_price, exit_qty, '損切り')
                        self.notifier.send_message(
                            f"✂️ {symbol}: 損切り決済 {entry_price}円→{exit_price}円（{pnl:+,.0f}円 / {pnl_pct:+.1f}%）"
                        )
                        del self.active_positions[symbol]
                    continue
                active_symbols.add(symbol)
                profit_loss = pos.get('profit_loss') or 0
                profit_loss_rate = pos.get('profit_loss_rate') or 0
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

            # 利確判定（+2%到達で逆指値キャンセル→成行売り）
            for symbol, pos_info in list(self.active_positions.items()):
                if symbol not in current_symbols:
                    continue  # 既に決済済み
                target_price = pos_info.get('target_price')
                if not target_price:
                    continue
                # 現在値取得（positionsのデータを使用）
                pos_data = next((p for p in positions if p['symbol'] == symbol), None)
                if not pos_data:
                    continue
                cp = pos_data.get('current_price') or 0
                if cp <= 0:
                    continue
                # 利確判定（+2%到達で成行売り）
                if cp >= target_price:
                    logger.info(f"{symbol}: 利確条件到達（現在値{cp} >= 目標{target_price}）→ 成行売り")
                    qty = pos_info.get('qty')
                    try:
                        self.kabu_client.send_order(
                            symbol=symbol, exchange=9, side=1, qty=qty,
                            order_type=1, price=0
                        )
                        # OCO補完：逆指値注文があればキャンセル
                        stop_order_id = pos_info.get('stop_order_id')
                        if stop_order_id:
                            try:
                                self.kabu_client.cancel_order(stop_order_id)
                                logger.info(f"{symbol}: 逆指値注文キャンセル完了（OCO）")
                            except Exception as e:
                                logger.warning(f"{symbol}: 逆指値キャンセル失敗（手動確認推奨）: {e}")
                        entry_price = pos_info.get('entry_price', 0)
                        profit_loss = (cp - entry_price) * qty if entry_price else 0
                        self.save_trade_history(symbol, entry_price, cp, qty, '利確')
                        del self.active_positions[symbol]
                        logger.info(f"{symbol}: 利確決済完了 {entry_price}円→{cp}円（{profit_loss:+,.0f}円）")
                        self.notifier.send_message(f"✅ {symbol}: 利確決済 {entry_price}円→{cp}円（{profit_loss:+,.0f}円）")
                        break
                    except Exception as e:
                        logger.error(f"{symbol}: 利確成行売り失敗: {e}")
                        self.notifier.send_error(f"🚨 {symbol}: 利確決済失敗！手動確認必須: {e}")
                    continue

                # 損切り判定（-1%到達で成行売り）
                stop_price = pos_info.get('stop_price')
                if stop_price and cp <= stop_price:
                    logger.info(f"{symbol}: 損切り条件到達（現在値{cp} <= 損切{stop_price}）→ 成行売り")
                    qty = pos_info.get('qty')
                    try:
                        self.kabu_client.send_order(
                            symbol=symbol, exchange=9, side=1, qty=qty,
                            order_type=1, price=0
                        )
                        entry_price = pos_info.get('entry_price', 0)
                        profit_loss = (cp - entry_price) * qty if entry_price else 0
                        self.save_trade_history(symbol, entry_price, cp, qty, '損切り')
                        del self.active_positions[symbol]
                        logger.info(f"{symbol}: 損切り決済完了 {entry_price}円→{cp}円（{profit_loss:+,.0f}円）")
                        self.notifier.send_message(f"✂️ {symbol}: 損切り決済 {entry_price}円→{cp}円（{profit_loss:+,.0f}円）")
                        break
                    except Exception as e:
                        logger.error(f"{symbol}: 損切り成行売り失敗: {e}")
                        self.notifier.send_error(f"🚨 {symbol}: 損切り決済失敗！手動確認必須: {e}")

            # 決済済みポジション検知（active_positionsにあるがAPIのポジションにない）
            closed_symbols = []
            for symbol, pos_info in list(self.active_positions.items()):
                if symbol not in current_symbols:
                    # get_orders()で実際に約定済みか確認
                    if not self._is_position_actually_closed(symbol, pos_info):
                        logger.warning(
                            f"{symbol}: get_positions()に存在しないが約定確認できず。"
                            "APIエラーの可能性があるためスキップ。"
                        )
                        self.notifier.send_error(
                            f"⚠️ {symbol}: ポジション消失を検知しましたが約定確認できず。手動確認を推奨します。"
                        )
                        continue

                    # 約定確認できた → 残注文キャンセル＋履歴保存
                    entry_price = pos_info.get('entry_price', 0)
                    qty = pos_info.get('qty', 0)

                    self._cancel_existing_orders(symbol, pos_info)

                    exit_price, exit_reason = self._get_actual_exit_info(symbol, pos_info)

                    self.save_trade_history(symbol, entry_price, exit_price, qty, exit_reason)
                    closed_symbols.append(symbol)
                    logger.info(f"{symbol}: 自動決済検知 → {exit_reason} @ {exit_price:.0f}円 トレード履歴保存")

            # 決済済みポジションをactive_positionsから削除
            for symbol in closed_symbols:
                del self.active_positions[symbol]

        except Exception as e:
            logger.error(f"ポジション監視エラー: {e}")

    def _is_position_actually_closed(self, symbol, pos_info):
        """get_orders()で実際に約定済みかを確認"""
        target_ids = {
            pos_info.get('stop_order_id'),
            pos_info.get('target_order_id'),
        } - {None}

        try:
            orders = self.kabu_client.get_orders(symbol=symbol)
            for order in orders:
                if order.get('state') == 5 and order.get('side') == '1':
                    # 注文IDが一致するか、売り約定済み注文があれば確定
                    if not target_ids or order.get('order_id') in target_ids:
                        return True
            return False
        except Exception as e:
            logger.warning(f"{symbol}: 約定確認のget_orders()失敗: {e}")
            return False

    # ========== パターンB エントリーロジック ==========

    def _is_etf(self, symbol, symbol_name):
        """ETF/ETN/REITかどうか判定（短縮名・正式名の両方に対応）"""
        etf_keywords = ['ETF', 'ETN', '投信', '上場投信', '債券', 'リート', 'REIT',
                        'インデックス', 'ファンド', 'ヘッジ', 'ブル', 'ベア',
                        'レバレッジ', '先進国', '新興国', 'ナスダック', 'S&P', 'TOPIX',
                        'MAXIS', 'NEXT', 'ダイワ', '野村', 'iシェアーズ',
                        # 全角対応
                        'ＥＴＦ', 'ＥＴＮ', 'ＲＥＩＴ',
                        # 短縮名対応
                        'ＭＸ', 'ＮＦ', 'ＳＭＤ', '上場', '国債', '米債',
                        'ｉＦ', 'インバース', 'ダブルインバース']
        for keyword in etf_keywords:
            if keyword in symbol_name:
                return True
        # 英数字混在コード（534A等）はETF/ETN
        try:
            code = int(symbol)
            if code < 1000 or code > 9999:
                return True
        except ValueError:
            return True
        return False

    def scan_pattern_b_candidates(self):
        """
        パターンB: 売買高急増ランキングから候補銘柄を検知し、
        価格履歴を蓄積する。

        Returns:
            list: 個別株上位10銘柄のシンボルリスト
        """
        try:
            # 登録銘柄を全解除（board取得で自動登録され上限50に達するため）
            self.kabu_client.unregister_all()

            ranking = self.kabu_client.get_ranking(ranking_type=6, exchange_division="ALL", limit=50)

            if not ranking:
                return []

            # 50件全体→ETF/低位株/薄商い除外→board取得→上位10件
            # 全フィルタ通過後に10件を確定する
            top_symbols = []
            for item in ranking:
                if len(top_symbols) >= 10:
                    break

                symbol = item['symbol']
                symbol_name = item.get('symbol_name', '')

                # ETFフィルタ
                if self._is_etf(symbol, symbol_name):
                    logger.info(f"パターンB除外（ETF）: {symbol} {symbol_name}")
                    continue

                # 株価フィルタ（200円以上）
                current_price = item.get('current_price', 0) or 0
                if current_price < 200:
                    logger.info(f"パターンB除外（低位株）: {symbol} 現在値{current_price}円")
                    continue

                # 予算上限フィルター（1単元100株が予算内か）
                # self.budgetは買付余力×INVESTMENT_RATIO適用済み
                if current_price * 100 > self.budget:
                    logger.info(f"パターンB除外（予算超過）: {symbol} 現在値{current_price}円 必要額{current_price*100:,.0f}円 予算{self.budget:,.0f}円")
                    continue

                # 売買代金フィルタ（3,000万円以上）
                # ランキングAPIのTradingVolumeは千株単位
                trading_volume = item.get('trading_volume', 0) or 0
                turnover = current_price * trading_volume * 1000
                if turnover < 30_000_000:
                    logger.info(f"パターンB除外（薄商い）: {symbol} 売買代金{turnover/10000:.0f}万円")
                    continue

                # /board で詳細情報取得
                try:
                    board = self.kabu_client.get_symbol(symbol)
                except Exception as e:
                    logger.info(f"パターンB除外（board取得失敗）: {symbol} {e}")
                    time.sleep(0.3)
                    continue

                # board正式名でETF再チェック（短縮名で漏れたETFを捕捉）
                full_name = board.get('symbol_name', '') or ''
                if self._is_etf(symbol, full_name):
                    logger.info(f"パターンB除外（ETF・正式名）: {symbol} {full_name}")
                    time.sleep(0.3)
                    continue

                # 時価総額フィルター（50億円以上）
                current_price_board = board.get('current_price') or 0
                if self.issued_shares_dict and current_price_board > 0:
                    issued_shares = self.issued_shares_dict.get(symbol)
                    if issued_shares:
                        market_cap = current_price_board * issued_shares
                        if market_cap < 5_000_000_000:
                            logger.info(f"パターンB除外（時価総額不足）: {symbol} {market_cap/100_000_000:.0f}億円")
                            time.sleep(0.3)
                            continue

                # 寄りからの上昇率フィルター（+3%超えは除外・高値掴み防止）
                current_price_board = board.get('current_price') or 0
                opening_price = board.get('opening_price') or 0
                if opening_price > 0 and current_price_board > 0:
                    change_from_open = (current_price_board - opening_price) / opening_price * 100
                    if change_from_open > 3.0:
                        logger.info(f"パターンB除外（高値）: {symbol} 寄りから+{change_from_open:.1f}%")
                        time.sleep(0.3)
                        continue

                # 板の厚さフィルター（AskQtyがエントリー予定株数の2倍未満は除外）
                ask_qty = board.get('ask_qty', 0) or 0
                if ask_qty > 0 and current_price_board > 0:
                    estimated_qty = max(100, int(self.budget / current_price_board / 100) * 100)
                    if ask_qty < estimated_qty * 2:
                        logger.info(f"パターンB除外（板薄）: {symbol} AskQty={ask_qty:.0f}株 < 必要{estimated_qty * 2}株")
                        time.sleep(0.3)
                        continue

                # 全フィルタ通過 → 採用
                top_symbols.append(symbol)
                logger.info(
                    f"パターンB採用: {symbol} {board.get('symbol_name', '')} "
                    f"現在値={current_price_board}円 "
                    f"VWAP={board.get('vwap')} "
                    f"始値={opening_price} "
                    f"売買代金={turnover/10000:.0f}万円"
                )

                # 累積出来高→差分（その1分間の出来高）に変換
                current_cumulative = board.get('trading_volume', 0) or 0
                last_cumulative = self.pattern_b_last_volume.get(symbol, 0)
                delta_volume = current_cumulative - last_cumulative
                if delta_volume < 0:
                    delta_volume = 0
                self.pattern_b_last_volume[symbol] = current_cumulative

                price_record = {
                    'time': datetime.now(),
                    'price': board.get('current_price'),
                    'volume': delta_volume,
                    'vwap': board.get('vwap'),
                    'opening_price': board.get('opening_price'),
                    'rapid_trade_pct': item.get('rapid_trade_pct', 0),
                }
                logger.debug(
                    f"パターンB {symbol}: 始値={price_record['opening_price']}, "
                    f"現在値={price_record['price']}, 出来高差分={delta_volume}, "
                    f"RapidTrade={price_record['rapid_trade_pct']:.1f}%"
                )

                # 価格履歴に追加（銘柄ごと）
                if symbol not in self.pattern_b_price_history:
                    self.pattern_b_price_history[symbol] = []
                self.pattern_b_price_history[symbol].append(price_record)

                # 直近25分のデータのみ保持（20分平均+バッファ）
                cutoff = datetime.now() - timedelta(minutes=25)
                self.pattern_b_price_history[symbol] = [
                    r for r in self.pattern_b_price_history[symbol]
                    if r['time'] >= cutoff
                ]

                time.sleep(0.3)  # API レート制限対策

            # candidates_*.csvの材料銘柄のboard取得・price_history蓄積
            priority_added = []
            for symbol in self.pattern_b_candidate_symbols:
                if symbol in top_symbols:
                    continue  # 既にランキング経由で追加済み
                if len(top_symbols) + len(priority_added) >= 10:
                    break  # 最大10件
                try:
                    board = self.kabu_client.get_symbol(symbol, exchange=1)
                    current_price_board = board.get('current_price') or 0
                    if current_price_board <= 0:
                        continue

                    # price_history蓄積
                    current_cumulative = board.get('trading_volume', 0) or 0
                    last_cumulative = self.pattern_b_last_volume.get(symbol, 0)
                    delta_volume = current_cumulative - last_cumulative
                    if delta_volume < 0:
                        delta_volume = 0
                    self.pattern_b_last_volume[symbol] = current_cumulative

                    price_record = {
                        'time': datetime.now(),
                        'price': current_price_board,
                        'volume': delta_volume,
                        'vwap': board.get('vwap'),
                        'opening_price': board.get('opening_price'),
                        'rapid_trade_pct': 0,
                    }
                    if symbol not in self.pattern_b_price_history:
                        self.pattern_b_price_history[symbol] = []
                    self.pattern_b_price_history[symbol].append(price_record)

                    cutoff = datetime.now() - timedelta(minutes=25)
                    self.pattern_b_price_history[symbol] = [
                        r for r in self.pattern_b_price_history[symbol]
                        if r['time'] >= cutoff
                    ]

                    priority_added.append(symbol)
                    logger.info(f"パターンB材料銘柄: {symbol} {board.get('symbol_name', '')} 現在値={current_price_board}円")
                    time.sleep(0.3)
                except Exception as e:
                    logger.debug(f"パターンB材料銘柄 {symbol}: board取得失敗: {e}")

            if priority_added:
                top_symbols = priority_added + top_symbols
                top_symbols = top_symbols[:10]

            logger.info(f"パターンBスキャン: ランキング{len(ranking)}件→フィルタ通過{len(top_symbols)}件（材料{len(priority_added)}件追加）")

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
        # パターンB上限チェック
        pattern_b_count = sum(1 for v in self.active_positions.values() if v.get('entry_pattern') == 'B')
        if pattern_b_count >= self.max_positions_b:
            return False
        if len(self.active_positions) >= self.max_positions_total:
            return False
        # 同一銘柄への重複エントリー防止
        if symbol in self.active_positions:
            return False

        # エントリー失敗銘柄は当日中スキップ
        if symbol in self.entry_blacklist:
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
            if current_price < vwap * 0.998:
                logger.info(f"パターンB {symbol}: ❌VWAP割れ（現在値{current_price} < VWAP{vwap:.0f}×0.998）")
                return False

        # 条件2: 現在値が寄り付き値から+3%以内
        # /board の OpeningPrice を使用（取得できなければ履歴の最初の価格で代用）
        opening_price = latest.get('opening_price')
        if opening_price is None or opening_price <= 0:
            opening_price = history[0]['price']
        if opening_price and opening_price > 0:
            change_from_open = (current_price - opening_price) / opening_price * 100
            if change_from_open > 3.0:
                logger.info(f"パターンB {symbol}: ❌高値掴み防止（寄りから+{change_from_open:.1f}%）")
                return False

        # 条件3: 直近5本が上昇トレンド（価格が切り上がっている）
        recent_5 = history[-5:]
        prices = [r['price'] for r in recent_5 if r['price'] is not None]
        if len(prices) < 5:
            logger.info(f"パターンB {symbol}: ❌履歴不足（{len(prices)}/5本）")
            return False

        up_count = sum(1 for i in range(len(prices) - 1) if prices[i + 1] > prices[i])
        is_uptrend = up_count >= 3  # 5本中3本以上
        if not is_uptrend:
            logger.info(f"パターンB {symbol}: ❌トレンドなし（直近5本で上昇{up_count}回）")
            return False

        # 条件4: 出来高急増（RapidTradePercentage優先、フォールバックで差分計算）
        latest_rapid = latest.get('rapid_trade_pct', 0)
        if latest_rapid > 0:
            # ランキングAPIの値を使用（100% = 通常の2倍）
            if latest_rapid < 100:
                logger.info(f"パターンB {symbol}: ❌出来高不足（RapidTrade={latest_rapid:.1f}%）")
                return False
        else:
            # フォールバック：自前の差分計算
            volumes = [r['volume'] for r in history if r.get('volume', 0) > 0]
            if len(volumes) >= 5:
                avg_volume = sum(volumes[:-1]) / len(volumes[:-1])
                latest_volume = volumes[-1]
                if avg_volume > 0 and latest_volume < avg_volume * 2.0:
                    logger.info(f"パターンB {symbol}: ❌出来高不足（差分: {latest_volume:.0f} < {avg_volume:.0f}×2）")
                    return False

        logger.info(f"パターンB {symbol}: エントリー条件充足（価格{current_price}, VWAP{vwap}, 5本上昇, RapidTrade={latest_rapid:.0f}%）")
        return True

    def execute_pattern_b_entry(self, symbol):
        """パターンBのエントリーを実行（entry_with_stop_and_targetに委譲）"""
        return self.entry_with_stop_and_target(symbol, entry_pattern='B')
