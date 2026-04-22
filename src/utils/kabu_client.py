"""
kabu Station API クライアント
auカブコム証券のkabu Station APIとの通信を管理
"""
import os
import json
import requests
from datetime import datetime
from loguru import logger
from src.utils.config import Config


class KabuClient:
    """kabu Station API クライアント"""

    def __init__(self):
        """
        初期化
        環境変数から API URL と パスワードを取得
        """
        self.api_url = Config.KABU_API_URL
        self.api_password = Config.KABU_API_PASSWORD
        self.token = None
        self.token_expires_at = None

        logger.info(f"kabu Station API クライアントを初期化しました (URL: {self.api_url})")

    def _api_request(self, method, url, **kwargs):
        """
        API リクエストを送信（401時にトークンリフレッシュして1回リトライ）
        """
        token = self.get_token()
        headers = kwargs.pop('headers', {})
        headers['Content-Type'] = 'application/json'
        headers['X-API-KEY'] = token

        response = method(url, headers=headers, timeout=10, **kwargs)

        if response.status_code == 401:
            logger.warning("APIキー不一致（401）→ トークンをリフレッシュしてリトライ")
            token = self.get_token(force_refresh=True)
            headers['X-API-KEY'] = token
            response = method(url, headers=headers, timeout=10, **kwargs)

        return response

    def get_token(self, force_refresh=False):
        """
        APIトークンを取得

        Args:
            force_refresh: 強制的に新しいトークンを取得するか

        Returns:
            str: APIトークン

        Raises:
            Exception: トークン取得に失敗した場合
        """
        # 既存のトークンがあり、強制更新でなく、5分以内なら再利用
        if self.token and not force_refresh and self.token_expires_at:
            elapsed = (datetime.now() - self.token_expires_at).total_seconds()
            if elapsed < 300:  # 5分以内
                logger.debug("既存のAPIトークンを使用")
                return self.token

        logger.info("APIトークンを取得中...")

        try:
            url = f"{self.api_url}/token"
            headers = {
                "Content-Type": "application/json"
            }
            body = json.dumps({
                "APIPassword": self.api_password
            })

            response = requests.post(url, headers=headers, data=body, timeout=10)

            if response.status_code == 200:
                token_data = response.json()
                self.token = token_data.get('Token')
                self.token_expires_at = datetime.now()

                logger.success(f"APIトークン取得成功: {self.token[:20]}...")
                return self.token
            else:
                error_msg = f"APIトークン取得失敗: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            logger.error(f"APIトークン取得エラー: {e}")
            raise

    def get_wallet_cash(self):
        """
        買付余力を取得

        Returns:
            dict: {
                'stock_account_wallet': float,  # 現物買付余力
                'au_kc_stock_account_wallet': float,
                'au_jbn_stock_account_wallet': float
            }

        Raises:
            Exception: 取得に失敗した場合
        """
        try:
            url = f"{self.api_url}/wallet/cash"

            response = self._api_request(requests.get, url)

            if response.status_code == 200:
                wallet_data = response.json()

                result = {
                    'stock_account_wallet': wallet_data.get('StockAccountWallet'),
                    'au_kc_stock_account_wallet': wallet_data.get('AuKCStockAccountWallet'),
                    'au_jbn_stock_account_wallet': wallet_data.get('AuJbnStockAccountWallet')
                }

                logger.info(f"買付余力取得成功: 現物={result['stock_account_wallet']}円")
                return result
            else:
                error_msg = f"買付余力取得失敗: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            logger.error(f"買付余力取得エラー: {e}")
            raise

    def get_wallet_margin(self):
        """
        信用取引余力を取得（/wallet/margin）

        Returns:
            dict: {'margin_account_wallet': float}
        """
        try:
            url = f"{self.api_url}/wallet/margin"
            response = self._api_request(requests.get, url)
            if response.status_code == 200:
                data = response.json()
                result = {
                    'margin_account_wallet': data.get('MarginAccountWallet')
                }
                logger.info(f"信用余力取得成功: {result['margin_account_wallet']:,.0f}円")
                return result
            else:
                error_msg = f"信用余力取得失敗: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            logger.error(f"信用余力取得エラー: {e}")
            raise

    def get_symbol(self, symbol, exchange=1):
        """
        銘柄情報を取得（時価情報・板情報）

        注意: リアルタイム価格を取得するために /board エンドポイントを使用します。
              /symbol エンドポイントは静的な銘柄情報のみで CurrentPrice は含まれません。

        Args:
            symbol: 銘柄コード (例: "7203")
            exchange: 市場コード (9=SOR推奨, 1=東証は廃止)

        Returns:
            dict: {
                'symbol': str,           # 銘柄コード
                'symbol_name': str,      # 銘柄名
                'current_price': float,  # 現在値
                'bid_price': float,      # 買気配値
                'ask_price': float,      # 売気配値
                'trading_volume': int,   # 出来高
                'upper_limit': float,    # 値幅上限
                'lower_limit': float     # 値幅下限
            }

        Raises:
            Exception: 取得に失敗した場合
        """
        try:
            # リアルタイム価格取得には /board エンドポイントを使用
            url = f"{self.api_url}/board/{symbol}@{exchange}"

            response = self._api_request(requests.get, url)

            if response.status_code == 200:
                board_data = response.json()

                result = {
                    'symbol': board_data.get('Symbol'),
                    'symbol_name': board_data.get('SymbolName'),
                    'current_price': board_data.get('CurrentPrice'),
                    'bid_price': board_data.get('BidPrice'),
                    'ask_price': board_data.get('AskPrice'),
                    'trading_volume': board_data.get('TradingVolume'),
                    'vwap': board_data.get('VWAP'),
                    'opening_price': board_data.get('OpeningPrice'),
                    'ask_qty': board_data.get('AskQty'),
                    'previous_close': board_data.get('PreviousClose'),
                    'upper_limit': board_data.get('UpperLimit'),
                    'lower_limit': board_data.get('LowerLimit'),
                    'market_cap_value': board_data.get('MarketCapValue')
                }

                logger.info(f"銘柄情報取得成功: {result['symbol']} {result['symbol_name']} 現在値={result['current_price']}円")
                return result
            else:
                error_msg = f"銘柄情報取得失敗: {response.status_code} - {response.text}"
                if response.status_code == 400:
                    logger.debug(error_msg)  # 銘柄が見つからない等はDEBUGレベル
                else:
                    logger.error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            logger.debug(f"銘柄情報取得エラー: {e}")
            raise

    def send_order(self, symbol, exchange, side, qty, order_type, price=0, stop_price=0):
        """
        注文を発注

        Args:
            symbol: 銘柄コード (例: "7203")
            exchange: 市場コード (9=SOR推奨, 1=東証は廃止)
            side: 売買区分 (2=買, 1=売)
            qty: 数量
            order_type: 注文種類 (1=成行, 2=指値, 3=逆指値)
                        ※注意: APIのFrontOrderTypeは10=成行, 20=指値, 30=逆指値
            price: 指値価格 (成行の場合は0)
            stop_price: 逆指値トリガー価格 (逆指値の場合のみ)

        Returns:
            dict: {
                'order_id': str,  # 注文番号
                'result_code': int,
                'result_msg': str
            }

        Raises:
            Exception: 発注に失敗した場合
        """
        try:
            url = f"{self.api_url}/sendorder"

            # order_typeをAPIのFrontOrderTypeに変換
            # 1=成行 → 10, 2=指値 → 20, 3=逆指値 → 30
            # order_type >= 10 の場合はそのまま使用（直接FrontOrderType指定）
            if order_type >= 10:
                front_order_type = order_type
            else:
                front_order_type_map = {1: 10, 2: 20, 3: 30}
                front_order_type = front_order_type_map.get(order_type, 10)

            # 信用デイトレード（API経由で手数料・金利・貸株料無料）
            if side == 2:  # 買い（信用新規）
                cash_margin = 2   # 信用新規
                deliv_type = 0    # 指定なし
            else:  # 売り（信用返済）
                cash_margin = 3   # 信用返済
                deliv_type = 2    # お預り金（返済時必須）

            # 注文リクエストボディ
            order_data = {
                "Password": self.api_password,
                "Symbol": symbol,
                "Exchange": exchange,
                "SecurityType": 1,  # 1=株式
                "Side": str(side),
                "CashMargin": cash_margin,
                "MarginTradeType": 3,  # 3=一般信用（デイトレ）・手数料無料
                "DelivType": deliv_type,
                "FundType": "  ",  # 半角スペース2つ
                "AccountType": 4,  # 4=特定
                "Qty": qty,
                "FrontOrderType": front_order_type,  # 10=成行, 20=指値, 30=逆指値
                "Price": price,
                "ExpireDay": 0  # 0=当日中
            }

            # 信用返済の場合は建玉自動選択
            if cash_margin == 3:
                order_data["ClosePositionOrder"] = 0  # 0=日付（古い順）で自動選択

            # 逆指値の場合
            if order_type == 3:
                order_data["ReverseLimitOrder"] = {
                    "TriggerSec": 1,
                    "TriggerPrice": stop_price,
                    "UnderOver": 1 if side == 1 else 2,  # 売=1(以下), 買=2(以上)
                    "AfterHitOrderType": 1,  # 1=成行
                    "AfterHitPrice": 0
                }

            body = json.dumps(order_data)

            logger.info(f"注文発注: {symbol} {'買' if side == 2 else '売'} {qty}株 種類={order_type} (FrontOrderType={front_order_type})")
            logger.debug(f"注文データ: {order_data}")

            response = self._api_request(requests.post, url, data=body)

            if response.status_code == 200:
                order_result = response.json()

                result = {
                    'order_id': order_result.get('OrderId'),
                    'result_code': order_result.get('Result'),
                    'result_msg': order_result.get('ResultMsg', '')
                }

                if result['result_code'] == 0:
                    logger.success(f"注文発注成功: 注文番号={result['order_id']}")
                else:
                    logger.warning(f"注文発注警告: コード={result['result_code']} メッセージ={result['result_msg']}")

                return result
            else:
                error_msg = f"注文発注失敗: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            logger.error(f"注文発注エラー: {e}")
            raise

    def cancel_order(self, order_id):
        """
        注文を取消

        Args:
            order_id: 注文番号

        Returns:
            dict: {
                'result_code': int,
                'result_msg': str
            }

        Raises:
            Exception: 取消に失敗した場合
        """
        try:
            url = f"{self.api_url}/cancelorder"

            cancel_data = {
                "OrderId": order_id,
                "Password": self.api_password
            }

            body = json.dumps(cancel_data)

            logger.info(f"注文取消: 注文番号={order_id}")

            response = self._api_request(requests.put, url, data=body)

            if response.status_code == 200:
                cancel_result = response.json()

                result = {
                    'result_code': cancel_result.get('Result'),
                    'result_msg': cancel_result.get('ResultMsg', '')
                }

                if result['result_code'] == 0:
                    logger.success(f"注文取消成功: 注文番号={order_id}")
                else:
                    logger.warning(f"注文取消警告: コード={result['result_code']} メッセージ={result['result_msg']}")

                return result
            else:
                error_msg = f"注文取消失敗: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            logger.error(f"注文取消エラー: {e}")
            raise

    def get_positions(self):
        """
        保有ポジション一覧を取得

        Returns:
            list: [
                {
                    'symbol': str,           # 銘柄コード
                    'symbol_name': str,      # 銘柄名
                    'side': str,             # 売買区分 ("1"=売, "2"=買)
                    'qty': int,              # 数量
                    'price': float,          # 約定価格
                    'current_price': float,  # 現在値
                    'profit_loss': float,    # 評価損益
                    'profit_loss_rate': float # 評価損益率(%)
                },
                ...
            ]

        Raises:
            Exception: 取得に失敗した場合
        """
        try:
            url = f"{self.api_url}/positions"

            response = self._api_request(requests.get, url)

            if response.status_code == 200:
                positions_data = response.json()

                result = []
                for pos in positions_data:
                    position = {
                        'symbol': pos.get('Symbol'),
                        'symbol_name': pos.get('SymbolName'),
                        'side': pos.get('Side'),
                        'qty': pos.get('LeavesQty'),
                        'price': pos.get('Price'),
                        'current_price': pos.get('CurrentPrice'),
                        'profit_loss': pos.get('ProfitLoss'),
                        'profit_loss_rate': pos.get('ProfitLossRate')
                    }
                    result.append(position)

                logger.info(f"保有ポジション取得成功: {len(result)}件")
                return result
            else:
                error_msg = f"保有ポジション取得失敗: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            logger.error(f"保有ポジション取得エラー: {e}")
            raise

    def get_orders(self, symbol=None):
        """
        注文一覧を取得

        Args:
            symbol: 銘柄コード（Noneの場合は全注文）

        Returns:
            list: 注文リスト
        """
        try:
            url = f"{self.api_url}/orders"
            params = {'details': 'true'}
            if symbol:
                params['product'] = 0  # 0=すべて
                params['symbol'] = symbol

            response = self._api_request(requests.get, url, params=params)

            if response.status_code == 200:
                orders = response.json()
                result = []
                for order in orders:
                    # 約定済みの詳細を取得
                    details = order.get('Details', [])
                    exec_price = None
                    for detail in details:
                        if detail.get('ExecPrice') is not None:
                            exec_price = detail.get('ExecPrice')

                    result.append({
                        'order_id': order.get('ID', ''),
                        'symbol': order.get('Symbol', ''),
                        'side': order.get('Side', ''),
                        'state': order.get('State', 0),  # 5=完了
                        'order_type': order.get('CashMargin', 0),
                        'price': order.get('Price', 0),
                        'exec_price': exec_price,
                        'qty': order.get('OrderQty', 0),
                        'cum_qty': order.get('CumQty', 0),
                    })

                logger.debug(f"注文一覧取得成功: {len(result)}件")
                return result
            else:
                logger.error(f"注文一覧取得失敗: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"注文一覧取得エラー: {e}")
            return []

    def unregister_all(self):
        """登録銘柄を全解除（レジスト数上限50対策）"""
        try:
            url = f"{self.api_url}/unregister/all"
            response = self._api_request(requests.put, url)
            if response.status_code == 200:
                logger.debug("登録銘柄全解除成功")
            else:
                logger.debug(f"登録銘柄全解除失敗: {response.status_code}")
        except Exception as e:
            logger.debug(f"登録銘柄全解除エラー: {e}")

    def get_ranking(self, ranking_type=6, exchange_division="ALL", limit=50):
        """
        ランキング情報を取得

        Args:
            ranking_type: ランキング種別（6=売買高急増）
            exchange_division: 市場区分（"ALL"=全市場）

        Returns:
            list: ランキング銘柄リスト
                [{
                    'rank': int,
                    'symbol': str,
                    'symbol_name': str,
                    'current_price': float,
                    'change_pct': float,
                    'trading_volume': float,
                }]
        """
        try:
            url = f"{self.api_url}/ranking"
            params = {
                "Type": ranking_type,
                "ExchangeDivision": exchange_division,
                "Count": limit
            }

            response = self._api_request(requests.get, url, params=params)

            if response.status_code == 200:
                data = response.json()
                ranking_list = data.get('Ranking', [])

                result = []
                for item in ranking_list:
                    result.append({
                        'rank': item.get('No', 0),
                        'symbol': item.get('Symbol', ''),
                        'symbol_name': item.get('SymbolName', ''),
                        'current_price': item.get('CurrentPrice', 0),
                        'change_pct': item.get('ChangePercentage', 0),
                        'trading_volume': item.get('TradingVolume', 0),
                        'rapid_trade_pct': item.get('RapidTradePercentage', 0),
                    })

                logger.debug(f"ランキング取得成功: {len(result)}件")
                return result
            else:
                logger.error(f"ランキング取得失敗: {response.status_code} - {response.text}")
                return []

        except Exception as e:
            logger.error(f"ランキング取得エラー: {e}")
            return []
