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
        # 既存のトークンがあり、強制更新でない場合はそれを返す
        if self.token and not force_refresh:
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
        token = self.get_token()

        try:
            url = f"{self.api_url}/wallet/cash"
            headers = {
                "Content-Type": "application/json",
                "X-API-KEY": token
            }

            response = requests.get(url, headers=headers, timeout=10)

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

    def get_symbol(self, symbol, exchange=1):
        """
        銘柄情報を取得

        Args:
            symbol: 銘柄コード (例: "7203")
            exchange: 市場コード (1=東証, デフォルト1)

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
        token = self.get_token()

        try:
            url = f"{self.api_url}/symbol/{symbol}@{exchange}"
            headers = {
                "Content-Type": "application/json",
                "X-API-KEY": token
            }

            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                symbol_data = response.json()

                result = {
                    'symbol': symbol_data.get('Symbol'),
                    'symbol_name': symbol_data.get('SymbolName'),
                    'current_price': symbol_data.get('CurrentPrice'),
                    'bid_price': symbol_data.get('BidPrice'),
                    'ask_price': symbol_data.get('AskPrice'),
                    'trading_volume': symbol_data.get('TradingVolume'),
                    'upper_limit': symbol_data.get('UpperLimit'),
                    'lower_limit': symbol_data.get('LowerLimit')
                }

                logger.info(f"銘柄情報取得成功: {result['symbol']} {result['symbol_name']} 現在値={result['current_price']}円")
                return result
            else:
                error_msg = f"銘柄情報取得失敗: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            logger.error(f"銘柄情報取得エラー: {e}")
            raise

    def send_order(self, symbol, exchange, side, qty, order_type, price=0, stop_price=0):
        """
        注文を発注

        Args:
            symbol: 銘柄コード (例: "7203")
            exchange: 市場コード (1=東証)
            side: 売買区分 (2=買, 1=売)
            qty: 数量
            order_type: 注文種類 (1=成行, 2=指値, 3=逆指値)
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
        token = self.get_token()

        try:
            url = f"{self.api_url}/sendorder"
            headers = {
                "Content-Type": "application/json",
                "X-API-KEY": token
            }

            # 注文リクエストボディ
            order_data = {
                "Password": self.api_password,  # 注文パスワード（APIパスワードと同じ）
                "Symbol": symbol,
                "Exchange": exchange,
                "SecurityType": 1,  # 1=株式
                "Side": str(side),
                "CashMargin": 1,  # 1=現物
                "DelivType": 2,  # 2=お預り金
                "AccountType": 4,  # 4=特定
                "Qty": qty,
                "FrontOrderType": order_type,  # 1=成行, 2=指値, 3=逆指値
                "Price": price,
                "ExpireDay": 0  # 0=当日中
            }

            # 逆指値の場合
            if order_type == 3:
                order_data["TriggerPrice"] = stop_price
                order_data["UnderOver"] = 2 if side == 1 else 1  # 売=2(以下), 買=1(以上)

            body = json.dumps(order_data)

            logger.info(f"注文発注: {symbol} {'買' if side == 2 else '売'} {qty}株 種類={order_type}")
            logger.debug(f"注文データ: {order_data}")

            response = requests.post(url, headers=headers, data=body, timeout=10)

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
        token = self.get_token()

        try:
            url = f"{self.api_url}/cancelorder"
            headers = {
                "Content-Type": "application/json",
                "X-API-KEY": token
            }

            cancel_data = {
                "OrderId": order_id,
                "Password": self.api_password
            }

            body = json.dumps(cancel_data)

            logger.info(f"注文取消: 注文番号={order_id}")

            response = requests.put(url, headers=headers, data=body, timeout=10)

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
        token = self.get_token()

        try:
            url = f"{self.api_url}/positions"
            headers = {
                "Content-Type": "application/json",
                "X-API-KEY": token
            }

            response = requests.get(url, headers=headers, timeout=10)

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
