"""
kabu Station API 銘柄登録テスト
銘柄を登録してから価格情報を取得する
"""
import json
import requests
from loguru import logger
from src.utils.config import Config

def get_token():
    """APIトークンを取得"""
    api_url = Config.KABU_API_URL
    api_password = Config.KABU_API_PASSWORD

    url = f"{api_url}/token"
    headers = {"Content-Type": "application/json"}
    body = json.dumps({"APIPassword": api_password})

    response = requests.post(url, headers=headers, data=body, timeout=10)

    if response.status_code == 200:
        token_data = response.json()
        token = token_data.get('Token')
        logger.success(f"APIトークン取得成功: {token[:20]}...")
        return token
    else:
        logger.error(f"APIトークン取得失敗: {response.status_code} - {response.text}")
        return None

def register_symbol(token, symbol, exchange=1):
    """銘柄を登録"""
    api_url = Config.KABU_API_URL

    url = f"{api_url}/register"
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": token
    }
    body = json.dumps({
        "Symbols": [
            {
                "Symbol": symbol,
                "Exchange": exchange
            }
        ]
    })

    logger.info(f"銘柄登録リクエスト: {symbol}@{exchange}")
    response = requests.put(url, headers=headers, data=body, timeout=10)

    logger.info(f"銘柄登録レスポンス: {response.status_code}")
    logger.info(f"レスポンスボディ: {response.text}")

    if response.status_code == 200:
        logger.success(f"銘柄登録成功: {symbol}@{exchange}")
        return True
    else:
        logger.error(f"銘柄登録失敗: {response.status_code} - {response.text}")
        return False

def get_board(token, symbol, exchange=1):
    """板情報・時価情報を取得（/board エンドポイント）"""
    api_url = Config.KABU_API_URL

    url = f"{api_url}/board/{symbol}@{exchange}"
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": token
    }

    logger.info(f"板情報取得: {symbol}@{exchange}")
    response = requests.get(url, headers=headers, timeout=10)

    if response.status_code == 200:
        board_data = response.json()

        logger.info(f"レスポンスJSON: {json.dumps(board_data, indent=2, ensure_ascii=False)}")

        current_price = board_data.get('CurrentPrice')
        symbol_name = board_data.get('SymbolName')

        logger.success(f"板情報取得成功: {symbol} {symbol_name} 現在値={current_price}円")
        return board_data
    else:
        logger.error(f"板情報取得失敗: {response.status_code} - {response.text}")
        return None

def main():
    logger.info("=" * 60)
    logger.info("kabu Station API 銘柄登録テスト")
    logger.info("=" * 60)

    # STEP 1: トークン取得
    token = get_token()
    if not token:
        logger.error("トークン取得失敗")
        return

    # STEP 2: テスト銘柄（候補CSVから）
    test_symbols = [
        ("4075", 1),  # ﾃｲ･ｴｽ ﾃｯｸ - 東証
        ("7110", 1),  # ﾀﾂﾓ - 東証
        ("3070", 1),  # ｼﾞｪﾘｰﾋﾞｰﾝｽﾞｸﾞﾙｰﾌﾟ - 東証
    ]

    for symbol, exchange in test_symbols:
        logger.info("=" * 60)
        logger.info(f"テスト: {symbol}@{exchange}")
        logger.info("=" * 60)

        # 登録前に価格取得
        logger.info("【登録前】板情報取得")
        get_board(token, symbol, exchange)

        # 銘柄登録
        logger.info("【登録実行】")
        register_symbol(token, symbol, exchange)

        # 登録後に価格取得
        logger.info("【登録後】板情報取得")
        get_board(token, symbol, exchange)

        logger.info("")

if __name__ == "__main__":
    main()
