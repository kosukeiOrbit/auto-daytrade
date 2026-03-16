"""
kabu Station API 疎通確認スクリプト
"""
import os
import json
import requests
from dotenv import load_dotenv
from loguru import logger

# 環境変数読み込み
load_dotenv()

KABU_API_URL = os.getenv('KABU_API_URL')
KABU_API_PASSWORD = os.getenv('KABU_API_PASSWORD')

print("=" * 60)
print("kabu Station API 疎通確認")
print("=" * 60)
print(f"API URL: {KABU_API_URL}")
print(f"API Password: {'*' * len(KABU_API_PASSWORD) if KABU_API_PASSWORD else 'None'}")
print()

# STEP 1: APIトークン取得
print("STEP 1: APIトークン取得")
print("-" * 60)

try:
    auth_url = f"{KABU_API_URL}/token"
    auth_payload = {
        "APIPassword": KABU_API_PASSWORD
    }

    headers = {
        "Content-Type": "application/json"
    }

    # kabu Station APIの仕様に合わせて、json.dumps()で文字列化してdata=で渡す
    body = json.dumps(auth_payload)

    print(f"Request URL: {auth_url}")
    print(f"Request Body: {auth_payload}")
    print(f"Request Headers: {headers}")

    response = requests.post(auth_url, headers=headers, data=body, timeout=10)

    print(f"Response Status: {response.status_code}")
    print(f"Response Body: {response.text}")

    if response.status_code == 200:
        token_data = response.json()
        api_token = token_data.get('Token')

        if api_token:
            print(f"[SUCCESS] APIトークン取得成功")
            print(f"Token: {api_token[:20]}..." if len(api_token) > 20 else f"Token: {api_token}")
        else:
            print(f"[ERROR] レスポンスにTokenが含まれていません")
            exit(1)
    else:
        print(f"[ERROR] APIトークン取得失敗")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        exit(1)

except Exception as e:
    print(f"[ERROR] APIトークン取得エラー: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print()

# STEP 2: 残高照会（買付余力）
print("STEP 2: 残高照会（買付余力）")
print("-" * 60)

try:
    wallet_url = f"{KABU_API_URL}/wallet/cash"
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": api_token
    }

    print(f"Request URL: {wallet_url}")
    print(f"Request Headers: X-API-KEY={api_token[:20]}...")

    response = requests.get(wallet_url, headers=headers, timeout=10)

    print(f"Response Status: {response.status_code}")
    print(f"Response Body: {response.text}")

    if response.status_code == 200:
        wallet_data = response.json()

        print(f"\n[SUCCESS] 残高照会成功")
        print("=" * 60)
        print("買付余力情報:")
        print("-" * 60)

        # 買付余力（現金）
        if 'StockAccountWallet' in wallet_data:
            cash_balance = wallet_data['StockAccountWallet']
            if cash_balance is not None:
                print(f"  現物買付余力: {cash_balance:,.0f}円")
            else:
                print(f"  現物買付余力: null (検証環境)")

        # その他の情報を表示
        for key, value in wallet_data.items():
            if key != 'StockAccountWallet':
                if value is not None:
                    print(f"  {key}: {value}")
                else:
                    print(f"  {key}: null (検証環境)")

        print("=" * 60)
    else:
        print(f"[ERROR] 残高照会失敗")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        exit(1)

except Exception as e:
    print(f"[ERROR] 残高照会エラー: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print()
print("=" * 60)
print("kabu Station API 疎通確認完了")
print("=" * 60)
