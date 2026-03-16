"""
設定ファイルの読み込み
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# プロジェクトルートの.envファイルを読み込み
project_root = Path(__file__).parent.parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)


class Config:
    """環境変数から設定を取得"""

    # J-Quants API
    JQUANTS_API_KEY = os.getenv('JQUANTS_API_KEY')

    # Alpha Vantage API
    ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')

    # Discord Webhook
    DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')

    # kabu Station API (フェーズ5で使用)
    KABU_API_URL = os.getenv('KABU_API_URL', 'http://localhost:18080/kabusapi')
    KABU_API_PASSWORD = os.getenv('KABU_API_PASSWORD')

    @classmethod
    def validate(cls):
        """必須の設定が存在するかチェック"""
        if not cls.JQUANTS_API_KEY:
            raise ValueError(
                "JQUANTS_API_KEY が設定されていません。\n"
                ".env ファイルを作成して設定してください。"
            )
