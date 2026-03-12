"""
J-Quants API 接続テスト
"""
from loguru import logger
from src.utils.jquants_client import JQuantsClient


def main():
    """メイン処理"""
    logger.info("=" * 50)
    logger.info("J-Quants API 接続テスト開始")
    logger.info("=" * 50)

    try:
        # クライアント初期化
        client = JQuantsClient()

        # 接続テスト
        logger.info("\n[1] 接続テスト実行中...")
        if client.test_connection():
            logger.success("✓ 接続テスト成功")
        else:
            logger.error("✗ 接続テスト失敗")
            return

        # 上場銘柄一覧取得テスト
        logger.info("\n[2] 上場銘柄一覧取得テスト...")
        listed_info = client.get_listed_info()
        logger.info(f"取得銘柄数: {len(listed_info)}")
        logger.info(f"カラム: {list(listed_info.columns)}")
        logger.info(f"\n最初の3銘柄:\n{listed_info.head(3)}")

        # 日次株価データ取得テスト（1銘柄のみ）
        logger.info("\n[3] 日次株価データ取得テスト（トヨタ自動車: 7203）...")
        daily_quotes = client.get_daily_quotes(code="72030")
        if daily_quotes is not None and len(daily_quotes) > 0:
            logger.info(f"取得データ数: {len(daily_quotes)}")
            logger.info(f"\n最新データ:\n{daily_quotes.tail(3)}")
            logger.success("✓ 日次株価データ取得成功")
        else:
            logger.warning("日次株価データが空です")

        logger.info("\n" + "=" * 50)
        logger.success("全てのテストが完了しました")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
