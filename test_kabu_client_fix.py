"""
修正後のkabu_client.pyをテスト
"""
from loguru import logger
from src.utils.kabu_client import KabuClient

def main():
    logger.info("=" * 60)
    logger.info("修正後のkabu_client.pyテスト")
    logger.info("=" * 60)

    client = KabuClient()

    # 候補銘柄から数銘柄テスト
    test_symbols = ["4075", "7110", "3070", "5888"]

    for symbol in test_symbols:
        logger.info("=" * 60)
        try:
            result = client.get_symbol(symbol, exchange=1)
            logger.success(f"✓ {symbol}: {result['symbol_name']} = {result['current_price']}円")
        except Exception as e:
            logger.error(f"✗ {symbol}: エラー - {e}")
        logger.info("")

if __name__ == "__main__":
    main()
