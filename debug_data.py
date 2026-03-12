"""
データ構造確認用スクリプト
"""
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
from src.utils.jquants_client import JQuantsClient


def main():
    """メイン処理"""
    try:
        client = JQuantsClient()

        # 過去3日分のデータを取得
        jst = tz.gettz("Asia/Tokyo")
        today = datetime.now(jst)
        start_date = today - timedelta(days=5)

        logger.info(f"データ取得期間: {start_date.strftime('%Y-%m-%d')} 〜 {today.strftime('%Y-%m-%d')}")

        # 特定銘柄（トヨタ自動車）で確認
        df = client.get_daily_quotes(code="72030", date=start_date)

        logger.info(f"\n取得データ件数: {len(df)}")
        logger.info(f"\nカラム一覧:\n{df.columns.tolist()}")
        logger.info(f"\nデータサンプル（最新5件）:\n{df.tail(5)}")

        # 全銘柄の最新1日分
        logger.info("\n" + "=" * 60)
        logger.info("全銘柄の最新データを確認")
        df_all = client.get_daily_quotes(date=today)
        logger.info(f"取得件数: {len(df_all)}")
        logger.info(f"\nサンプル（最初の5件）:\n{df_all.head(5)}")

    except Exception as e:
        logger.error(f"エラー: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
