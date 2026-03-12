"""
スクリーニング機能テスト（前日データ）
"""
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
from src.screening import Screener


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("スクリーニング機能テスト開始（前日データ）")
    logger.info("=" * 60)

    try:
        # 前日の日付を取得
        jst = tz.gettz("Asia/Tokyo")
        yesterday = datetime.now(jst) - timedelta(days=1)
        logger.info(f"対象日: {yesterday.strftime('%Y-%m-%d')}")

        # スクリーナー初期化（予算50万円と仮定）
        budget = 500_000  # 50万円
        screener = Screener(budget=budget)

        # スクリーニング実行
        # 条件: 上昇率3%以上、売買代金上位20銘柄
        candidates = screener.get_candidates(
            min_price_change_rate=3.0,
            top_n_by_value=20,
            date=yesterday
        )

        if len(candidates) > 0:
            logger.info(f"\n候補銘柄一覧:\n{candidates}")

            # CSVに保存
            screener.save_candidates(candidates, filepath="data/candidates_test.csv")

            # 統計情報
            logger.info("\n統計情報:")
            logger.info(f"  平均上昇率: {candidates['ChangeRate'].mean():.2f}%")
            logger.info(f"  最大上昇率: {candidates['ChangeRate'].max():.2f}%")
            logger.info(f"  平均売買代金: {candidates['TradingValue'].mean():,.0f}円")
            logger.info(f"  平均終値: {candidates['C'].mean():.0f}円")

            logger.success(f"\n✓ スクリーニング成功: {len(candidates)}銘柄が条件に該当")
        else:
            logger.warning("該当する銘柄が見つかりませんでした")

    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
