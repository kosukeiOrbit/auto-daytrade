"""
スクリーニング機能テスト
"""
from loguru import logger
from src.screening import Screener


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("スクリーニング機能テスト開始")
    logger.info("=" * 60)

    try:
        # スクリーナー初期化（予算50万円と仮定）
        budget = 500_000  # 50万円
        screener = Screener(budget=budget)

        # スクリーニング実行
        # 条件: 上昇率3%以上、売買代金上位20銘柄
        candidates = screener.get_candidates(
            min_price_change_rate=3.0,
            top_n_by_value=20
        )

        if len(candidates) > 0:
            logger.info(f"\n候補銘柄一覧 (上位10銘柄):\n{candidates.head(10)}")

            # CSVに保存
            screener.save_candidates(candidates)

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
