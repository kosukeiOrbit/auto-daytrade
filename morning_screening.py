"""
朝6:30実行スクリーニング（出来高急増 + 地合いチェック + Claude材料判定）
"""
from datetime import datetime
from dateutil import tz
from loguru import logger
from src.screening import Screener
from src.utils.market_sentiment import MarketSentiment
from src.utils.material_judge import MaterialJudge


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("朝スクリーニング開始（morning_screening.py）")
    logger.info("=" * 60)

    # 日本時間
    jst = tz.gettz("Asia/Tokyo")
    today = datetime.now(jst)

    logger.info(f"実行日時: {today.strftime('%Y-%m-%d %H:%M:%S')}")

    # STEP 1a: 出来高急増銘柄を抽出
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1a: 出来高急増銘柄を抽出")
    logger.info("=" * 60)

    screener = Screener(budget=800_000)  # 予算80万円
    volume_candidates = screener.get_volume_surge_candidates(
        surge_threshold=2.0,  # 20日平均の2倍以上
        lookback_days=20,
        date=today
    )

    if len(volume_candidates) == 0:
        logger.warning("出来高急増銘柄が見つかりませんでした")
        logger.info("=" * 60)
        logger.info("朝スクリーニング終了（候補なし）")
        logger.info("=" * 60)
        return

    logger.success(f"出来高急増銘柄: {len(volume_candidates)}銘柄")

    # STEP 1b: TDnet引け後適時開示取得（TODO: 口座開設後に実装）
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1b: TDnet引け後適時開示取得（未実装）")
    logger.info("=" * 60)
    logger.warning("TDnet MCP連携は口座開設後に実装予定です")

    # STEP 1c: 候補プール（現時点では出来高急増銘柄のみ）
    candidates = volume_candidates.copy()

    # STEP 2: 地合いチェック
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: 地合いチェック（NYダウ・ナスダック）")
    logger.info("=" * 60)

    sentiment = MarketSentiment()
    us_market = sentiment.get_us_market_close()

    if us_market is None:
        logger.warning("米国市場データ取得失敗 → 地合いチェックスキップ")
        market_status = 'normal'
    else:
        market_status = sentiment.check_market_sentiment(
            us_market['dow_change_pct'],
            us_market['nasdaq_change_pct'],
            threshold=-2.0
        )

    # 地合いによる候補絞り込み
    if market_status == 'skip_all':
        logger.error("地合い悪化のため全スキップします")
        logger.info("=" * 60)
        logger.info("朝スクリーニング終了（地合い悪化）")
        logger.info("=" * 60)
        return
    elif market_status == 'volume_only':
        logger.warning("地合いやや悪化: 出来高急増銘柄のみ対象（TDnet銘柄は除外）")
        # 現時点ではTDnet銘柄がないため、そのまま継続

    # STEP 3: ニュース取得（TODO: 株探スクレイピング）
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: ニュース・開示内容取得（未実装）")
    logger.info("=" * 60)
    logger.warning("株探スクレイピングは今後実装予定です")

    # STEP 4: Claude API材料判定（ニュースがないためスキップ）
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: Claude API材料判定")
    logger.info("=" * 60)

    judge = MaterialJudge()

    # デモ: 最初の3銘柄だけ判定（ニュースがないため簡易判定）
    for idx, row in candidates.head(3).iterrows():
        code = row['Code']
        # 銘柄名は取得できないため、コードのみ
        name = f"銘柄{code}"

        # ニュースがないため、簡易テキストを生成
        news_text = f"出来高急増: {row['VolumeSurgeRatio']:.2f}倍"

        judgment = judge.judge_material(code, name, news_text)

        # 除外判定
        if judge.should_exclude(judgment):
            logger.info(f"  → 除外: {code} {name}")
        else:
            logger.info(f"  → 採用: {code} {name}")

    # STEP 5: 最終候補リスト出力
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: 最終候補リスト出力")
    logger.info("=" * 60)

    # CSV保存
    output_path = f"data/candidates_{today.strftime('%Y%m%d')}.csv"
    screener.save_candidates(candidates, filepath=output_path)

    logger.success(f"\n最終候補: {len(candidates)}銘柄")
    logger.success(f"保存先: {output_path}")

    # 上位5銘柄を表示
    logger.info("\n【上位5銘柄】")
    for idx, row in candidates.head(5).iterrows():
        logger.info(
            f"  {idx + 1}. {row['Code']}: "
            f"出来高急増{row['VolumeSurgeRatio']:.2f}倍, "
            f"終値{row['C']:.0f}円"
        )

    logger.info("\n" + "=" * 60)
    logger.info("朝スクリーニング完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
