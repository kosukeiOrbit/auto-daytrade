"""
簡易版スクリーニングテスト（25日MA無し）
"""
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
from src.utils.jquants_client import JQuantsClient
from src.utils.news_scraper import NewsScraper
import pandas as pd


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("簡易版スクリーニングテスト（25日MAフィルタなし）")
    logger.info("=" * 60)

    jst = tz.gettz("Asia/Tokyo")
    test_date = datetime(2026, 3, 11, tzinfo=jst)

    logger.info(f"テスト日: {test_date.strftime('%Y-%m-%d')}")

    # J-Quants APIクライアント
    client = JQuantsClient()

    # 過去20日分のデータ取得
    start_date = test_date - timedelta(days=30)

    logger.info(f"\n株価データ取得中（{start_date.strftime('%Y-%m-%d')} 〜 {test_date.strftime('%Y-%m-%d')}）...")
    df_prices = client.client.get_eq_bars_daily_range(
        start_dt=start_date,
        end_dt=test_date
    )
    logger.info(f"取得件数: {len(df_prices)}件")

    # 日付を変換
    df_prices['Date'] = pd.to_datetime(df_prices['Date'])
    target_date_str = test_date.strftime('%Y-%m-%d')

    # 20日平均出来高を計算
    logger.info("\n20日平均出来高を計算中...")
    df_prices = df_prices.sort_values(['Code', 'Date'])
    df_prices['AvgVolume'] = df_prices.groupby('Code')['Vo'].transform(
        lambda x: x.rolling(window=20, min_periods=20).mean()
    )

    # 対象日のデータのみ抽出
    df_target = df_prices[df_prices['Date'] == target_date_str].copy()
    df_target = df_target[df_target['AvgVolume'].notna()]

    logger.info(f"対象日のデータ: {len(df_target)}件")

    # 4桁コードのみフィルタ（ETF等の5桁コードを除外）
    logger.info("\n4桁コードフィルタ（通常の株式銘柄のみ）...")
    # Codeが整数の場合は4桁(1000-9999)でフィルタ
    df_target = df_target[
        (df_target['Code'] >= 1000) & (df_target['Code'] <= 9999)
    ].copy()
    logger.info(f"4桁コード銘柄: {len(df_target)}件")

    # 出来高急増フィルタ（2倍以上）
    logger.info("\n出来高急増フィルタ（2.0倍以上）...")
    df_target['VolumeSurgeRatio'] = df_target['Vo'] / df_target['AvgVolume']
    df_filtered = df_target[df_target['VolumeSurgeRatio'] >= 2.0].copy()
    logger.info(f"出来高急増銘柄: {len(df_filtered)}件")

    # 25日移動平均を計算して分析
    logger.info("\n25日移動平均を計算中...")
    df_prices['MA25'] = df_prices.groupby('Code')['C'].transform(
        lambda x: x.rolling(window=25, min_periods=25).mean()
    )

    # 対象日のMA25をマージ
    df_ma = df_prices[df_prices['Date'] == target_date_str][['Code', 'MA25']].copy()
    df_filtered = df_filtered.merge(df_ma, on='Code', how='left')

    # MA25との差を計算
    df_filtered['MA25Diff'] = ((df_filtered['C'] - df_filtered['MA25']) / df_filtered['MA25'] * 100)

    # MA25以上、MA25の95%以上、MA25の90%以上でそれぞれカウント
    ma25_above = len(df_filtered[df_filtered['C'] >= df_filtered['MA25']])
    ma25_95pct = len(df_filtered[df_filtered['C'] >= df_filtered['MA25'] * 0.95])
    ma25_90pct = len(df_filtered[df_filtered['C'] >= df_filtered['MA25'] * 0.90])

    logger.info(f"  MA25以上: {ma25_above}件")
    logger.info(f"  MA25の95%以上: {ma25_95pct}件")
    logger.info(f"  MA25の90%以上: {ma25_90pct}件")

    # 予算内フィルタ（80万円以内）
    logger.info("\n予算内フィルタ (1単元100株 ≤ 800,000円)...")
    df_filtered['UnitPrice'] = df_filtered['C'] * 100
    df_filtered = df_filtered[df_filtered['UnitPrice'] <= 800_000].copy()
    logger.info(f"予算内銘柄: {len(df_filtered)}件")

    if len(df_filtered) == 0:
        logger.warning("候補銘柄なし")
        return

    # 出来高急増率で降順ソート
    df_filtered = df_filtered.sort_values('VolumeSurgeRatio', ascending=False)

    logger.success(f"\n最終候補: {len(df_filtered)}銘柄")

    # 上位3銘柄の詳細を表示
    logger.info("\n" + "=" * 60)
    logger.info("上位3銘柄の詳細")
    logger.info("=" * 60)

    news_scraper = NewsScraper()

    for idx, row in df_filtered.head(3).iterrows():
        code = str(row['Code'])
        company_name = news_scraper.get_company_name(code)
        news_text = news_scraper.get_stock_news(code, max_articles=2)

        logger.info(f"\n【{code} {company_name}】")
        logger.info(f"出来高急増率: {row['VolumeSurgeRatio']:.2f}倍")
        logger.info(f"終値: {row['C']:.0f}円")
        logger.info(f"25日移動平均: {row['MA25']:.0f}円 (差: {row['MA25Diff']:+.1f}%)")
        logger.info(f"1単元価格: {row['UnitPrice']:.0f}円")

        if news_text:
            logger.info(f"ニュース:\n{news_text[:300]}...")
        else:
            logger.info("ニュース: なし")

    logger.info("\n" + "=" * 60)
    logger.info("テスト完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
