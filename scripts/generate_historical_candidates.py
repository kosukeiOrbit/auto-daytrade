"""
過去60営業日分のcandidates CSVを一括生成（API効率化版）

J-Quants APIの一括取得を使って、1回のAPI呼び出しで60営業日分のデータを取得し、
メモリ上で日付ごとに分割してCSV生成します。

実行方法:
    python scripts/generate_historical_candidates.py
"""
import os
import sys
import time
from datetime import datetime, timedelta
from dateutil import tz
from loguru import logger
import jpholiday
import pandas as pd

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.jquants_client import JQuantsClient
from src.utils.config import Config

# ログファイル設定
os.makedirs("logs", exist_ok=True)
logger.add(
    "logs/generate_historical_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    encoding="utf-8",
    level="INFO"
)


def get_business_days(start_date, num_days=60):
    """
    過去N営業日のリストを生成（土日祝除外）

    Args:
        start_date: 起点日（datetime）
        num_days: 取得する営業日数

    Returns:
        list: 営業日のリスト（datetime）
    """
    business_days = []
    current_date = start_date

    while len(business_days) < num_days:
        # 土日祝日をスキップ
        if current_date.weekday() < 5 and not jpholiday.is_holiday(current_date):
            business_days.append(current_date)
        current_date -= timedelta(days=1)

    # 古い順にソート
    business_days.reverse()
    return business_days


def filter_volume_surge(df_day, df_all, listed_info, surge_threshold=2.0, lookback_days=20):
    """
    1日分のデータから出来高急増銘柄を抽出

    Args:
        df_day: その日の全銘柄株価データ
        df_all: 全期間の株価データ（前日ボラティリティ計算用）
        listed_info: 上場銘柄情報
        surge_threshold: 出来高急増倍率閾値
        lookback_days: 平均出来高の計算期間

    Returns:
        DataFrame: 出来高急増銘柄リスト
    """
    # 必要なカラムが揃っているか確認
    required_cols = ['Code', 'Date', 'O', 'H', 'L', 'C', 'Vo']
    if not all(col in df_day.columns for col in required_cols):
        logger.warning(f"必要なカラムが不足しています: {df_day.columns.tolist()}")
        return pd.DataFrame()

    # 当日の出来高データ
    df_day = df_day.copy()
    df_day['Volume'] = df_day['Vo']

    # VolAveカラムが存在しない、またはNaNの場合はフィルタリングできない
    if 'VolAve' not in df_day.columns:
        logger.warning("VolAveカラムが存在しません。20日移動平均が事前計算されていない可能性があります。")
        return pd.DataFrame()

    # VolAveがNaNでない銘柄のみを対象とする（20日分のデータがある銘柄）
    total_stocks = len(df_day)
    df_day = df_day[df_day['VolAve'].notna()].copy()
    valid_stocks = len(df_day)

    if len(df_day) == 0:
        logger.info(f"20日移動平均が計算できた銘柄がありません（全{total_stocks}銘柄中、有効データ0件）")
        return pd.DataFrame()

    logger.info(f"20日移動平均あり: {valid_stocks}/{total_stocks}銘柄")

    # 出来高急増倍率計算
    df_day['VolumeSurgeRatio'] = df_day['Volume'] / df_day['VolAve'].replace(0, 1)

    # デバッグ: 急増倍率の分布を確認
    ratio_stats = df_day['VolumeSurgeRatio'].describe()
    max_ratio = df_day['VolumeSurgeRatio'].max()
    logger.info(f"VolumeSurgeRatio 最大値: {max_ratio:.2f}x, 平均: {ratio_stats['mean']:.2f}x")

    # 出来高急増銘柄をフィルタ
    candidates = df_day[df_day['VolumeSurgeRatio'] >= surge_threshold].copy()

    if len(candidates) == 0:
        logger.info(f"出来高急増銘柄なし（閾値: {surge_threshold}x、最大: {max_ratio:.2f}x）")
        return pd.DataFrame()

    # 英数字混在コード除外（253A0, 382A0等）
    before_count = len(candidates)
    candidates['CodeStr'] = candidates['Code'].astype(str)
    candidates = candidates[candidates['CodeStr'].str.match(r'^\d+$')].copy()
    alpha_excluded = before_count - len(candidates)
    if alpha_excluded > 0:
        logger.info(f"  英数字混在コード除外: {alpha_excluded}件（ETN・外国ETF）")
    candidates = candidates.drop(columns=['CodeStr'])

    if len(candidates) == 0:
        logger.info("英数字混在コード除外後、候補なし")
        return pd.DataFrame()

    # 上場銘柄情報とマージして銘柄名を追加
    if listed_info is not None:
        listed_info_subset = listed_info[['Code', 'CoName', 'Mkt']].copy()
        candidates = candidates.merge(listed_info_subset, on='Code', how='left')
        candidates.rename(columns={'CoName': 'Name', 'Mkt': 'MarketCode'}, inplace=True)

        # ETF・債券系キーワード除外
        before_count = len(candidates)
        etf_keywords = [
            'ETF', 'ＥＴＦ', 'REIT', 'インデックス', '上場投資',
            '国債', 'TOPIX', 'Nikkei', '日経', 'インフラファンド',
            'ブラックロック', 'iシェアーズ', 'アセットマネジメント',
            '投資信託', 'ファンド', 'グローバルX', 'WisdomTree'
        ]
        mask_etf = candidates['Name'].fillna('').str.contains('|'.join(etf_keywords), case=False, regex=True)
        candidates = candidates[~mask_etf].copy()
        name_excluded = before_count - len(candidates)
        if name_excluded > 0:
            logger.info(f"  ETF・債券系除外: {name_excluded}件（銘柄名キーワード）")
    else:
        candidates['Name'] = ''
        candidates['MarketCode'] = ''

    if len(candidates) == 0:
        logger.info("ETF除外後、候補なし")
        return pd.DataFrame()

    # ボラティリティフィルタ（前日の日中値幅1.5%以上）
    target_date = df_day['Date'].iloc[0]
    df_all_sorted = df_all.sort_values(['Code', 'Date'])
    df_all_sorted['IntradayRange'] = ((df_all_sorted['H'] - df_all_sorted['L']) / df_all_sorted['O'] * 100)

    # 前日のデータを取得
    df_prev = df_all_sorted[df_all_sorted['Date'] < target_date].groupby('Code').tail(1)
    df_prev_volatility = df_prev[['Code', 'IntradayRange']].copy()
    df_prev_volatility = df_prev_volatility.rename(columns={'IntradayRange': 'PrevIntradayRange'})

    # マージ
    candidates = candidates.merge(df_prev_volatility, on='Code', how='left')

    # 前日値幅1.5%未満を除外
    before_count = len(candidates)
    candidates = candidates[candidates['PrevIntradayRange'] >= 1.5].copy()
    volatility_excluded = before_count - len(candidates)
    if volatility_excluded > 0:
        logger.info(f"  低ボラ除外: {volatility_excluded}件（前日値幅1.5%未満）")

    candidates = candidates.drop(columns=['PrevIntradayRange'], errors='ignore')

    if len(candidates) == 0:
        logger.info("ボラティリティフィルタ後、候補なし")
        return pd.DataFrame()

    # カラム名を統一
    rename_dict = {
        'O': 'Open',
        'H': 'High',
        'L': 'Low',
        'C': 'Close',
        'Vo': 'Volume'
    }
    # Vaカラムがあれば VWAPにリネーム
    if 'Va' in candidates.columns:
        rename_dict['Va'] = 'VWAP'
    candidates.rename(columns=rename_dict, inplace=True)

    # 出来高急増倍率でソート（降順）
    candidates = candidates.sort_values('VolumeSurgeRatio', ascending=False)

    # 必要なカラムのみ抽出
    output_cols = [
        'Code', 'Name', 'Date', 'Open', 'High', 'Low', 'Close',
        'Volume', 'VWAP', 'VolumeSurgeRatio', 'MarketCode'
    ]
    candidates = candidates[[col for col in output_cols if col in candidates.columns]]

    return candidates


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("過去120営業日分のcandidates CSV一括生成（キャッシュ利用版）")
    logger.info("=" * 60)

    # 日本時間
    jst = tz.gettz("Asia/Tokyo")
    now = datetime.now(jst)

    # 過去120営業日を取得（約6ヶ月間）
    logger.info("営業日リストを生成中...")
    business_days = get_business_days(now, num_days=120)
    logger.info(f"対象期間: {business_days[0].strftime('%Y-%m-%d')} ～ {business_days[-1].strftime('%Y-%m-%d')}")

    # STEP 1: J-Quants APIクライアント初期化
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: J-Quants API初期化")
    logger.info("=" * 60)
    jquants = JQuantsClient()

    # 上場銘柄情報取得（1回のみ）
    logger.info("上場銘柄情報を取得中...")
    listed_info = jquants.get_listed_info()
    logger.success(f"上場銘柄情報取得完了: {len(listed_info)}銘柄")

    # STEP 2: 株価データを日付ごとに順次取得（レート制限対策）
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: 株価データ取得（120営業日分、キャッシュ利用）")
    logger.info("=" * 60)

    logger.info(f"期間: {business_days[0].strftime('%Y-%m-%d')} ～ {business_days[-1].strftime('%Y-%m-%d')}")
    logger.info("日付ごとに順次取得中... (120リクエスト、キャッシュから高速取得)")

    df_all_list = []

    for i, date in enumerate(business_days, start=1):
        try:
            logger.info(f"  [{i}/{len(business_days)}] {date.strftime('%Y-%m-%d')} 取得中...")

            # 1日分の全銘柄データを取得（キャッシュ対応）
            df_day = jquants.get_daily_quotes(date=date)

            if df_day is not None and len(df_day) > 0:
                df_all_list.append(df_day)
                logger.info(f"    → {len(df_day)}銘柄取得")
            else:
                logger.warning(f"    → データなし（休場日の可能性）")

            # レート制限対策: 1秒待機（60リクエスト/分以内）
            if i < len(business_days):
                time.sleep(1)

        except Exception as e:
            logger.error(f"  [{i}/{len(business_days)}] {date.strftime('%Y-%m-%d')} エラー: {e}")
            # エラーが発生してもスキップして続行
            continue

    if len(df_all_list) == 0:
        logger.error("株価データが1件も取得できませんでした")
        raise Exception("株価データ取得失敗")

    # 全データを結合
    df_all = pd.concat(df_all_list, ignore_index=True)
    logger.success(f"株価データ取得完了: {len(df_all):,}行 ({len(df_all_list)}日分)")

    # 日付カラムをdatetime型に変換（時刻部分を正規化: 00:00:00）
    if 'Date' in df_all.columns:
        df_all['Date'] = pd.to_datetime(df_all['Date']).dt.normalize()
    else:
        logger.error("Dateカラムが見つかりません")
        return

    # STEP 2.5: 20日移動平均出来高を事前計算
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2.5: 20日移動平均出来高を計算")
    logger.info("=" * 60)

    # CodeとDateでソート（移動平均計算のため）
    df_all = df_all.sort_values(['Code', 'Date']).reset_index(drop=True)

    # 銘柄ごとに20日移動平均出来高を計算（min_periods=10で柔軟に）
    logger.info("銘柄ごとの20日移動平均を計算中...")
    df_all['VolAve'] = df_all.groupby('Code')['Vo'].transform(
        lambda x: x.rolling(window=20, min_periods=10).mean()
    )

    # 計算結果をログ出力
    valid_avg_count = df_all['VolAve'].notna().sum()
    logger.success(f"20日移動平均計算完了: {valid_avg_count:,}行（有効データ）")

    # STEP 3: メモリ上で日付ごとに分割してCSV生成
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: 日付ごとにスクリーニング実行")
    logger.info("=" * 60)

    success_count = 0
    skip_count = 0
    error_count = 0

    for i, date in enumerate(business_days, start=1):
        date_str = date.strftime('%Y%m%d')
        output_path = f"data/candidates_{date_str}.csv"

        # 既存ファイルチェック
        if os.path.exists(output_path):
            logger.info(f"[{i}/{len(business_days)}] {date.strftime('%Y-%m-%d')}: 既存ファイルをスキップ")
            skip_count += 1
            continue

        logger.info(f"[{i}/{len(business_days)}] {date.strftime('%Y-%m-%d')}: 処理中...")

        try:
            # その日のデータを抽出（Timestamp型で比較）
            df_day = df_all[df_all['Date'] == pd.Timestamp(date.date())].copy()

            if len(df_day) == 0:
                logger.warning(f"{date_str}: データなし（休場日の可能性）")
                error_count += 1
                continue

            logger.info(f"  → {len(df_day)}銘柄のデータを取得")

            # デバッグ: VolAveカラムの有無と内容を確認
            if 'VolAve' in df_day.columns:
                vol_ave_valid = df_day['VolAve'].notna().sum()
                logger.info(f"  VolAveカラム: あり、有効データ={vol_ave_valid}/{len(df_day)}")
            else:
                logger.warning(f"  VolAveカラム: なし")

            # 出来高急増銘柄を抽出（閾値: 2.0倍 = 20日平均の2.0倍以上）
            candidates = filter_volume_surge(
                df_day,
                df_all,  # 全期間データ（前日ボラティリティ計算用）
                listed_info,
                surge_threshold=2.0,
                lookback_days=20
            )

            if len(candidates) == 0:
                logger.warning(f"{date_str}: 候補銘柄なし")
                error_count += 1
                continue

            # 材料判定カラムを追加（全てTrueで固定、API節約）
            candidates['has_material'] = True
            candidates['material_strength'] = '中'  # デフォルト「中」
            candidates['material_type'] = '出来高急増'
            candidates['material_summary'] = f'{date_str} 出来高急増銘柄'

            # CSV保存
            os.makedirs("data", exist_ok=True)
            candidates.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.success(f"{date_str}: 保存完了 ({len(candidates)}銘柄) → {output_path}")

            success_count += 1

        except Exception as e:
            logger.error(f"{date_str}: エラー - {e}")
            error_count += 1
            continue

    # 結果サマリー
    logger.info("\n" + "=" * 60)
    logger.info("一括生成完了")
    logger.info("=" * 60)
    logger.info(f"成功: {success_count}日")
    logger.info(f"スキップ（既存）: {skip_count}日")
    logger.info(f"エラー: {error_count}日")
    logger.info(f"合計: {len(business_days)}日")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        # 設定検証
        Config.validate()
        main()
    except Exception as e:
        logger.error(f"実行エラー: {e}")
        logger.exception("詳細なエラー情報:")
        raise
