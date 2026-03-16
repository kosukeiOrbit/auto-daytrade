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


def filter_volume_surge(df_day, listed_info, surge_threshold=2.0, lookback_days=20):
    """
    1日分のデータから出来高急増銘柄を抽出

    Args:
        df_day: その日の全銘柄株価データ
        listed_info: 上場銘柄情報
        surge_threshold: 出来高急増倍率閾値
        lookback_days: 平均出来高の計算期間

    Returns:
        DataFrame: 出来高急増銘柄リスト
    """
    # 必要なカラムが揃っているか確認
    required_cols = ['Code', 'Date', 'O', 'H', 'L', 'C', 'Vo', 'Vwap']
    if not all(col in df_day.columns for col in required_cols):
        logger.warning(f"必要なカラムが不足しています: {df_day.columns.tolist()}")
        return pd.DataFrame()

    # 当日の出来高データ
    df_day = df_day.copy()
    df_day['Volume'] = df_day['Vo']
    df_day['VolumeAvg'] = df_day.get('VolAve', df_day['Vo'])  # 平均出来高（APIから取得できる場合）

    # 出来高急増倍率計算
    df_day['VolumeSurgeRatio'] = df_day['Volume'] / df_day['VolumeAvg'].replace(0, 1)

    # 出来高急増銘柄をフィルタ
    candidates = df_day[df_day['VolumeSurgeRatio'] >= surge_threshold].copy()

    if len(candidates) == 0:
        return pd.DataFrame()

    # 上場銘柄情報とマージして銘柄名を追加
    if listed_info is not None:
        listed_info_subset = listed_info[['Code', 'CompanyName', 'MarketCode']].copy()
        candidates = candidates.merge(listed_info_subset, on='Code', how='left')
        candidates.rename(columns={'CompanyName': 'Name'}, inplace=True)
    else:
        candidates['Name'] = ''
        candidates['MarketCode'] = ''

    # カラム名を統一
    candidates.rename(columns={
        'O': 'Open',
        'H': 'High',
        'L': 'Low',
        'C': 'Close',
        'Vo': 'Volume',
        'Vwap': 'VWAP'
    }, inplace=True)

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
    logger.info("過去60営業日分のcandidates CSV一括生成（API効率化版）")
    logger.info("=" * 60)

    # 日本時間
    jst = tz.gettz("Asia/Tokyo")
    now = datetime.now(jst)

    # 過去60営業日を取得
    logger.info("営業日リストを生成中...")
    business_days = get_business_days(now, num_days=60)
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
    logger.info("STEP 2: 株価データ取得（60営業日分、順次実行）")
    logger.info("=" * 60)

    logger.info(f"期間: {business_days[0].strftime('%Y-%m-%d')} ～ {business_days[-1].strftime('%Y-%m-%d')}")
    logger.info("日付ごとに順次取得中... (60リクエスト、約60秒)")

    df_all_list = []

    for i, date in enumerate(business_days, start=1):
        try:
            logger.info(f"  [{i}/{len(business_days)}] {date.strftime('%Y-%m-%d')} 取得中...")

            # 1日分の全銘柄データを取得
            df_day = jquants.client.get_eq_bars_daily(
                date_yyyymmdd=date.strftime('%Y-%m-%d')
            )

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

    # STEP 3: メモリ上で日付ごとに分割してCSV生成
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: 日付ごとにスクリーニング実行")
    logger.info("=" * 60)

    # 日付カラムをdatetime型に変換（時刻部分を正規化: 00:00:00）
    if 'Date' in df_all.columns:
        df_all['Date'] = pd.to_datetime(df_all['Date']).dt.normalize()
    else:
        logger.error("Dateカラムが見つかりません")
        return

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

            # 出来高急増銘柄を抽出
            candidates = filter_volume_surge(
                df_day,
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
