"""
過去60営業日分のcandidates CSVを一括生成

J-Quants APIを使って過去の候補銘柄CSVを生成します。
材料判定はスキップし、has_material=True固定でAPI節約。

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

from src.screening import Screener
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


def generate_candidates_for_date(screener, date, max_retries=3):
    """
    指定日のcandidates CSVを生成（429エラーリトライ対応）

    Args:
        screener: Screenerインスタンス
        date: 対象日（datetime）
        max_retries: 429エラー時の最大リトライ回数

    Returns:
        tuple: (success: bool, filepath: str, skipped: bool)
    """
    date_str = date.strftime('%Y%m%d')
    output_path = f"data/candidates_{date_str}.csv"

    # 既存ファイルチェック
    if os.path.exists(output_path):
        logger.info(f"{date_str}: 既存ファイルをスキップ ({output_path})")
        return True, output_path, True  # skipped=True

    retry_count = 0
    while retry_count <= max_retries:
        try:
            # 出来高急増銘柄を取得
            candidates = screener.get_volume_surge_candidates(
                surge_threshold=2.0,
                lookback_days=20,
                date=date
            )

            if len(candidates) == 0:
                logger.warning(f"{date_str}: 候補銘柄なし")
                return False, None, False

            # 材料判定カラムを追加（全てTrueで固定、API節約）
            candidates['has_material'] = True
            candidates['material_strength'] = '中'  # デフォルト「中」
            candidates['material_type'] = '出来高急増'
            candidates['material_summary'] = f'{date_str} 出来高急増銘柄'

            # CSV保存
            screener.save_candidates(candidates, filepath=output_path)
            logger.success(f"{date_str}: 保存完了 ({len(candidates)}銘柄)")

            return True, output_path, False

        except Exception as e:
            error_msg = str(e)

            # 429エラー（レート制限）の場合はリトライ
            if '429' in error_msg or 'too many' in error_msg.lower():
                retry_count += 1
                if retry_count <= max_retries:
                    wait_time = 30
                    logger.warning(
                        f"{date_str}: レート制限エラー（429）- "
                        f"{wait_time}秒待機してリトライ ({retry_count}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"{date_str}: レート制限エラー - 最大リトライ回数超過")
                    return False, None, False
            else:
                # 429以外のエラーはリトライせず終了
                logger.error(f"{date_str}: エラー - {e}")
                return False, None, False

    return False, None, False


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("過去60営業日分のcandidates CSV一括生成")
    logger.info("=" * 60)

    # 日本時間
    jst = tz.gettz("Asia/Tokyo")
    now = datetime.now(jst)

    # 過去60営業日を取得
    logger.info("営業日リストを生成中...")
    business_days = get_business_days(now, num_days=60)
    logger.info(f"対象期間: {business_days[0].strftime('%Y-%m-%d')} ～ {business_days[-1].strftime('%Y-%m-%d')}")

    # Screener初期化（予算800万円固定）
    budget = 800_000
    screener = Screener(budget=budget)

    # 各営業日に対して処理
    success_count = 0
    skip_count = 0
    error_count = 0
    start_time = time.time()

    for i, date in enumerate(business_days, start=1):
        # 進捗表示（残り時間推定）
        if i > 1:
            elapsed = time.time() - start_time
            avg_time_per_day = elapsed / (i - 1)
            remaining_days = len(business_days) - i + 1
            estimated_remaining_minutes = (avg_time_per_day * remaining_days) / 60
            progress_info = f" (残り約{estimated_remaining_minutes:.1f}分)"
        else:
            progress_info = ""

        logger.info(f"\n[{i}/{len(business_days)}] {date.strftime('%Y-%m-%d')} 処理中...{progress_info}")

        success, filepath, skipped = generate_candidates_for_date(screener, date)

        if success:
            if skipped:
                skip_count += 1
            else:
                success_count += 1
        else:
            error_count += 1

        # APIレート制限対策：3秒待機（60リクエスト/分 = 1リクエスト/秒、余裕を持って3秒）
        if i < len(business_days):
            logger.debug("APIレート制限対策のため3秒待機...")
            time.sleep(3)

    # 結果サマリー
    total_time = (time.time() - start_time) / 60
    logger.info("\n" + "=" * 60)
    logger.info("一括生成完了")
    logger.info("=" * 60)
    logger.info(f"成功: {success_count}日")
    logger.info(f"スキップ（既存）: {skip_count}日")
    logger.info(f"エラー: {error_count}日")
    logger.info(f"合計: {len(business_days)}日")
    logger.info(f"所要時間: {total_time:.1f}分")
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
