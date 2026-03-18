"""
自動売買実行スクリプト
タスクスケジューラから9:00に起動され、候補銘柄CSVを読み込んでエントリー注文を実行する

【タスクスケジューラ登録手順】
1. タスクスケジューラを開く
2. 「基本タスクの作成」を選択
3. 名前: "DayTrade Auto Trading 9:00"
4. トリガー: 毎日 9:00
5. 操作: プログラムの開始
6. プログラム/スクリプト: C:\work\git\auto-daytrade\venv\Scripts\python.exe
7. 引数の追加: auto_trading.py
8. 開始: C:\work\git\auto-daytrade
9. 完了

【動作フロー】
1. kabuステーション起動確認（API疎通チェック）
2. candidates_YYYYMMDD.csv の読み込み（当日分）
3. 候補がなければDiscord通知して終了
4. execute_daily_trading() を実行してエントリー
5. 15:30に自動終了
"""
import os
import sys
import time
from datetime import datetime, timedelta
from loguru import logger
from src.trading.trade_executor import TradeExecutor
from src.utils.notifier import DiscordNotifier
from src.utils.kabu_client import KabuClient

# ログファイル設定
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"auto_trading_{datetime.now().strftime('%Y%m%d')}.log")
logger.add(
    log_file,
    rotation="1 day",
    retention="30 days",
    encoding="utf-8",
    level="INFO"
)


def check_kabu_station_running():
    """
    kabuステーション起動確認（API疎通チェック）

    Returns:
        bool: kabuステーションが起動していればTrue
    """
    logger.info("=" * 60)
    logger.info("kabuステーション起動確認")
    logger.info("=" * 60)

    try:
        client = KabuClient()
        token = client.get_token()

        if token:
            logger.success("kabuステーションAPI疎通確認成功")
            logger.info(f"APIトークン: {token[:20]}...")
            return True
        else:
            logger.error("kabuステーションAPI疎通確認失敗: トークン取得失敗")
            return False

    except Exception as e:
        logger.error(f"kabuステーションAPI疎通確認エラー: {e}")
        return False


def check_candidates_exist():
    """
    candidates_YYYYMMDD.csv の存在確認

    Returns:
        tuple: (exists: bool, file_path: str)
    """
    today = datetime.now()
    csv_filename = f"candidates_{today.strftime('%Y%m%d')}.csv"
    csv_path = os.path.join("data", csv_filename)

    logger.info("=" * 60)
    logger.info("候補銘柄CSV確認")
    logger.info("=" * 60)
    logger.info(f"確認対象: {csv_path}")

    exists = os.path.exists(csv_path)

    if exists:
        logger.success(f"候補銘柄CSVが見つかりました: {csv_path}")
    else:
        logger.warning(f"候補銘柄CSVが見つかりません: {csv_path}")

    return exists, csv_path


def wait_until_end_of_day():
    """
    15:30まで待機してから終了
    """
    now = datetime.now()
    end_time = now.replace(hour=15, minute=30, second=0, microsecond=0)

    # すでに15:30を過ぎている場合は即座に終了
    if now >= end_time:
        logger.info("すでに15:30を過ぎているため、即座に終了します")
        return

    wait_seconds = (end_time - now).total_seconds()
    logger.info(f"15:30まで待機します（{wait_seconds:.0f}秒 = {wait_seconds/60:.1f}分）")

    try:
        time.sleep(wait_seconds)
        logger.info("15:30到達。自動売買スクリプトを終了します")
    except KeyboardInterrupt:
        logger.warning("待機中に手動終了されました")


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("自動売買スクリプト開始")
    logger.info(f"実行時刻: {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}")
    logger.info("=" * 60)

    notifier = DiscordNotifier()

    # STEP 1: kabuステーション起動確認
    if not check_kabu_station_running():
        error_msg = "kabuステーションが起動していません。APIに接続できませんでした。"
        logger.error(error_msg)
        notifier.send_error(error_msg)
        sys.exit(1)

    # STEP 2: 候補銘柄CSV確認
    exists, csv_path = check_candidates_exist()

    if not exists:
        warning_msg = f"候補銘柄CSVが見つかりません。エントリーなしで終了します。\nファイル: {csv_path}"
        logger.warning(warning_msg)
        notifier.send_message(f"⚠️ 自動売買スキップ\n{warning_msg}")
        sys.exit(0)

    # STEP 3: 自動売買実行
    logger.info("=" * 60)
    logger.info("自動売買実行開始")
    logger.info("=" * 60)

    try:
        # TradeExecutor初期化
        # 予算80万円、最大損失率3%、最大連敗3回
        executor = TradeExecutor(
            budget=800000,
            max_daily_loss_rate=0.03,
            max_consecutive_losses=3
        )

        # 日次自動売買実行
        executor.execute_daily_trading()

        logger.success("自動売買実行完了")

    except Exception as e:
        error_msg = f"自動売買実行エラー: {e}"
        logger.error(error_msg)
        notifier.send_error(error_msg)
        sys.exit(1)

    # STEP 4: 15:30まで待機
    wait_until_end_of_day()

    logger.info("=" * 60)
    logger.info("自動売買スクリプト正常終了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
