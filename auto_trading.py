"""
自動売買実行スクリプト
タスクスケジューラから8:45に起動され、候補銘柄CSVを読み込んでエントリー注文を実行する

【タスクスケジューラ登録手順】
1. タスクスケジューラを開く
2. 「基本タスクの作成」を選択
3. 名前: "DayTrade Auto Trading 8:45"
4. トリガー: 毎日 8:45
5. 操作: プログラムの開始
6. プログラム/スクリプト: C:\work\git\auto-daytrade\venv\Scripts\python.exe
7. 引数の追加: auto_trading.py
8. 開始: C:\work\git\auto-daytrade
9. 完了

【動作フロー】
1. kabuステーション起動確認（API疎通チェック）
2. candidates_YYYYMMDD.csv の読み込み（当日分）
3. 候補がなければDiscord通知して終了
4. execute_daily_trading() を実行してエントリー（寄成・前場注文）
5. 11:30 前場引け含み損決済
6. 15:20 大引け前全ポジション決済
7. 15:30に自動終了
"""
import os
import sys
import time
from datetime import datetime
from loguru import logger
import jpholiday
from src.trading.trade_executor import TradeExecutor
from src.utils.notifier import DiscordNotifier
from src.utils.kabu_client import KabuClient
from src.utils.config import Config

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


def trading_loop(executor, notifier=None):
    """
    取引時間中の監視ループ

    - 11:25-11:35: 含み損ポジション強制決済
    - 15:15-15:25: 全ポジション強制決済
    - 15:30: ループ終了

    Args:
        executor: TradeExecutor インスタンス
    """
    logger.info("=" * 60)
    logger.info("取引監視ループ開始")
    logger.info("=" * 60)

    midday_exit_done = False
    eod_exit_done = False

    end_time = datetime.now().replace(hour=15, minute=30, second=0, microsecond=0)

    while True:
        now = datetime.now()
        current_hour = now.hour
        current_minute = now.minute

        # 15:30到達で終了
        if now >= end_time:
            logger.info("15:30到達。取引監視ループを終了します")
            break

        # 11:25-11:35: 前場引け含み損決済
        if not midday_exit_done and current_hour == 11 and 25 <= current_minute <= 35:
            logger.info(f"[{now.strftime('%H:%M:%S')}] 前場引け時刻 → 含み損決済実行")
            try:
                executor.force_exit_losing_positions_midday()
                midday_exit_done = True
            except Exception as e:
                logger.error(f"前場引け決済エラー: {e}")
                if notifier:
                    notifier.send_error(f"⚠️ 前場引け決済失敗: {e}\nポジションを手動確認してください")

        # 15:15-15:25: 大引け前全決済
        if not eod_exit_done and current_hour == 15 and 15 <= current_minute <= 25:
            logger.info(f"[{now.strftime('%H:%M:%S')}] 大引け前 → 全ポジション決済実行")
            try:
                executor.force_exit_all_positions_eod()
                eod_exit_done = True
            except Exception as e:
                logger.error(f"大引け前決済エラー: {e}")
                if notifier:
                    notifier.send_error(f"⚠️ 大引け前決済失敗: {e}\nポジションを手動確認してください")

        # 1分待機
        time.sleep(60)


def main():
    """メイン処理"""
    now = datetime.now()

    logger.info("=" * 60)
    logger.info("自動売買スクリプト開始")
    logger.info(f"実行時刻: {now.strftime('%Y/%m/%d %H:%M:%S')}")
    logger.info("=" * 60)

    # 営業日チェック（土日祝日は静かに終了）
    if now.weekday() >= 5 or jpholiday.is_holiday(now):
        weekday_name = ['月', '火', '水', '木', '金', '土', '日'][now.weekday()]
        logger.info(f"本日は営業日ではありません ({weekday_name}・{'祝日' if jpholiday.is_holiday(now) else '週末'})")
        logger.info("自動売買スクリプトを終了します")
        sys.exit(0)

    notifier = DiscordNotifier()

    # STEP 1: kabuステーション起動確認
    if not check_kabu_station_running():
        error_msg = "kabuステーションが起動していません。APIに接続できませんでした。"
        logger.error(error_msg)
        notifier.send_error(error_msg)
        sys.exit(1)

    # STEP 2: 候補銘柄CSV確認（最大3回リトライ、朝スクリーニング遅延対策）
    exists, csv_path = check_candidates_exist()

    if not exists:
        for retry in range(1, 4):
            logger.info(f"CSV未検出。リトライ {retry}/3（30秒待機）...")
            time.sleep(30)
            exists, csv_path = check_candidates_exist()
            if exists:
                break

    if not exists:
        warning_msg = f"候補銘柄CSVが見つかりません。エントリーなしで終了します。\nファイル: {csv_path}"
        logger.warning(warning_msg)
        notifier.send_message(f"⚠️ 自動売買スキップ\n{warning_msg}")
        sys.exit(0)

    # STEP 3: 買付余力取得と予算計算
    logger.info("=" * 60)
    logger.info("買付余力取得")
    logger.info("=" * 60)

    try:
        client = KabuClient()
        wallet = client.get_wallet_cash()
        available_cash = wallet['stock_account_wallet']

        # 検証環境の場合（nullの場合）は固定予算を使用
        if available_cash is None:
            budget = 800_000
            logger.warning(f"検証環境のため固定予算を使用: {budget:,}円")
        else:
            # 本番環境: 買付余力 × 投資比率
            budget = int(available_cash * Config.INVESTMENT_RATIO)
            logger.info(f"買付余力: {available_cash:,}円 × {Config.INVESTMENT_RATIO} = 本日の投資予算: {budget:,}円")

    except Exception as e:
        # API取得失敗時は固定予算を使用
        budget = 800_000
        logger.warning(f"買付余力取得エラー、固定予算を使用: {e}")

    # STEP 4: TradeExecutor初期化
    logger.info("=" * 60)
    logger.info("TradeExecutor初期化")
    logger.info("=" * 60)

    try:
        # TradeExecutor初期化
        # 最大損失率3%、最大連敗3回
        executor = TradeExecutor(
            budget=budget,
            max_daily_loss_rate=0.03,
            max_consecutive_losses=3
        )
    except Exception as e:
        error_msg = f"TradeExecutor初期化エラー: {e}"
        logger.error(error_msg)
        notifier.send_error(error_msg)
        sys.exit(1)

    # STEP 5: エントリー実行
    logger.info("=" * 60)
    logger.info("エントリー実行")
    logger.info("=" * 60)

    try:
        executor.execute_daily_trading()
        logger.success("エントリー実行完了")
    except Exception as e:
        error_msg = f"エントリー実行エラー: {e}"
        logger.error(error_msg)
        notifier.send_error(f"⚠️ {error_msg}\nポジションが残っている可能性があります。取引監視ループは継続します。")

    # STEP 6: 取引監視ループ（11:30含み損決済、15:20全決済）
    # エントリーが失敗しても、既存ポジションの決済のために必ず実行する
    try:
        trading_loop(executor, notifier)
    except Exception as e:
        error_msg = f"取引監視ループエラー: {e}"
        logger.error(error_msg)
        notifier.send_error(f"⚠️ {error_msg}\nポジションを手動確認してください")

    logger.info("=" * 60)
    logger.info("自動売買スクリプト正常終了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
