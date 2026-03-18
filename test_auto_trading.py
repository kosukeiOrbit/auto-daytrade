"""
auto_trading.pyのテスト版（15:30待機をスキップ）
検証モードでの動作確認用
"""
import os
import sys
from datetime import datetime
from loguru import logger
from src.trading.trade_executor import TradeExecutor
from src.utils.notifier import DiscordNotifier
from src.utils.kabu_client import KabuClient


def check_kabu_station_running():
    """kabuステーション起動確認（API疎通チェック）"""
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
    """candidates_YYYYMMDD.csv の存在確認"""
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


def main():
    """メイン処理（テスト版：15:30待機なし）"""
    logger.info("=" * 60)
    logger.info("自動売買テスト開始（検証モード）")
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
        # TradeExecutor初期化（検証環境用）
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
        import traceback
        traceback.print_exc()
        notifier.send_error(error_msg)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("自動売買テスト正常終了（15:30待機スキップ）")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
