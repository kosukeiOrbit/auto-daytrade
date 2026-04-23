"""
パターンA仮想モード（ドライラン）の引け後更新スクリプト
J-Quants から当日OHLCVを取得し、仮想損益を計算して CSV・Discord に反映する

タスクスケジューラ登録: 毎日 16:00 に実行
"""
import os
import sys
import csv
from datetime import datetime
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src.utils.jquants_client import JQuantsClient
from src.utils.notifier import DiscordNotifier

TAKE_PROFIT_PCT = 2.0
STOP_LOSS_PCT = 1.0


def update_dry_run(date_str=None):
    if date_str is None:
        date_str = datetime.now().strftime('%Y%m%d')

    csv_path = f"data/dry_run_{date_str}.csv"
    if not os.path.exists(csv_path):
        logger.info(f"ドライランCSVなし: {csv_path}")
        return

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        records = list(reader)

    if not records:
        logger.info("レコードなし")
        return

    jquants = JQuantsClient()
    notifier = DiscordNotifier()
    updated = False

    for rec in records:
        if rec.get('VirtualExitPrice'):
            continue

        if rec.get('GapFilterResult') != '通過':
            continue

        code = rec['Code']
        virtual_entry = float(rec['VirtualEntryPrice'])
        virtual_qty = int(float(rec['VirtualQty']))

        try:
            # J-Quants の日次データ取得
            # 当日分はAPIの反映タイミング次第（16:00以降推奨）
            quotes = jquants.get_daily_quotes(code, date_str=f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}")
            if quotes is None or len(quotes) == 0:
                logger.warning(f"{code}: 当日データ未取得（API未反映の可能性）")
                continue

            row = quotes.iloc[-1]
            open_price = row.get('Open', 0) or 0
            high_price = row.get('High', 0) or 0
            low_price = row.get('Low', 0) or 0
            close_price = row.get('Close', 0) or 0

            rec['OpenPrice'] = open_price
            rec['HighPrice'] = high_price
            rec['LowPrice'] = low_price
            rec['ClosePrice'] = close_price

            # 仮想損益判定
            target_price = virtual_entry * (1 + TAKE_PROFIT_PCT / 100)
            stop_price = virtual_entry * (1 - STOP_LOSS_PCT / 100)

            if low_price <= stop_price:
                exit_price = round(stop_price, 1)
                exit_reason = '損切り'
            elif high_price >= target_price:
                exit_price = round(target_price, 1)
                exit_reason = '利確'
            else:
                exit_price = close_price
                exit_reason = '引け決済'

            pnl = (exit_price - virtual_entry) * virtual_qty
            pnl_pct = (exit_price / virtual_entry - 1) * 100 if virtual_entry > 0 else 0

            rec['VirtualExitPrice'] = round(exit_price, 1)
            rec['VirtualExitReason'] = exit_reason
            rec['VirtualPnL'] = round(pnl, 0)
            rec['VirtualPnLPct'] = round(pnl_pct, 2)
            rec['VirtualHoldMinutes'] = ''

            updated = True
            logger.info(f"{code}: {virtual_entry}→{exit_price} {exit_reason} pnl={pnl:+,.0f}")

        except Exception as e:
            logger.warning(f"{code}: OHLCV取得失敗: {e}")

    if not updated:
        logger.info("更新対象なし")
        return

    # CSV上書き保存
    fieldnames = list(records[0].keys())
    with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    logger.info(f"CSV更新完了: {csv_path}")

    # Discord通知
    passed = [r for r in records if r.get('GapFilterResult') == '通過' and r.get('VirtualExitPrice')]
    if not passed:
        return

    total_pnl = sum(float(r['VirtualPnL']) for r in passed)
    wins = sum(1 for r in passed if float(r['VirtualPnL']) > 0)
    losses = len(passed) - wins
    win_rate = wins / len(passed) * 100 if passed else 0

    formatted_date = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
    msg = f"📊 [仮想結果] パターンA {len(passed)}銘柄（{formatted_date}）\n\n"
    for i, rec in enumerate(passed, 1):
        entry = float(rec['VirtualEntryPrice'])
        exit_p = float(rec['VirtualExitPrice'])
        pnl = float(rec['VirtualPnL'])
        pnl_pct = float(rec['VirtualPnLPct'])
        msg += (
            f"{i}. {rec['Code']} {rec['SymbolName']}: "
            f"{entry:,.0f}円→{exit_p:,.0f}円 {pnl_pct:+.1f}% "
            f"{rec['VirtualExitReason']}（仮想{pnl:+,.0f}円）\n"
        )

    msg += f"\n仮想合計: {total_pnl:+,.0f}円（{wins}勝{losses}敗 勝率{win_rate:.0f}%）"
    notifier.send_message(msg)
    logger.success("Discord通知完了")


if __name__ == "__main__":
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    logger.add(
        os.path.join(log_dir, f"update_dry_run_{datetime.now().strftime('%Y%m%d')}.log"),
        rotation="1 day", retention="30 days", encoding="utf-8", level="INFO"
    )

    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    update_dry_run(date_arg)
