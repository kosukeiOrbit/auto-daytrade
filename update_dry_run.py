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
from src.utils.kabu_client import KabuClient

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
    try:
        kabu = KabuClient()
    except Exception as e:
        logger.warning(f"KabuClient初期化失敗（時刻データ取得スキップ）: {e}")
        kabu = None

    # 当日の地合い（日経・TOPIX騰落率）
    nikkei_pct = None
    topix_pct = None
    try:
        import yfinance as yf
        for ticker, label in [('^N225', 'nikkei'), ('1306.T', 'topix')]:
            try:
                hist = yf.Ticker(ticker).history(period='5d')
                target_date = datetime.strptime(date_str, '%Y%m%d').date()
                day_data = hist[hist.index.date == target_date]
                if len(day_data) > 0:
                    o = day_data.iloc[0]['Open']
                    c = day_data.iloc[0]['Close']
                    if o > 0:
                        pct = (c - o) / o * 100
                        if label == 'nikkei':
                            nikkei_pct = round(pct, 2)
                        else:
                            topix_pct = round(pct, 2)
            except Exception as e:
                logger.debug(f"{ticker}取得失敗: {e}")
        logger.info(f"地合い: 日経{nikkei_pct:+.2f}% / TOPIX(1306){topix_pct:+.2f}%" if nikkei_pct is not None and topix_pct is not None else f"地合い: 日経{nikkei_pct} / TOPIX{topix_pct}")
    except Exception as e:
        logger.warning(f"地合い取得失敗: {e}")

    updated = False

    for rec in records:
        long_done = bool(rec.get('VirtualExitPrice'))
        short_done = bool(rec.get('ShortVirtualExitPrice'))
        if long_done and short_done:
            continue  # 両方計算済み → スキップ

        code = rec['Code']
        virtual_entry = float(rec['VirtualEntryPrice'])
        virtual_qty = int(float(rec['VirtualQty']))

        try:
            # OHLC取得（既存値を再利用 or 新規取得）
            if rec.get('OpenPrice') not in ('', None, 0, '0'):
                open_price = float(rec['OpenPrice'])
                high_price = float(rec['HighPrice'])
                low_price = float(rec['LowPrice'])
                close_price = float(rec['ClosePrice'])
            else:
                trade_date = datetime.strptime(date_str, '%Y%m%d')
                quotes = jquants.get_daily_quotes(code, date=trade_date)
                if quotes is None or len(quotes) == 0:
                    logger.warning(f"{code}: 当日データ未取得（API未反映の可能性）")
                    continue
                row = quotes.iloc[-1]
                open_price = row.get('O', 0) or row.get('Open', 0) or 0
                high_price = row.get('H', 0) or row.get('High', 0) or 0
                low_price = row.get('L', 0) or row.get('Low', 0) or 0
                close_price = row.get('C', 0) or row.get('Close', 0) or 0
                rec['OpenPrice'] = open_price
                rec['HighPrice'] = high_price
                rec['LowPrice'] = low_price
                rec['ClosePrice'] = close_price

            # 高値・安値の到達時刻（kabu /board から）
            if not rec.get('HighTime') and kabu is not None:
                try:
                    info = kabu.get_symbol(code, exchange=1)
                    # boardの正式レスポンスから時刻を取得（get_symbolではトリミング済みなので生APIを叩く）
                    import requests
                    url = f"{kabu.api_url}/board/{code}@1"
                    res = kabu._api_request(requests.get, url)
                    if res.status_code == 200:
                        b = res.json()
                        rec['HighTime'] = b.get('HighPriceTime', '') or ''
                        rec['LowTime'] = b.get('LowPriceTime', '') or ''
                        rec['OpenTime'] = b.get('OpeningPriceTime', '') or ''
                except Exception as e:
                    logger.debug(f"{code}: 時刻取得失敗: {e}")

            high_time = rec.get('HighTime', '') or ''
            low_time = rec.get('LowTime', '') or ''

            # 地合いデータ（全銘柄共通）
            if not rec.get('NikkeiChangePct') and nikkei_pct is not None:
                rec['NikkeiChangePct'] = nikkei_pct
            if not rec.get('TopixChangePct') and topix_pct is not None:
                rec['TopixChangePct'] = topix_pct

            # 時刻情報があれば「先に到達した方」を優先判定
            high_first = None  # True: 高値が先, False: 安値が先, None: 不明
            if high_time and low_time:
                high_first = high_time < low_time

            # ロング側（未計算なら）
            if not long_done:
                target_price = virtual_entry * (1 + TAKE_PROFIT_PCT / 100)
                stop_price = virtual_entry * (1 - STOP_LOSS_PCT / 100)
                reached_sl = low_price <= stop_price
                reached_tp = high_price >= target_price
                if reached_sl and reached_tp:
                    if high_first is True:
                        exit_price = round(target_price, 1); exit_reason = '利確'
                    elif high_first is False:
                        exit_price = round(stop_price, 1); exit_reason = '損切り'
                    else:
                        exit_price = round(stop_price, 1); exit_reason = '両方到達'
                elif reached_sl:
                    exit_price = round(stop_price, 1); exit_reason = '損切り'
                elif reached_tp:
                    exit_price = round(target_price, 1); exit_reason = '利確'
                else:
                    exit_price = close_price; exit_reason = '引け決済'
                pnl = (exit_price - virtual_entry) * virtual_qty
                pnl_pct = (exit_price / virtual_entry - 1) * 100 if virtual_entry > 0 else 0
                rec['VirtualExitPrice'] = round(exit_price, 1)
                rec['VirtualExitReason'] = exit_reason
                rec['VirtualPnL'] = round(pnl, 0)
                rec['VirtualPnLPct'] = round(pnl_pct, 2)
                rec['VirtualHoldMinutes'] = ''
            else:
                exit_reason = rec.get('VirtualExitReason', '')
                pnl = float(rec.get('VirtualPnL', 0) or 0)

            # ショート側（未計算なら）
            if not short_done:
                short_tp = virtual_entry * (1 - TAKE_PROFIT_PCT / 100)
                short_sl = virtual_entry * (1 + STOP_LOSS_PCT / 100)
                short_reached_tp = low_price <= short_tp
                short_reached_sl = high_price >= short_sl
                if short_reached_sl and short_reached_tp:
                    if high_first is True:
                        short_exit = round(short_sl, 1); short_reason = '損切り'
                    elif high_first is False:
                        short_exit = round(short_tp, 1); short_reason = '利確'
                    else:
                        short_exit = round(short_sl, 1); short_reason = '両方到達'
                elif short_reached_sl:
                    short_exit = round(short_sl, 1); short_reason = '損切り'
                elif short_reached_tp:
                    short_exit = round(short_tp, 1); short_reason = '利確'
                else:
                    short_exit = close_price; short_reason = '引け決済'
                short_pnl = (virtual_entry - short_exit) * virtual_qty
                short_pnl_pct = (virtual_entry / short_exit - 1) * 100 if short_exit > 0 else 0
                rec['ShortVirtualExitPrice'] = round(short_exit, 1)
                rec['ShortVirtualExitReason'] = short_reason
                rec['ShortVirtualPnL'] = round(short_pnl, 0)
                rec['ShortVirtualPnLPct'] = round(short_pnl_pct, 2)
            else:
                short_reason = rec.get('ShortVirtualExitReason', '')
                short_pnl = float(rec.get('ShortVirtualPnL', 0) or 0)

            updated = True
            logger.info(f"{code}: L:{exit_reason}{pnl:+,.0f} / S:{short_reason}{short_pnl:+,.0f}")

        except Exception as e:
            logger.warning(f"{code}: OHLCV取得失敗: {e}")

    if updated:
        # CSV上書き保存
        fieldnames = list(records[0].keys())
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        logger.info(f"CSV更新完了: {csv_path}")

    # Discord通知（CSV更新済みでも通知再送）
    completed = [r for r in records if r.get('VirtualExitPrice')]
    if not completed:
        logger.info("通知対象なし")
        return

    total_pnl = sum(float(r['VirtualPnL']) for r in completed)
    wins = sum(1 for r in completed if float(r['VirtualPnL']) > 0)
    losses = len(completed) - wins
    win_rate = wins / len(completed) * 100 if completed else 0

    formatted_date = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"

    # 通過銘柄のみの小計
    passed_only = [r for r in completed if r.get('GapFilterResult') == '通過']
    passed_pnl = sum(float(r['VirtualPnL']) for r in passed_only) if passed_only else 0
    passed_wins = sum(1 for r in passed_only if float(r['VirtualPnL']) > 0)
    passed_losses = len(passed_only) - passed_wins

    # 通過銘柄の詳細（上位10件まで）
    msg = f"📊 [仮想結果] パターンA {len(completed)}銘柄（{formatted_date}）\n\n"
    for i, rec in enumerate(completed[:10], 1):
        entry = float(rec['VirtualEntryPrice'])
        exit_p = float(rec['VirtualExitPrice'])
        pnl = float(rec['VirtualPnL'])
        pnl_pct = float(rec['VirtualPnLPct'])
        gap_filter = rec.get('GapFilterResult', '')
        gap_mark = '✅' if gap_filter == '通過' else '❌'
        reason = rec['VirtualExitReason']
        reason_mark = '△' if reason == '両方到達' else reason
        msg += (
            f"{i}. {rec['Code']} {rec['SymbolName']} [{gap_mark}]: "
            f"{pnl_pct:+.1f}% {reason_mark}（{pnl:+,.0f}円）\n"
        )
    if len(completed) > 10:
        both_count = sum(1 for r in completed if r.get('VirtualExitReason') == '両方到達')
        msg += f"...他{len(completed)-10}銘柄"
        if both_count > 0:
            msg += f"（うち△両方到達{both_count}件）"
        msg += "\n"

    if passed_only:
        p_both = sum(1 for r in passed_only if r.get('VirtualExitReason') == '両方到達')
        msg += f"\n✅通過のみ: {passed_pnl:+,.0f}円（{passed_wins}勝{passed_losses}敗"
        if p_both > 0:
            msg += f" △{p_both}件"
        msg += "）"
    msg += f"\n全体合計: {total_pnl:+,.0f}円（{wins}勝{losses}敗 勝率{win_rate:.0f}%）"

    # ショート版集計
    short_completed = [r for r in completed if r.get('ShortVirtualPnL') not in ('', None)]
    if short_completed:
        s_total = sum(float(r['ShortVirtualPnL']) for r in short_completed)
        s_wins = sum(1 for r in short_completed if float(r['ShortVirtualPnL']) > 0)
        s_losses = len(short_completed) - s_wins
        s_both = sum(1 for r in short_completed if r.get('ShortVirtualExitReason') == '両方到達')

        # GAP+0.5%以上の銘柄に絞ったショート（実戦想定）
        short_target = [r for r in short_completed if r.get('PreGapPct') not in ('', None) and float(r['PreGapPct']) >= 0.5]
        if short_target:
            st_pnl = sum(float(r['ShortVirtualPnL']) for r in short_target)
            st_wins = sum(1 for r in short_target if float(r['ShortVirtualPnL']) > 0)
            st_losses = len(short_target) - st_wins
            st_both = sum(1 for r in short_target if r.get('ShortVirtualExitReason') == '両方到達')

        msg += f"\n\n🔻 [ショート仮想] {len(short_completed)}銘柄"
        if short_target:
            msg += f"\n  GAP≥+0.5%絞り: {st_pnl:+,.0f}円（{st_wins}勝{st_losses}敗"
            if st_both > 0: msg += f" △{st_both}件"
            msg += "）"
        msg += f"\n  全体合計: {s_total:+,.0f}円（{s_wins}勝{s_losses}敗"
        if s_both > 0: msg += f" △{s_both}件"
        msg += "）"

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
