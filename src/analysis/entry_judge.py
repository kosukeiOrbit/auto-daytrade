"""
エントリー判定ロジック
"""
from typing import List, Dict
from loguru import logger
from .indicators import (
    calculate_vwap,
    calculate_change_rate,
    check_uptrend,
    check_vwap_touch
)


def check_entry(
    symbol: str,
    ohlcv: List[Dict],
    current_price: float,
    prev_close: float,
    max_gap_rate: float = 8.0,
    max_current_rate: float = 8.0,
    vwap_touch_tolerance: float = 1.005,
    trend_bars: int = 5
) -> Dict:
    """
    エントリー判定関数（設計書ベース）

    Parameters:
        symbol: 銘柄コード
        ohlcv: 当日の5分足データ
               [{"open", "high", "low", "close", "volume", "time"}, ...]
        current_price: 現在値
        prev_close: 前日終値
        max_gap_rate: 寄り付きギャップアップ上限（%）デフォルト8.0
        max_current_rate: 現時点上昇率上限（%）デフォルト8.0
        vwap_touch_tolerance: VWAPタッチ判定幅（倍率）デフォルト1.005
        trend_bars: トレンド判定に使用する本数 デフォルト5

    Returns:
        dict: 判定結果
            {
                "entry": bool,
                "reason": str,
                "entry_price": float (entryがTrueの場合),
                "stop_loss": float (entryがTrueの場合),
                "take_profit": float (entryがTrueの場合),
                "vwap": float (entryがTrueの場合),
                "gap_rate": float,
                "current_rate": float
            }
    """
    logger.info(f"エントリー判定開始: {symbol}")

    if not ohlcv or len(ohlcv) == 0:
        return {"entry": False, "reason": "OHLCVデータが空"}

    # 1. 前日比上昇率チェック
    open_price = ohlcv[0]["open"]  # 寄り付き価格
    gap_rate = calculate_change_rate(open_price, prev_close)
    current_rate = calculate_change_rate(current_price, prev_close)

    logger.debug(f"  ギャップ率: {gap_rate:.2f}%, 現在上昇率: {current_rate:.2f}%")

    if gap_rate > max_gap_rate:
        return {
            "entry": False,
            "reason": f"寄り付きギャップアップ+{max_gap_rate}%超・当日除外",
            "gap_rate": round(gap_rate, 2),
            "current_rate": round(current_rate, 2)
        }

    if current_rate > max_current_rate:
        return {
            "entry": False,
            "reason": f"現時点上昇率+{max_current_rate}%超・一時除外",
            "gap_rate": round(gap_rate, 2),
            "current_rate": round(current_rate, 2)
        }

    # 2. VWAP計算（当日始値から現在まで）
    vwap = calculate_vwap(ohlcv)

    if vwap == 0:
        return {
            "entry": False,
            "reason": "VWAP計算失敗",
            "gap_rate": round(gap_rate, 2),
            "current_rate": round(current_rate, 2)
        }

    logger.debug(f"  VWAP: {vwap:.2f}, 現在値: {current_price:.2f}")

    if current_price < vwap:
        return {
            "entry": False,
            "reason": "VWAP割れ",
            "gap_rate": round(gap_rate, 2),
            "current_rate": round(current_rate, 2),
            "vwap": round(vwap, 2)
        }

    # 3. トレンド判定（直近N本の終値が上昇トレンドか）
    if len(ohlcv) < trend_bars:
        return {
            "entry": False,
            "reason": f"データ不足（{len(ohlcv)}本 < {trend_bars}本）",
            "gap_rate": round(gap_rate, 2),
            "current_rate": round(current_rate, 2),
            "vwap": round(vwap, 2)
        }

    recent = ohlcv[-trend_bars:]
    closes = [bar["close"] for bar in recent]
    is_uptrend = check_uptrend(closes, min_bars=trend_bars)

    logger.debug(f"  トレンド判定: {'上昇' if is_uptrend else '非上昇'}")

    if not is_uptrend:
        return {
            "entry": False,
            "reason": "上昇トレンドではない",
            "gap_rate": round(gap_rate, 2),
            "current_rate": round(current_rate, 2),
            "vwap": round(vwap, 2)
        }

    # 4. VWAPタッチ確認（直近N本でVWAP付近に触れたか）
    vwap_touched = check_vwap_touch(recent, vwap, tolerance=vwap_touch_tolerance)

    logger.debug(f"  VWAPタッチ: {'あり' if vwap_touched else 'なし'}")

    if not vwap_touched:
        return {
            "entry": False,
            "reason": "VWAPタッチ未確認・飛びつき禁止",
            "gap_rate": round(gap_rate, 2),
            "current_rate": round(current_rate, 2),
            "vwap": round(vwap, 2)
        }

    # 5. エントリー可
    stop_loss = round(vwap * 0.99, 0)         # VWAP-1%
    take_profit = round(current_price * 1.02, 0)  # +2%

    logger.success(f"  ✓ エントリー条件クリア: {symbol}")

    return {
        "entry": True,
        "entry_price": current_price,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "vwap": round(vwap, 0),
        "gap_rate": round(gap_rate, 2),
        "current_rate": round(current_rate, 2),
        "reason": "全条件クリア"
    }
