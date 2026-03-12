"""
テクニカル指標計算関数
"""
from typing import List, Dict
import pandas as pd
from loguru import logger


def calculate_vwap(ohlcv: List[Dict]) -> float:
    """
    VWAP（Volume Weighted Average Price）を計算

    Args:
        ohlcv: OHLCVデータのリスト
               [{"open": float, "high": float, "low": float,
                 "close": float, "volume": int, "time": str}, ...]

    Returns:
        float: VWAP値
    """
    if not ohlcv or len(ohlcv) == 0:
        logger.warning("VWAP計算: データが空です")
        return 0.0

    try:
        # 典型価格 = (高値 + 安値 + 終値) / 3
        cum_tp_vol = sum(
            (bar["high"] + bar["low"] + bar["close"]) / 3 * bar["volume"]
            for bar in ohlcv
        )
        cum_vol = sum(bar["volume"] for bar in ohlcv)

        vwap = cum_tp_vol / cum_vol if cum_vol > 0 else 0.0
        logger.debug(f"VWAP計算完了: {vwap:.2f} (データ数: {len(ohlcv)})")
        return vwap

    except Exception as e:
        logger.error(f"VWAP計算エラー: {e}")
        return 0.0


def calculate_ma(prices: List[float], period: int = 5) -> float:
    """
    移動平均（Moving Average）を計算

    Args:
        prices: 価格のリスト（新しい順）
        period: 期間（デフォルト5）

    Returns:
        float: 移動平均値
    """
    if not prices or len(prices) < period:
        logger.warning(f"MA計算: データ不足（必要: {period}件、実際: {len(prices) if prices else 0}件）")
        return 0.0

    try:
        # 直近period件の平均
        recent_prices = prices[-period:]
        ma = sum(recent_prices) / period
        logger.debug(f"MA({period})計算完了: {ma:.2f}")
        return ma

    except Exception as e:
        logger.error(f"MA計算エラー: {e}")
        return 0.0


def calculate_change_rate(current_price: float, base_price: float) -> float:
    """
    変化率を計算

    Args:
        current_price: 現在価格
        base_price: 基準価格

    Returns:
        float: 変化率（%）
    """
    if base_price == 0:
        logger.warning("変化率計算: 基準価格が0です")
        return 0.0

    try:
        change_rate = (current_price - base_price) / base_price * 100
        return change_rate

    except Exception as e:
        logger.error(f"変化率計算エラー: {e}")
        return 0.0


def check_uptrend(closes: List[float], min_bars: int = 5) -> bool:
    """
    上昇トレンドかどうかを判定

    Args:
        closes: 終値のリスト（古い順）
        min_bars: 判定に必要な最低本数

    Returns:
        bool: 上昇トレンドの場合True
    """
    if not closes or len(closes) < min_bars:
        logger.debug(f"トレンド判定: データ不足（必要: {min_bars}件、実際: {len(closes) if closes else 0}件）")
        return False

    try:
        # 直近の終値が最初の終値と中間の終値より高い
        is_uptrend = closes[-1] > closes[0] and closes[-1] > closes[len(closes) // 2]
        logger.debug(f"トレンド判定: {'上昇' if is_uptrend else '非上昇'} (最新: {closes[-1]}, 最古: {closes[0]})")
        return is_uptrend

    except Exception as e:
        logger.error(f"トレンド判定エラー: {e}")
        return False


def check_vwap_touch(ohlcv: List[Dict], vwap: float, tolerance: float = 1.005) -> bool:
    """
    VWAPタッチを確認

    Args:
        ohlcv: OHLCVデータのリスト（直近のデータ）
        vwap: VWAP値
        tolerance: タッチ判定の許容幅（VWAP × tolerance）

    Returns:
        bool: VWAPにタッチした場合True
    """
    if not ohlcv or vwap == 0:
        return False

    try:
        # 直近のバーでVWAP付近（tolerance範囲内）に安値が触れたか
        vwap_threshold = vwap * tolerance
        touched = any(bar["low"] <= vwap_threshold for bar in ohlcv)
        logger.debug(f"VWAPタッチ判定: {'あり' if touched else 'なし'} (VWAP: {vwap:.2f}, 閾値: {vwap_threshold:.2f})")
        return touched

    except Exception as e:
        logger.error(f"VWAPタッチ判定エラー: {e}")
        return False
