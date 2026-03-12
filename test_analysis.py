"""
分析モジュールテスト
"""
from loguru import logger
from src.analysis import calculate_vwap, calculate_ma, check_entry


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("分析モジュールテスト開始")
    logger.info("=" * 60)

    # サンプルデータ：右肩上がりでVWAPタッチありのケース
    sample_ohlcv = [
        {"time": "09:00", "open": 1000, "high": 1020, "low": 990, "close": 1010, "volume": 10000},
        {"time": "09:05", "open": 1010, "high": 1030, "low": 1005, "close": 1025, "volume": 8000},
        {"time": "09:10", "open": 1025, "high": 1035, "low": 1015, "close": 1020, "volume": 6000},  # 押し目
        {"time": "09:15", "open": 1020, "high": 1045, "low": 1018, "close": 1040, "volume": 12000},
        {"time": "09:20", "open": 1040, "high": 1055, "low": 1035, "close": 1050, "volume": 15000},
        {"time": "09:25", "open": 1050, "high": 1065, "low": 1048, "close": 1060, "volume": 18000},
    ]

    prev_close = 1000  # 前日終値（上昇率を8%以内に調整）
    current_price = 1060  # 現在値（+6%）

    # 1. VWAP計算テスト
    logger.info("\n[1] VWAP計算テスト")
    vwap = calculate_vwap(sample_ohlcv)
    logger.info(f"  VWAP: {vwap:.2f}円")

    # 2. 移動平均計算テスト
    logger.info("\n[2] 移動平均計算テスト")
    closes = [bar["close"] for bar in sample_ohlcv]
    ma5 = calculate_ma(closes, period=5)
    logger.info(f"  MA(5): {ma5:.2f}円")

    # 3. エントリー判定テスト（成功ケース）
    logger.info("\n[3] エントリー判定テスト（成功ケース）")
    result = check_entry(
        symbol="1234",
        ohlcv=sample_ohlcv,
        current_price=current_price,
        prev_close=prev_close
    )
    logger.info(f"  判定結果: {result}")

    # 4. エントリー判定テスト（ギャップアップ超過ケース）
    logger.info("\n[4] エントリー判定テスト（ギャップアップ超過）")
    result_gap = check_entry(
        symbol="5678",
        ohlcv=sample_ohlcv,
        current_price=current_price,
        prev_close=900  # ギャップアップ11%
    )
    logger.info(f"  判定結果: {result_gap}")

    # 5. エントリー判定テスト（VWAP割れケース）
    logger.info("\n[5] エントリー判定テスト（VWAP割れ）")
    result_below_vwap = check_entry(
        symbol="9999",
        ohlcv=sample_ohlcv,
        current_price=1000,  # VWAPより低い
        prev_close=prev_close
    )
    logger.info(f"  判定結果: {result_below_vwap}")

    logger.info("\n" + "=" * 60)
    logger.success("全てのテストが完了しました")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
