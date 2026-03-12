"""
バックテストモジュールテスト
"""
from datetime import datetime
from loguru import logger
from src.backtest import BacktestEngine


def main():
    """メイン処理"""
    logger.info("=" * 60)
    logger.info("バックテストモジュールテスト開始")
    logger.info("=" * 60)

    # バックテストエンジン初期化
    engine = BacktestEngine(initial_capital=500_000)

    # テストシナリオ作成（仮想的なトレード履歴）
    test_scenarios = [
        # シナリオ1: 利確成功（+2%）
        {
            "symbol": "1001",
            "entry_time": datetime(2026, 3, 1, 9, 0),
            "entry_price": 1000,
            "stop_loss": 990,
            "take_profit": 1020,
            "exit_price": 1020,
            "exit_time": datetime(2026, 3, 1, 10, 0)
        },
        # シナリオ2: 損切（-1%）
        {
            "symbol": "1002",
            "entry_time": datetime(2026, 3, 2, 9, 0),
            "entry_price": 2000,
            "stop_loss": 1980,
            "take_profit": 2040,
            "exit_price": 1980,
            "exit_time": datetime(2026, 3, 2, 10, 0)
        },
        # シナリオ3: 利確成功（+2%）
        {
            "symbol": "1003",
            "entry_time": datetime(2026, 3, 3, 9, 0),
            "entry_price": 1500,
            "stop_loss": 1485,
            "take_profit": 1530,
            "exit_price": 1530,
            "exit_time": datetime(2026, 3, 3, 11, 0)
        },
        # シナリオ4: 利確成功（+2%）
        {
            "symbol": "1004",
            "entry_time": datetime(2026, 3, 4, 9, 0),
            "entry_price": 3000,
            "stop_loss": 2970,
            "take_profit": 3060,
            "exit_price": 3060,
            "exit_time": datetime(2026, 3, 4, 10, 30)
        },
        # シナリオ5: 損切（-1%）
        {
            "symbol": "1005",
            "entry_time": datetime(2026, 3, 5, 9, 0),
            "entry_price": 1200,
            "stop_loss": 1188,
            "take_profit": 1224,
            "exit_price": 1188,
            "exit_time": datetime(2026, 3, 5, 9, 30)
        },
        # シナリオ6: 利確成功（+2%）
        {
            "symbol": "1006",
            "entry_time": datetime(2026, 3, 6, 9, 0),
            "entry_price": 1800,
            "stop_loss": 1782,
            "take_profit": 1836,
            "exit_price": 1836,
            "exit_time": datetime(2026, 3, 6, 10, 0)
        },
        # シナリオ7: 利確成功（+2%）
        {
            "symbol": "1007",
            "entry_time": datetime(2026, 3, 7, 9, 0),
            "entry_price": 2500,
            "stop_loss": 2475,
            "take_profit": 2550,
            "exit_price": 2550,
            "exit_time": datetime(2026, 3, 7, 11, 0)
        },
        # シナリオ8: 利確成功（+2%）
        {
            "symbol": "1008",
            "entry_time": datetime(2026, 3, 8, 9, 0),
            "entry_price": 1600,
            "stop_loss": 1584,
            "take_profit": 1632,
            "exit_price": 1632,
            "exit_time": datetime(2026, 3, 8, 10, 30)
        },
        # シナリオ9: 損切（-1%）
        {
            "symbol": "1009",
            "entry_time": datetime(2026, 3, 9, 9, 0),
            "entry_price": 2200,
            "stop_loss": 2178,
            "take_profit": 2244,
            "exit_price": 2178,
            "exit_time": datetime(2026, 3, 9, 9, 45)
        },
        # シナリオ10: 利確成功（+2%）
        {
            "symbol": "1010",
            "entry_time": datetime(2026, 3, 10, 9, 0),
            "entry_price": 1400,
            "stop_loss": 1386,
            "take_profit": 1428,
            "exit_price": 1428,
            "exit_time": datetime(2026, 3, 10, 10, 0)
        }
    ]

    # バックテスト実行
    metrics = engine.run_simple_backtest(test_scenarios)

    # 結果表示
    logger.info("\n")
    metrics.display()

    # 合格基準チェック
    logger.info("\n")
    passed = metrics.passes_criteria()

    if passed:
        logger.success("✓ 全ての合格基準をクリアしました！")
    else:
        logger.warning("✗ 一部の基準を満たしていません")

    logger.info("\n" + "=" * 60)
    logger.info("テスト完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
