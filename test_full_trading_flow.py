"""
自動売買フルフローテスト（検証モード）
全APIの動作確認
"""
from loguru import logger
from datetime import datetime
from src.trading.trade_executor import TradeExecutor
from src.utils.kabu_client import KabuClient
from src.utils.notifier import DiscordNotifier
import pandas as pd

def test_api_connectivity():
    """API疎通確認"""
    logger.info("=" * 60)
    logger.info("STEP 1: API疎通確認")
    logger.info("=" * 60)

    client = KabuClient()

    try:
        # トークン取得
        token = client.get_token()
        logger.success(f"✓ トークン取得成功: {token[:20]}...")

        # 買付余力取得
        wallet = client.get_wallet_cash()
        logger.success(f"✓ 買付余力取得成功: {wallet['stock_account_wallet']:,.0f}円")

        # 保有ポジション取得
        positions = client.get_positions()
        logger.success(f"✓ 保有ポジション取得成功: {len(positions)}件")

        # 銘柄情報取得（候補から1銘柄）
        symbol_data = client.get_symbol("4075", exchange=1)
        logger.success(f"✓ 銘柄情報取得成功: {symbol_data['symbol_name']} = {symbol_data['current_price']}円")

        return True
    except Exception as e:
        logger.error(f"✗ API疎通確認失敗: {e}")
        return False

def test_candidate_loading():
    """候補銘柄CSV読み込みテスト"""
    logger.info("=" * 60)
    logger.info("STEP 2: 候補銘柄CSV読み込み")
    logger.info("=" * 60)

    try:
        today = datetime.now()
        csv_filename = f"candidates_{today.strftime('%Y%m%d')}.csv"
        csv_path = f"data/{csv_filename}"

        df = pd.read_csv(csv_path)
        logger.success(f"✓ CSV読み込み成功: {len(df)}件")
        logger.info(f"  カラム: {list(df.columns)}")

        # 材料強度分布
        if 'material_strength' in df.columns:
            strength_counts = df['material_strength'].value_counts()
            logger.info(f"  材料強度分布: {strength_counts.to_dict()}")

        return True, df
    except Exception as e:
        logger.error(f"✗ CSV読み込み失敗: {e}")
        return False, None

def test_filtering(executor, df):
    """フィルタリングテスト"""
    logger.info("=" * 60)
    logger.info("STEP 3: フィルタリングテスト")
    logger.info("=" * 60)

    try:
        today = datetime.now()
        filtered_df = executor.apply_filters(df, today)

        logger.success(f"✓ フィルタリング成功")
        logger.info(f"  フィルタ前: {len(df)}件")
        logger.info(f"  フィルタ後: {len(filtered_df)}件")
        logger.info(f"  除外: {len(df) - len(filtered_df)}件")

        if len(filtered_df) > 0:
            logger.info(f"  残存銘柄:")
            for idx, row in filtered_df.iterrows():
                code = row['Code']
                strength = row.get('material_strength', 'N/A')
                logger.info(f"    - {code}: 材料強度={strength}")

        return True, filtered_df
    except Exception as e:
        logger.error(f"✗ フィルタリング失敗: {e}")
        return False, None

def test_price_fetching(client, filtered_df):
    """価格取得テスト"""
    logger.info("=" * 60)
    logger.info("STEP 4: 候補銘柄の価格取得テスト")
    logger.info("=" * 60)

    success_count = 0
    fail_count = 0

    for idx, row in filtered_df.iterrows():
        code = str(row['Code'])
        try:
            symbol_data = client.get_symbol(code, exchange=1)
            current_price = symbol_data['current_price']

            if current_price is not None:
                logger.success(f"✓ {code}: {symbol_data['symbol_name']} = {current_price}円")
                success_count += 1
            else:
                logger.warning(f"⚠ {code}: 価格がNone")
                fail_count += 1
        except Exception as e:
            logger.error(f"✗ {code}: エラー - {e}")
            fail_count += 1

    logger.info(f"価格取得結果: 成功={success_count}件, 失敗={fail_count}件")
    return success_count > 0

def test_order_simulation(executor, filtered_df):
    """注文シミュレーション（実際の注文はしない）"""
    logger.info("=" * 60)
    logger.info("STEP 5: 注文シミュレーション")
    logger.info("=" * 60)

    try:
        # 買付余力取得
        wallet = executor.kabu_client.get_wallet_cash()
        available_cash = wallet['stock_account_wallet']
        logger.info(f"買付余力: {available_cash:,.0f}円")

        # 予算配分計算
        budget_per_stock = executor.budget // len(filtered_df) if len(filtered_df) > 0 else 0
        logger.info(f"1銘柄あたり予算: {budget_per_stock:,.0f}円 (予算{executor.budget:,.0f}円 ÷ {len(filtered_df)}銘柄)")

        # 各銘柄の注文数量計算
        logger.info("=" * 60)
        logger.info("注文シミュレーション:")
        for idx, row in filtered_df.iterrows():
            code = str(row['Code'])
            try:
                symbol_data = executor.kabu_client.get_symbol(code, exchange=1)
                current_price = symbol_data['current_price']
                symbol_name = symbol_data['symbol_name']

                if current_price is None or current_price <= 0:
                    logger.warning(f"  {code} ({symbol_name}): 価格取得失敗 - スキップ")
                    continue

                # 数量計算（100株単位）
                qty = (budget_per_stock // current_price // 100) * 100

                if qty < 100:
                    logger.warning(f"  {code} ({symbol_name}): 予算不足 (価格={current_price}円, 必要={current_price * 100:,.0f}円)")
                    continue

                order_value = current_price * qty
                logger.info(f"  {code} ({symbol_name}):")
                logger.info(f"    価格: {current_price}円")
                logger.info(f"    数量: {qty}株")
                logger.info(f"    金額: {order_value:,.0f}円")
                logger.info(f"    → 成行買注文（シミュレーション）")

            except Exception as e:
                logger.error(f"  {code}: エラー - {e}")

        logger.success("✓ 注文シミュレーション完了（実際の注文は行っていません）")
        return True
    except Exception as e:
        logger.error(f"✗ 注文シミュレーション失敗: {e}")
        return False

def test_discord_notification():
    """Discord通知テスト"""
    logger.info("=" * 60)
    logger.info("STEP 6: Discord通知テスト")
    logger.info("=" * 60)

    try:
        notifier = DiscordNotifier()
        test_message = "✅ 自動売買フルフローテスト完了\n全APIの動作確認OK"
        notifier.send_message(test_message)
        logger.success("✓ Discord通知送信成功")
        return True
    except Exception as e:
        logger.error(f"✗ Discord通知失敗: {e}")
        return False

def main():
    logger.info("=" * 80)
    logger.info("自動売買フルフローテスト（検証モード）")
    logger.info(f"実行時刻: {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}")
    logger.info("=" * 80)

    # STEP 1: API疎通確認
    if not test_api_connectivity():
        logger.error("API疎通確認に失敗しました。テストを中断します。")
        return

    # STEP 2: 候補銘柄CSV読み込み
    success, df = test_candidate_loading()
    if not success or df is None or len(df) == 0:
        logger.error("候補銘柄CSVの読み込みに失敗しました。テストを中断します。")
        return

    # TradeExecutor初期化
    executor = TradeExecutor(
        budget=800000,
        max_daily_loss_rate=0.03,
        max_consecutive_losses=3
    )

    # STEP 3: フィルタリング
    success, filtered_df = test_filtering(executor, df)
    if not success or filtered_df is None or len(filtered_df) == 0:
        logger.warning("フィルタ後の候補銘柄が0件です。")
        return

    # STEP 4: 価格取得
    client = KabuClient()
    if not test_price_fetching(client, filtered_df):
        logger.error("価格取得に失敗しました。")
        return

    # STEP 5: 注文シミュレーション
    if not test_order_simulation(executor, filtered_df):
        logger.error("注文シミュレーションに失敗しました。")
        return

    # STEP 6: Discord通知
    test_discord_notification()

    logger.info("=" * 80)
    logger.success("✅ 全テスト完了")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()
