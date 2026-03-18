"""
フィルタロジックのテスト
"""
import pandas as pd
from datetime import datetime
from loguru import logger

# テスト用のサンプルデータ
test_data = [
    # material_strength='強', 終値=1818円 → 残る
    {'Code': '4075', 'C': 1818.0, 'material_strength': '強', 'material_summary': 'AWS共同プロジェクト'},
    # material_strength=空欄 → 除外
    {'Code': '7110', 'C': 2012.0, 'material_strength': '', 'material_summary': ''},
    # material_strength='弱' → 除外
    {'Code': '1234', 'C': 500.0, 'material_strength': '弱', 'material_summary': '材料弱'},
    # material_strength='中', 終値=80円 → 除外（100円未満）
    {'Code': '5678', 'C': 80.0, 'material_strength': '中', 'material_summary': '材料中'},
    # material_strength='中', 終値=150円 → 残る
    {'Code': '9999', 'C': 150.0, 'material_strength': '中', 'material_summary': '材料中'},
]

def test_filters():
    """フィルタのテスト"""
    logger.info("=" * 60)
    logger.info("フィルタロジックテスト")
    logger.info("=" * 60)

    df = pd.DataFrame(test_data)
    logger.info(f"テストデータ: {len(df)}件")
    print(df[['Code', 'C', 'material_strength', 'material_summary']])

    initial_count = len(df)

    # フィルタ1: material_strength フィルタ（'強'または'中'のみ）
    logger.info("\n[フィルタ1] material_strength ('強'または'中'のみ)")
    if 'material_strength' in df.columns:
        before_count = len(df)
        df = df[
            (df['material_strength'] == '強') |
            (df['material_strength'] == '中')
        ]
        filtered_count = before_count - len(df)
        logger.info(f"除外: {filtered_count}件 (残り{len(df)}件)")
        print(df[['Code', 'C', 'material_strength', 'material_summary']])

    # フィルタ2: 超低位株除外（100円未満）
    logger.info("\n[フィルタ2] 100円未満除外")
    if 'C' in df.columns:
        before_count = len(df)
        df = df[df['C'] >= 100]
        filtered_count = before_count - len(df)
        logger.info(f"除外: {filtered_count}件 (残り{len(df)}件)")
        print(df[['Code', 'C', 'material_strength', 'material_summary']])

    logger.info("\n" + "=" * 60)
    logger.info(f"フィルタ適用結果: {len(df)}件 (除外: {initial_count - len(df)}件)")
    logger.info("=" * 60)

    # 期待結果: 4075 (強, 1818円) と 9999 (中, 150円) の2件が残る
    expected_codes = ['4075', '9999']
    actual_codes = df['Code'].tolist()

    if set(actual_codes) == set(expected_codes):
        logger.success("[OK] フィルタテスト成功")
        return True
    else:
        logger.error(f"[NG] フィルタテスト失敗: 期待={expected_codes}, 実際={actual_codes}")
        return False


if __name__ == "__main__":
    success = test_filters()
    if success:
        print("\n✓ すべてのテストが成功しました")
    else:
        print("\n✗ テストが失敗しました")
