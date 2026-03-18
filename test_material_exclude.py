"""
材料判定の除外ロジックテスト
"""
from src.utils.material_judge import MaterialJudge

judge = MaterialJudge()

# テストケース1: ネガティブ材料 (下方修正)
test1 = {
    'has_material': False,
    'material_type': None,
    'strength': None,
    'summary': '最終赤字拡大の下方修正',
    'risk': None
}

# テストケース2: ネガティブ材料 (減益)
test2 = {
    'has_material': False,
    'material_type': None,
    'strength': '弱',
    'summary': '通期減益見通し',
    'risk': None
}

# テストケース3: ポジティブ材料
test3 = {
    'has_material': True,
    'material_type': '決算好調',
    'strength': '中',
    'summary': '増収増益達成',
    'risk': None
}

# テストケース4: 材料なし（strength=弱）
test4 = {
    'has_material': False,
    'material_type': 'その他',
    'strength': '弱',
    'summary': 'ニュースなし',
    'risk': None
}

# テストケース5: 材料なし（strength=None、ネガティブキーワードなし）
test5 = {
    'has_material': False,
    'material_type': None,
    'strength': None,
    'summary': '明確な材料情報なし',
    'risk': None
}

print("=" * 60)
print("材料判定除外ロジックテスト")
print("=" * 60)
print()

test_cases = [
    ("ネガティブ材料(下方修正)", test1, True),
    ("ネガティブ材料(減益)", test2, True),
    ("ポジティブ材料", test3, False),
    ("材料なし(strength=弱)", test4, True),
    ("材料なし(ネガティブキーワードなし)", test5, False),
]

all_passed = True

for name, test_data, expected_exclude in test_cases:
    result = judge.should_exclude(test_data)
    status = "[PASS]" if result == expected_exclude else "[FAIL]"
    print(f"{status} {name}")
    print(f"  入力: has_material={test_data['has_material']}, strength={test_data['strength']}, summary={test_data['summary']}")
    print(f"  期待: exclude={expected_exclude}, 実際: exclude={result}")
    print()

    if result != expected_exclude:
        all_passed = False

print("=" * 60)
if all_passed:
    print("[OK] すべてのテストが成功しました")
else:
    print("[NG] 一部のテストが失敗しました")
print("=" * 60)
