# デイトレ自動化システム

日本株デイトレードの段階的自動化システム

## 📋 目次

- [セットアップ](#セットアップ)
- [使い方](#使い方)
- [プロジェクト構造](#プロジェクト構造)
- [開発ステータス](#開発ステータス)

## セットアップ

### 1. 仮想環境の作成
```bash
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # macOS/Linux
```

### 2. 依存パッケージのインストール
```bash
pip install -r requirements.txt
```

### 3. 環境変数の設定
`.env`ファイルを作成し、J-Quants APIキーを設定：

```bash
# .env
JQUANTS_API_KEY=your_api_key_here
```

> **注意**: J-Quants APIキーは[J-Quants公式サイト](https://application.jpx-jquants.com/)から取得してください（Lightプラン: 月1,650円）

## 使い方

### 1. スクリーニング実行

指定した条件で候補銘柄を抽出：

```bash
python test_screening.py
```

**フィルタ条件**:
- 前日比上昇率: 3%以上
- 売買代金: 上位20銘柄
- 1単元価格: 50万円以内

**出力例**:
```
スクリーニング結果: 16銘柄
平均上昇率: 8.85%
最大上昇率: 23.54% (銘柄コード: 1234)
```

### 2. エントリー判定テスト

VWAP・移動平均・トレンドなどの技術指標を使った判定ロジックをテスト：

```bash
python test_analysis.py
```

**判定条件**:
- ギャップアップ: +8%以内
- 現在上昇率: +8%以内
- VWAP: 現在価格がVWAPを上回る
- トレンド: 直近5本の足が上昇トレンド
- VWAPタッチ: 直近5本でVWAP付近にタッチ

### 3. バックテスト実行

過去データで戦略の精度を検証：

```bash
python test_integrated_backtest.py
```

**バックテスト内容**:
- 過去10日間（営業日のみ）の実データを使用
- スクリーニング → エントリー判定 → 損益シミュレーション
- パフォーマンス指標の計算（勝率・R倍数・最大DD等）

**出力例**:
```
総トレード数: 9
勝率: 0.00%
総損益: +0円
最大ドローダウン: 0円 (0.00%)
```

> **制約**: J-Quants API無料プランでは日足データのみ取得可能。正確な損益シミュレーションには5分足データが必要（kabuステーション連携後に対応予定）

### 4. バックテスト結果の可視化

グラフとチャートで結果を分析：

```bash
python test_visualization.py
```

**生成されるグラフ**:
- `backtest_results/equity_curve_*.png` - 資金曲線
- `backtest_results/trade_timeline_*.png` - トレード履歴タイムライン
- `backtest_results/performance_summary_*.png` - パフォーマンスサマリー

### 5. プログラムから利用

```python
from src.screening import Screener
from src.analysis import check_entry
from src.backtest import IntegratedBacktest, BacktestVisualizer
from datetime import datetime, timedelta
from dateutil import tz

# 1. スクリーニング
screener = Screener(budget=500_000)
candidates = screener.get_candidates(
    min_price_change_rate=3.0,
    top_n_by_value=20
)
print(f"候補銘柄: {len(candidates)}銘柄")

# 2. エントリー判定（例）
ohlcv = [...]  # 5分足データ
result = check_entry(
    symbol="1234",
    ohlcv=ohlcv,
    current_price=1500,
    prev_close=1400
)
if result["entry"]:
    print(f"エントリー可: {result['entry_price']}円")
    print(f"利確: {result['take_profit']}円")
    print(f"損切: {result['stop_loss']}円")

# 3. バックテスト実行
jst = tz.gettz("Asia/Tokyo")
backtest = IntegratedBacktest(initial_capital=500_000)
metrics = backtest.run_historical_backtest(
    start_date=datetime.now(jst) - timedelta(days=10),
    end_date=datetime.now(jst),
    budget=500_000,
    min_change_rate=3.0,
    top_n=20
)
metrics.display()

# 4. 可視化
visualizer = BacktestVisualizer(
    trades=backtest.simulator.trades,
    metrics=metrics
)
visualizer.plot_all(output_dir="backtest_results")
```

## プロジェクト構造

```
auto-daytrade/
├── src/
│   ├── screening/           # スクリーニング機能
│   │   └── screener.py      # 銘柄抽出ロジック
│   ├── analysis/            # チャート分析・判定ロジック
│   │   ├── indicators.py    # VWAP・移動平均・トレンド計算
│   │   └── entry_judge.py   # エントリー判定ロジック
│   ├── backtest/            # バックテストフレームワーク
│   │   ├── simulator.py     # トレードシミュレーター
│   │   ├── metrics.py       # パフォーマンス指標計算
│   │   ├── engine.py        # バックテストエンジン
│   │   ├── integrated_backtest.py  # 統合バックテスト
│   │   └── visualizer.py    # 結果可視化
│   └── utils/               # 共通ユーティリティ
│       └── jquants_client.py  # J-Quants API クライアント
├── backtest_results/        # バックテスト結果（グラフ）
├── test_*.py                # 各機能のテストスクリプト
├── daytrade_automation.md   # 設計書（詳細）
└── README.md                # このファイル
```

## 開発ステータス

**フェーズ1: 環境構築** ✅ 完了
- Python環境・J-Quants API連携・GitHubリポジトリ作成

**フェーズ2: スクリーニング自動化** ✅ 完了
- 全銘柄データ取得・条件フィルタ・候補リスト生成

**フェーズ3: チャート判定** ✅ 完了
- VWAP・移動平均計算・エントリー判定ロジック実装

**フェーズ4: バックテスト検証** ✅ 完了
- トレードシミュレーター・パフォーマンス指標計算・可視化機能

**フェーズ5: 自動発注** 🔜 未実装
- kabuステーション連携（証券口座開設承認待ち）

詳細は[daytrade_automation.md](daytrade_automation.md)を参照。

---

**免責事項**: 本システムは技術的な学習・研究目的のプロジェクトです。株式投資には元本割れのリスクがあります。投資判断は自己責任で行ってください。
