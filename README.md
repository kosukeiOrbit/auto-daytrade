# auto-daytrade

日本株デイトレード自動化システム（三菱UFJ eスマート証券 kabuステーションAPI）

## 概要

- **パターンA**: 前日スクリーニング銘柄への寄り付き成行エントリー（利確+2% / 損切-1%）
- **パターンB**: 場中の出来高急増銘柄への指値+1%エントリー（利確+1.5% / 損切-1%）
- 本番稼働中（2026年3月19日〜）
- 信用デイトレード（手数料・金利・貸株料無料）

## 実行スケジュール

| 時刻 | スクリプト | 内容 |
|------|-----------|------|
| 7:00 | morning_screening.py | スクリーニング・材料判定・候補CSV生成・Discord通知 |
| 8:45 | auto_trading.py | 起動・パターンAエントリー・寄り付き待機 |
| 9:00 | （市場） | 寄り付きで約定 → 約定確認 → 損切り/利確設定 |
| 9:30〜15:25 | auto_trading.py | パターンBスキャン・エントリー（1分ごと） |
| 常時 | 監視スレッド | ポジション監視（3秒ごと・利確/損切り判定） |
| 11:25〜11:35 | auto_trading.py | 含み損ポジション強制決済 |
| 15:15〜15:25 | auto_trading.py | 全ポジション強制決済 |
| 15:30 | auto_trading.py | 日次レポート生成・Discord通知・終了 |

## セットアップ

### 1. 仮想環境の作成
```bash
python -m venv venv
venv\Scripts\activate  # Windows
```

### 2. 依存パッケージのインストール
```bash
pip install -r requirements.txt
```

### 3. 環境変数の設定
`.env`ファイルを作成：

```bash
JQUANTS_API_KEY=your_key
ANTHROPIC_API_KEY=your_key
DISCORD_WEBHOOK_URL=your_url
KABU_API_URL=http://localhost:18080/kabusapi
KABU_API_PASSWORD=your_password
INVESTMENT_RATIO=0.80
```

### 4. タスクスケジューラ登録（Windows）
- 7:00: `python morning_screening.py`
- 8:45: `python auto_trading.py`

## プロジェクト構造

```
auto-daytrade/
├── morning_screening.py              # 朝7:00スクリーニング
├── auto_trading.py                   # 8:45自動売買（パターンA+B）
├── src/
│   ├── screening/
│   │   └── screener.py               # 出来高急増・各種フィルタ
│   ├── trading/
│   │   └── trade_executor.py         # 発注・ポジション管理・決済・安全装置
│   ├── backtest/
│   │   ├── minute_backtest.py        # パターンA分足バックテスト
│   │   └── pattern_b_backtest.py     # パターンB分足バックテスト
│   └── utils/
│       ├── config.py                 # 環境変数管理
│       ├── jquants_client.py         # J-Quants APIクライアント
│       ├── kabu_client.py            # kabuステーションAPIクライアント
│       ├── cache_manager.py          # APIキャッシュ（週次/月次/日次）
│       ├── market_sentiment.py       # 地合いチェック（kabuStation先物API+yfinance）
│       ├── material_judge.py         # Claude API材料判定
│       ├── tdnet_scraper.py          # TDnet適時開示スクレイピング
│       ├── news_scraper.py           # 株探ニューススクレイピング
│       └── notifier.py              # Discord通知
├── scripts/
│   ├── generate_historical_candidates.py  # 過去候補CSV一括生成
│   ├── precompute_minute_cache.py    # 分足キャッシュ事前生成
│   └── compare_ranking.py           # ランキング比較スクリプト
├── run_minute_backtest.py            # パターンA分足バックテスト実行
├── run_pattern_b_backtest.py         # パターンB分足バックテスト実行
├── run_param_sweep.py                # 利確/損切パラメータスイープ
├── test_pattern_b.py                 # パターンB単体検証（ドライラン）
├── data/
│   ├── candidates_YYYYMMDD.csv       # 日次スクリーニング結果
│   ├── trade_history.csv             # 本番トレード履歴
│   ├── daily_report_YYYYMMDD.json    # 日次レポート
│   └── cache/                        # APIキャッシュ・分足キャッシュ
├── equities_bars_minute/             # J-Quants分足データ（バックテスト用）
├── backtest_results/                 # バックテスト結果CSV・グラフ
├── logs/                             # 実行ログ
├── daytrade_automation.md            # 設計書（詳細）
└── requirements.txt
```

## 使用ツール

| カテゴリ | ツール | 料金 |
|----------|--------|------|
| 実行環境 | Python 3.13 | 無料 |
| 発注API | kabuステーションAPI | 無料 |
| 株価データ | J-Quants API Lightプラン | 月1,650円 |
| 材料判定 | Anthropic Claude API | 月約300円 |
| 通知 | Discord Webhook | 無料 |

## 開発ステータス

- **フェーズ1: 環境構築** ✅ 完了
- **フェーズ2: スクリーニング自動化** ✅ 完了
- **フェーズ3: APIキャッシュ最適化** ✅ 完了
- **フェーズ4: バックテスト検証** ✅ 完了（分足バックテスト・パラメータスイープ）
- **フェーズ5: 自動発注** ✅ 完了（パターンA+B・信用デイトレード）
- **フェーズ6: 本番稼働** ✅ 稼働中（2026年3月19日〜）

現在は本番稼働・改善フェーズ。

詳細は[daytrade_automation.md](daytrade_automation.md)を参照。

---

*免責事項: 本システムは技術的な学習・研究目的のプロジェクトです。株式投資には元本割れのリスクがあります。投資判断は自己責任で行ってください。*
