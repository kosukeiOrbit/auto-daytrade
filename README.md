# デイトレ自動化システム

日本株デイトレードの段階的自動化システム

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
`.env.example`をコピーして`.env`を作成し、APIキーを設定してください。

```bash
copy .env.example .env
```

`.env`ファイルを編集してJ-Quants APIキーを設定：
```
JQUANTS_REFRESH_TOKEN=your_refresh_token_here
```

## プロジェクト構造

```
auto-daytrade/
├── src/
│   ├── screening/      # スクリーニング機能
│   ├── analysis/       # チャート分析・判定ロジック
│   ├── trading/        # 発注・ポジション管理
│   └── utils/          # 共通ユーティリティ
├── tests/              # テストコード
├── data/               # データ保存用（gitignore対象）
└── logs/               # ログファイル（gitignore対象）
```

## 開発ステータス

詳細は[daytrade_automation.md](daytrade_automation.md)を参照。

現在：フェーズ1（環境構築中）
