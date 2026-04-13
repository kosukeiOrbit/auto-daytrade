"""
Discord Webhook を使った通知クラス
"""
import requests
from datetime import datetime
from loguru import logger
from .config import Config


class DiscordNotifier:
    """Discord Webhook 通知クラス"""

    def __init__(self):
        """初期化"""
        self.webhook_url = Config.DISCORD_WEBHOOK_URL

        if not self.webhook_url or self.webhook_url == "your_discord_webhook_url_here":
            logger.warning("DISCORD_WEBHOOK_URL が設定されていません。通知は送信されません。")
            self.enabled = False
        else:
            self.enabled = True
            logger.info("Discord Webhook 通知を有効化しました")

    def _send_message(self, content):
        """
        Discord Webhook APIにメッセージを送信

        Args:
            content: 送信するメッセージ

        Returns:
            bool: 送信成功時True
        """
        if not self.enabled:
            logger.debug("Discord通知無効のためスキップ")
            return False

        try:
            data = {
                'content': content
            }

            response = requests.post(self.webhook_url, json=data, timeout=10)
            response.raise_for_status()

            logger.success("Discord通知を送信しました")
            return True

        except Exception as e:
            logger.error(f"Discord通知送信エラー: {e}")
            return False

    def send_morning_report(self, candidates_df, judgments, sentiment_message='', tdnet_count=0, budget=None):
        """
        朝のスクリーニング結果を送信

        Args:
            candidates_df: 候補銘柄DataFrame
            judgments: Claude判定結果（dict: {code: judgment}）
            sentiment_message: 地合い判定メッセージ（str）
            tdnet_count: TDnet開示件数（int）
            budget: 本日の投資予算（int、オプション）
        """
        if candidates_df is None or len(candidates_df) == 0:
            content = f"📊 **{datetime.now().strftime('%Y/%m/%d')} 朝スクリーニング結果**\n❌ 候補銘柄なし"
            self._send_message(content)
            return

        # メッセージ構築
        content = f"📊 **{datetime.now().strftime('%Y/%m/%d')} 朝スクリーニング結果**\n"

        # 投資予算
        if budget:
            content += f"💰 本日の投資予算: {budget:,}円\n"

        # 候補数
        material_count = sum(1 for j in judgments.values() if j and j.get('has_material', False))
        content += f"✅ 候補: {material_count}銘柄（材料あり）\n"

        # 地合い・TDnet情報
        content += f"地合い: {sentiment_message} / TDnet開示: {tdnet_count}件\n\n"

        # パターンA選定順でソート: 材料あり・TOB除外・強>中>弱・同強度内TradingValue降順
        tob_keywords = ['TOB', 'MBO', '公開買付', '株式交換', '完全子会社化', '非公開化',
                    '買収防衛', 'スクイーズアウト', '株式併合', '上場廃止']
        strength_order = {'強': 0, '中': 1, '弱': 2}

        sortable = []
        for idx, row in candidates_df.iterrows():
            code = str(row['Code'])
            judgment = judgments.get(code)
            if not judgment or not judgment.get('has_material', False):
                continue
            summary = str(judgment.get('summary', ''))
            material_type = str(judgment.get('material_type', ''))
            if any(kw in summary or kw in material_type for kw in tob_keywords):
                continue
            strength = judgment.get('strength', '弱')
            trading_value = row.get('TradingValue', 0) or 0
            sortable.append((
                strength_order.get(strength, 9),
                -trading_value,
                row,
                judgment
            ))

        sortable.sort(key=lambda x: (x[0], x[1]))

        # 上位5銘柄を表示
        for rank, (_, _, row, judgment) in enumerate(sortable[:5], start=1):
            code = str(row['Code'])
            surge_ratio = row.get('VolumeSurgeRatio', 0)
            close_price = row.get('C', 0)
            trading_value = row.get('TradingValue', 0) or 0
            tv_oku = trading_value / 1e8
            company_name = judgment.get('company_name', '')

            label = "  ← パターンA選定" if rank == 1 else ""
            content += f"**{rank}位 {code} {company_name}** ×{surge_ratio:.1f}倍 | {close_price:,.0f}円 | 売買代金{tv_oku:.1f}億{label}\n"

            material_type = judgment.get('material_type', '-')
            strength = judgment.get('strength', '-')
            summary = judgment.get('summary', '-')
            content += f"　[{material_type}・{strength}] {summary}\n\n"

        self._send_message(content)

    def send_entry_signal(self, code, name, price, reason):
        """
        エントリーシグナル発生時の通知（フェーズ5用）

        Args:
            code: 銘柄コード
            name: 銘柄名
            price: エントリー価格
            reason: エントリー理由
        """
        content = (
            f"🔔 **エントリーシグナル**\n"
            f"{code} {name} | {price:.0f}円\n"
            f"理由: {reason}"
        )
        self._send_message(content)

    def send_exit(self, code, name, price, pnl, pnl_pct, reason):
        """
        決済シグナル発生時の通知（フェーズ5用）

        Args:
            code: 銘柄コード
            name: 銘柄名
            price: 決済価格
            pnl: 損益（円）
            pnl_pct: 損益率（%）
            reason: 決済理由
        """
        # 利確 or 損切り判定
        if pnl >= 0:
            emoji = "💰"
            title = "利確完了"
        else:
            emoji = "✂️"
            title = "損切り"

        pnl_sign = "+" if pnl >= 0 else ""

        content = (
            f"{emoji} **{title}**\n"
            f"{code} {name} | {price:.0f}円\n"
            f"損益: {pnl_sign}{pnl:.0f}円 ({pnl_sign}{pnl_pct:.2f}%)\n"
            f"理由: {reason}"
        )
        self._send_message(content)

    def send_error(self, message):
        """
        エラー発生時の通知

        Args:
            message: エラーメッセージ
        """
        content = f"🚨 **エラー**: {message}"
        self._send_message(content)

    def send_trade_notification(self, action, symbol, price, qty, stop_price=None, target_price=None):
        """
        取引通知を送信

        Args:
            action: アクション（エントリー、決済など）
            symbol: 銘柄コード
            price: 価格
            qty: 数量
            stop_price: 損切り価格（オプション）
            target_price: 利確価格（オプション）
        """
        content = f"📈 **{action}**\n{symbol} | {qty}株 @ {price:.0f}円\n"

        if stop_price:
            content += f"損切り: {stop_price:.0f}円\n"
        if target_price:
            content += f"利確: {target_price:.0f}円\n"

        self._send_message(content)

    def send_daily_report(self, report):
        """
        日次トレードレポートをDiscordに送信

        Args:
            report: generate_daily_report()の戻り値（dict）
        """
        date_str = report.get('date', '')
        formatted_date = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}" if len(date_str) == 8 else date_str

        content = f"📊 **本日のトレードレポート（{formatted_date}）**\n"
        content += "━━━━━━━━━━━━━━━━━━\n"

        # 信用余力
        opening = report.get('opening_wallet')
        closing = report.get('closing_wallet')
        if opening is not None and closing is not None:
            diff = report.get('wallet_diff', 0) or 0
            diff_sign = "+" if diff >= 0 else ""
            content += f"💳 信用余力：{opening:,.0f}円 → {closing:,.0f}円（{diff_sign}{diff:,.0f}円）\n"

        # 現金余力
        opening_cash = report.get('opening_cash')
        closing_cash = report.get('closing_cash')
        if opening_cash is not None and closing_cash is not None:
            cash_diff = closing_cash - opening_cash
            cash_sign = "+" if cash_diff >= 0 else ""
            content += f"💰 現金余力：{opening_cash:,.0f}円 → {closing_cash:,.0f}円（{cash_sign}{cash_diff:,.0f}円）\n"
        elif closing_cash is not None:
            content += f"💰 現金余力：{closing_cash:,.0f}円\n"

        trade_count = report.get('trade_count', 0)

        if trade_count > 0:
            total_pnl = report.get('total_pnl', 0)
            pnl_emoji = "📈" if total_pnl >= 0 else "📉"
            pnl_sign = "+" if total_pnl >= 0 else ""
            win = report.get('win_count', 0)
            lose = report.get('lose_count', 0)
            win_rate = report.get('win_rate', 0)

            content += f"{pnl_emoji} 本日損益：{pnl_sign}{total_pnl:,.0f}円\n"
            content += f"🏆 勝率：{win}勝{lose}敗（{win_rate:.1f}%）\n"
            content += f"🔢 トレード数：{trade_count}件\n"
        else:
            content += "本日はトレードなし\n"

        tp = report.get('take_profit_pct', 2.0)
        sl = report.get('stop_loss_pct', 1.0)
        content += f"⚙️ パラメータ：利確+{tp}% / 損切-{sl}%\n"

        # トレード詳細
        if trade_count > 0:
            content += "\n**【トレード詳細】**\n"
            for t in report.get('trades', []):
                code = t.get('code', '')
                name = t.get('symbol_name', '')
                pattern = t.get('entry_pattern', 'A')
                strength = t.get('material_strength', '')
                m_type = t.get('material_type', '')
                entry_time = t.get('entry_time', '')
                exit_time = t.get('exit_time', '')
                hold = t.get('hold_minutes', 0)
                pnl = t.get('profit_loss', 0)
                pct = t.get('profit_pct', 0)
                reason = t.get('exit_reason', '')
                surge = t.get('volume_surge', 0)
                vwap_ratio = t.get('entry_vwap_ratio', '')
                mfe = t.get('mfe_pct', 0)
                mae = t.get('mae_pct', 0)

                pnl_sign = "+" if pnl >= 0 else ""
                pct_sign = "+" if pct >= 0 else ""

                content += f"\n**{code} {name}**｜パターン:{pattern}"
                if strength:
                    material_info = f"材料:{strength}"
                    if m_type:
                        material_info += f"（{m_type}）"
                    content += f"｜{material_info}"
                content += f"\n{entry_time}→{exit_time}（{hold}分）\n"
                content += f"{pnl_sign}{pnl:,.0f}円（{pct_sign}{pct:.1f}%）｜{reason}\n"
                content += f"出来高:{surge:.1f}倍"
                if vwap_ratio != '':
                    content += f"｜VWAP比:{vwap_ratio}%"
                content += f"｜MFE:{mfe:+.1f}%｜MAE:{mae:+.1f}%\n"

        content += "━━━━━━━━━━━━━━━━━━"

        self._send_message(content)

    def send_message(self, message):
        """
        汎用メッセージ送信

        Args:
            message: 送信するメッセージ
        """
        self._send_message(message)
