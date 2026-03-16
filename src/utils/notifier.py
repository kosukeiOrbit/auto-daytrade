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

    def send_morning_report(self, candidates_df, judgments, sentiment=None, tdnet_count=0):
        """
        朝のスクリーニング結果を送信

        Args:
            candidates_df: 候補銘柄DataFrame
            judgments: Claude判定結果（dict: {code: judgment}）
            sentiment: 地合い情報（dict or None）
            tdnet_count: TDnet開示件数（int）
        """
        if candidates_df is None or len(candidates_df) == 0:
            content = f"📊 **{datetime.now().strftime('%Y/%m/%d')} 朝スクリーニング結果**\n❌ 候補銘柄なし"
            self._send_message(content)
            return

        # メッセージ構築
        content = f"📊 **{datetime.now().strftime('%Y/%m/%d')} 朝スクリーニング結果**\n"

        # 候補数
        material_count = sum(1 for j in judgments.values() if j and j.get('has_material', False))
        content += f"✅ 候補: {material_count}銘柄（材料あり）\n"

        # 地合い・TDnet情報
        sentiment_status = "正常"
        if sentiment:
            dow_change = sentiment.get('dow_change_pct', 0)
            nasdaq_change = sentiment.get('nasdaq_change_pct', 0)
            if dow_change < -2.0 or nasdaq_change < -2.0:
                sentiment_status = f"やや悪化 (ダウ{dow_change:+.1f}% / ナス{nasdaq_change:+.1f}%)"
        else:
            sentiment_status = "データ取得失敗"

        content += f"地合い: {sentiment_status} / TDnet開示: {tdnet_count}件\n\n"

        # 上位5銘柄（材料ありのみ）
        rank = 0
        for idx, row in candidates_df.head(10).iterrows():
            code = str(row['Code'])

            # judgmentsに存在し、材料ありの場合のみ表示
            if code not in judgments:
                continue

            judgment = judgments[code]
            if not judgment or not judgment.get('has_material', False):
                continue

            rank += 1
            if rank > 5:
                break

            # 銘柄情報
            surge_ratio = row.get('VolumeSurgeRatio', 0)
            close_price = row.get('C', 0)
            company_name = judgment.get('company_name', '')
            small_cap_flag = row.get('SmallCapFlag', False)

            # 小型株フラグを表示
            small_cap_warning = " ⚠️小型注意" if small_cap_flag else ""
            content += f"**{rank}位 {code} {company_name}** ×{surge_ratio:.1f}倍 | {close_price:.0f}円{small_cap_warning}\n"

            # 材料判定結果
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
