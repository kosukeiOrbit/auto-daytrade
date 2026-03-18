"""
Claude API材料判定（ニュース・開示内容の自動解析）
"""
import json
import os
from anthropic import Anthropic
from loguru import logger


class MaterialJudge:
    """材料判定クラス"""

    def __init__(self):
        """
        初期化

        Note:
            ANTHROPIC_API_KEYを環境変数に設定する必要があります
        """
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            logger.warning("ANTHROPIC_API_KEY が設定されていません。Claude API材料判定は利用できません。")
            self.client = None
        else:
            self.client = Anthropic(api_key=api_key)
            logger.info("Claude API材料判定を初期化しました")

    def judge_material(self, code, name, news_text):
        """
        銘柄の材料を判定

        Args:
            code: 銘柄コード
            name: 銘柄名
            news_text: ニュース・開示内容のテキスト

        Returns:
            dict: {
                'has_material': bool,
                'material_type': str,
                'strength': str,  # '強' | '中' | '弱'
                'summary': str,
                'risk': str | None
            }
        """
        if not self.client:
            logger.warning(f"{code} {name}: Claude APIが利用できません（スキップ）")
            return {
                'has_material': False,
                'material_type': 'その他',
                'strength': '弱',
                'summary': 'API未設定',
                'risk': None
            }

        if not news_text or news_text.strip() == "":
            logger.info(f"{code} {name}: ニュースなし")
            return {
                'has_material': False,
                'material_type': 'その他',
                'strength': '弱',
                'summary': 'ニュースなし',
                'risk': None
            }

        logger.info(f"{code} {name}: Claude APIで材料判定中...")

        prompt = f"""銘柄名: {name}（{code}）
直近ニュース・適時開示:
{news_text}

【重要】以下の判定基準に従って評価してください:
- **必ず銘柄名「{name}」に直接関連する材料のみを判定すること**
- 他の銘柄（TOKYO BASE、ソニー等）の情報が含まれていても無視すること
- 銘柄名が明示されていない一般的な市況・セクター情報は除外すること
- 決算発表から3営業日以上経過している場合、strengthを「弱」寄りに評価すること
- ニュースの日付が古い（1週間以上前）場合は、strengthを「弱」寄りに評価すること
- 減益・業績悪化・ネガティブ材料は has_material=false とすること

以下をJSON形式のみで回答してください（説明不要）:
{{
  "has_material": true/false,
  "material_type": "業績上方修正|決算好調|株式分割|自社株買い|テーマ株|その他",
  "strength": "強|中|弱",
  "summary": "材料の内容を20字以内で",
  "risk": "注意点があれば（翌日決算・高信用倍率等）、なければnull"
}}"""

        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=500,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            # レスポンスからJSONを抽出
            content = response.content[0].text.strip()

            # JSONパース（マークダウンのコードブロックがあれば除去）
            if content.startswith("```json"):
                content = content[7:]  # ```json を削除
            if content.startswith("```"):
                content = content[3:]  # ``` を削除
            if content.endswith("```"):
                content = content[:-3]  # ``` を削除

            result = json.loads(content.strip())

            # 材料検証: 銘柄名との関連性チェック
            if result['has_material'] and result.get('summary'):
                # summaryに銘柄名の一部（最初の2-3文字）が含まれているか確認
                # 例: 「クラシコム」→「クラシ」、「ソニー」→「ソニ」
                name_prefix = name[:2] if len(name) >= 2 else name

                # 銘柄名チェック（部分一致でOK）
                # summaryに銘柄名が含まれていない場合、has_material=Falseに変更
                if name_prefix not in result['summary'] and name not in result['summary']:
                    logger.warning(
                        f"{code} {name}: 材料が銘柄名と一致しない可能性があります "
                        f"(要約: {result['summary']}) → has_material=Falseに変更"
                    )
                    result['has_material'] = False
                    result['strength'] = '弱'

            logger.success(
                f"{code} {name}: "
                f"材料={result['has_material']}, "
                f"種類={result['material_type']}, "
                f"強度={result['strength']}, "
                f"要約={result['summary']}"
            )

            return result

        except Exception as e:
            logger.error(f"{code} {name}: Claude API エラー: {e}")
            # エラー時はデフォルト値を返す
            return {
                'has_material': False,
                'material_type': 'その他',
                'strength': '弱',
                'summary': f'API エラー: {str(e)[:20]}',
                'risk': None
            }

    def should_exclude(self, judgment):
        """
        候補から除外すべきかを判定

        Args:
            judgment: judge_material()の戻り値

        Returns:
            bool: True=除外すべき, False=残す
        """
        # has_material=false の場合、ネガティブ材料かチェック
        if not judgment['has_material']:
            summary = judgment.get('summary', '')
            if summary:
                # ネガティブキーワードをチェック
                negative_keywords = [
                    '下方修正', '赤字', '減益', '悪化', '低迷', '減収',
                    '下振れ', '損失', '減少', '縮小', '撤退', '停止'
                ]
                if any(keyword in summary for keyword in negative_keywords):
                    # ネガティブ材料は除外
                    return True

            # ネガティブでない場合は strength をチェック
            if judgment['strength'] == '弱':
                return True

        return False
