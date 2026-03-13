"""
市場センチメント取得（NYダウ・ナスダック終値）
"""
import requests
import time
from datetime import datetime, timedelta
from loguru import logger


class MarketSentiment:
    """市場センチメント分析クラス"""

    def __init__(self):
        """初期化"""
        self.base_url = "https://query1.finance.yahoo.com/v8/finance/chart"

    def get_us_market_close(self, date=None):
        """
        米国市場終値を取得（NYダウ・ナスダック）

        Args:
            date: 対象日（datetime）。Noneの場合は前営業日

        Returns:
            dict: {
                'dow_close': float,
                'dow_change_pct': float,
                'nasdaq_close': float,
                'nasdaq_change_pct': float,
                'date': str
            }
        """
        if date is None:
            date = datetime.now()

        # 前営業日を取得（米国時間基準）
        target_date = date - timedelta(days=1)

        logger.info(f"米国市場終値取得: {target_date.strftime('%Y-%m-%d')}")

        try:
            # NYダウ（^DJI）を取得
            dow_data = self._fetch_yahoo_finance('^DJI', target_date)

            # ナスダック（^IXIC）を取得
            nasdaq_data = self._fetch_yahoo_finance('^IXIC', target_date)

            result = {
                'dow_close': dow_data['close'],
                'dow_change_pct': dow_data['change_pct'],
                'nasdaq_close': nasdaq_data['close'],
                'nasdaq_change_pct': nasdaq_data['change_pct'],
                'date': target_date.strftime('%Y-%m-%d')
            }

            logger.success(
                f"NYダウ: {result['dow_close']:.2f} ({result['dow_change_pct']:+.2f}%), "
                f"ナスダック: {result['nasdaq_close']:.2f} ({result['nasdaq_change_pct']:+.2f}%)"
            )

            return result

        except Exception as e:
            logger.error(f"米国市場データ取得エラー: {e}")
            return None

    def _fetch_yahoo_finance(self, symbol, date, max_retries=3, retry_delay=1):
        """
        Yahoo FinanceからOHLCVデータを取得（リトライ機能付き）

        Args:
            symbol: シンボル（例: ^DJI, ^IXIC）
            date: 対象日
            max_retries: 最大リトライ回数（デフォルト3回）
            retry_delay: リトライ間隔（秒、デフォルト1秒）

        Returns:
            dict: {'close': float, 'change_pct': float}
        """
        # 期間を設定（対象日の前後1週間）
        start_timestamp = int((date - timedelta(days=7)).timestamp())
        end_timestamp = int((date + timedelta(days=1)).timestamp())

        url = f"{self.base_url}/{symbol}"
        params = {
            'period1': start_timestamp,
            'period2': end_timestamp,
            'interval': '1d'
        }

        last_error = None
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()

                data = response.json()

                # チャートデータを解析
                chart = data['chart']['result'][0]
                timestamps = chart['timestamp']
                quotes = chart['indicators']['quote'][0]

                # 最新の終値を取得
                close_prices = quotes['close']

                # Noneを除外して最新の2つの終値を取得
                valid_closes = [c for c in close_prices if c is not None]

                if len(valid_closes) < 2:
                    raise ValueError(f"十分なデータが取得できませんでした: {symbol}")

                latest_close = valid_closes[-1]
                prev_close = valid_closes[-2]
                change_pct = ((latest_close - prev_close) / prev_close) * 100

                return {
                    'close': latest_close,
                    'change_pct': change_pct
                }

            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    logger.warning(f"{symbol} 取得失敗 (試行{attempt + 1}/{max_retries}): {e} → {retry_delay}秒後にリトライ")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"{symbol} 取得失敗 ({max_retries}回試行): {e}")
                    raise last_error

    def check_market_sentiment(self, dow_change_pct, nasdaq_change_pct, threshold=-2.0):
        """
        地合いをチェックしてスキップ判定

        Args:
            dow_change_pct: NYダウ変化率（%）
            nasdaq_change_pct: ナスダック変化率（%）
            threshold: スキップ閾値（デフォルト-2.0%）

        Returns:
            str: 'skip_all' | 'volume_only' | 'normal'
        """
        if dow_change_pct < threshold and nasdaq_change_pct < threshold:
            logger.warning(f"地合い悪化: NYダウ{dow_change_pct:.2f}%, ナスダック{nasdaq_change_pct:.2f}% → 全スキップ")
            return 'skip_all'
        elif dow_change_pct < threshold or nasdaq_change_pct < threshold:
            logger.warning(f"地合いやや悪化: 出来高急増銘柄のみ対象")
            return 'volume_only'
        else:
            logger.success("地合い良好: 通常通り処理")
            return 'normal'
