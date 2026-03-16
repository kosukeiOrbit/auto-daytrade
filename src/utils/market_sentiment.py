"""
市場センチメント取得（NYダウ・ナスダック終値）
yfinance ライブラリ使用
"""
import yfinance as yf
import time
from datetime import datetime, timedelta
from loguru import logger


class MarketSentiment:
    """市場センチメント分析クラス"""

    def __init__(self):
        """初期化"""
        pass

    def get_us_market_close(self, date=None):
        """
        米国市場終値を取得（NYダウ・ナスダック）
        yfinance ライブラリを使用

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

        logger.info(f"米国市場終値取得 (yfinance): {target_date.strftime('%Y-%m-%d')}")

        try:
            # NYダウ（^DJI）を取得
            dow_data = self._fetch_yfinance('^DJI')

            # ナスダック（^IXIC）を取得
            nasdaq_data = self._fetch_yfinance('^IXIC')

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

    def _fetch_yfinance(self, symbol, max_retries=3, retry_delay=2):
        """
        yfinanceからOHLCVデータを取得（リトライ機能付き）

        Args:
            symbol: シンボル（例: ^DJI, ^IXIC）
            max_retries: 最大リトライ回数（デフォルト3回）
            retry_delay: リトライ間隔（秒、デフォルト2秒）

        Returns:
            dict: {'close': float, 'change_pct': float}
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                # yfinanceでティッカーを取得
                ticker = yf.Ticker(symbol)

                # 直近5営業日のデータを取得（余裕を持って）
                hist = ticker.history(period="5d")

                if hist is None or len(hist) < 2:
                    raise ValueError(f"十分なデータが取得できませんでした: {symbol}")

                # 最新2営業日の終値を取得
                close_prices = hist['Close'].dropna()

                if len(close_prices) < 2:
                    raise ValueError(f"十分な終値データが取得できませんでした: {symbol}")

                latest_close = close_prices.iloc[-1]
                prev_close = close_prices.iloc[-2]

                change_pct = ((latest_close - prev_close) / prev_close) * 100

                latest_date = close_prices.index[-1].strftime('%Y-%m-%d')
                prev_date = close_prices.index[-2].strftime('%Y-%m-%d')

                logger.debug(f"{symbol}: {latest_date}={latest_close:.2f}, {prev_date}={prev_close:.2f}, 変化率={change_pct:+.2f}%")

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
