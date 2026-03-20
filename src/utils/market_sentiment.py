"""
市場センチメント取得（日経先物・NYダウ・ナスダック）
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

    def get_nikkei_futures(self):
        """
        日経先物の直近変化率を取得

        Returns:
            dict: {'close': float, 'change_pct': float} or None
        """
        logger.info("日経先物データ取得中 (yfinance)...")

        # 複数シンボルを試行（yfinanceの日経先物シンボルは不安定なため）
        symbols = ['NKD=F', '^N225']
        for symbol in symbols:
            try:
                data = self._fetch_yfinance(symbol, max_retries=2, retry_delay=2)
                logger.success(f"日経先物取得成功 ({symbol}): {data['close']:.0f} ({data['change_pct']:+.2f}%)")
                return data
            except Exception as e:
                logger.debug(f"{symbol} 取得失敗: {e}")
                continue

        logger.warning("日経先物データ取得失敗（全シンボル）")
        return None

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

        target_date = date - timedelta(days=1)

        logger.info(f"米国市場終値取得 (yfinance): {target_date.strftime('%Y-%m-%d')}")

        try:
            dow_data = self._fetch_yfinance('^DJI')
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
            symbol: シンボル（例: ^DJI, ^IXIC, NKD=F）
            max_retries: 最大リトライ回数（デフォルト3回）
            retry_delay: リトライ間隔（秒、デフォルト2秒）

        Returns:
            dict: {'close': float, 'change_pct': float}
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="5d")

                if hist is None or len(hist) < 2:
                    raise ValueError(f"十分なデータが取得できませんでした: {symbol}")

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

    def check_market_sentiment(self, nikkei_change_pct=None, dow_change_pct=None, nasdaq_change_pct=None, threshold=-1.5):
        """
        地合いをチェックしてスキップ判定

        Args:
            nikkei_change_pct: 日経先物変化率（%）。Noneの場合はフォールバック判定
            dow_change_pct: NYダウ変化率（%）
            nasdaq_change_pct: ナスダック変化率（%）
            threshold: スキップ閾値（デフォルト-1.5%）

        Returns:
            dict: {
                'status': 'skip_all' | 'normal',
                'message': str  # Discord通知用テキスト
            }
        """
        # メインチェック: 日経先物
        if nikkei_change_pct is not None:
            if nikkei_change_pct < threshold:
                msg = f"⚠️スキップ / 日経先物 {nikkei_change_pct:+.1f}%（閾値{threshold}%超え）"
                logger.warning(f"地合い悪化: {msg}")
                return {'status': 'skip_all', 'message': msg}
            else:
                msg = f"正常 / 日経先物 {nikkei_change_pct:+.1f}%"
                logger.success(f"地合い良好: {msg}")
                return {'status': 'normal', 'message': msg}

        # フォールバック: NYダウ・ナスダック
        if dow_change_pct is not None and nasdaq_change_pct is not None:
            if dow_change_pct < threshold and nasdaq_change_pct < threshold:
                msg = f"⚠️スキップ / NYダウ {dow_change_pct:+.1f}% / ナスダック {nasdaq_change_pct:+.1f}%（日経先物取得失敗）"
                logger.warning(f"地合い悪化: {msg}")
                return {'status': 'skip_all', 'message': msg}
            else:
                msg = f"正常 / NYダウ {dow_change_pct:+.1f}% / ナスダック {nasdaq_change_pct:+.1f}%（日経先物取得失敗）"
                logger.success(f"地合い: {msg}")
                return {'status': 'normal', 'message': msg}

        # すべて取得失敗
        logger.error("地合いデータ全取得失敗 → 通常通り処理")
        return {'status': 'normal', 'message': 'データ取得失敗（通常通り処理）'}
