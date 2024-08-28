import ccxt
from datetime import datetime, timedelta

class BinanceFetcher:
    def __init__(self, api_key, secret):
        # self.exchange = ccxt.binance({
        #     'apiKey': api_key,
        #     'secret': secret,
        # })
        self.exchange = ccxt.binance()

    def fetch_data(self, symbol, timeframe, since):
        all_ohlcv = []
        now = self.exchange.milliseconds()

        while since < now:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since)
            if len(ohlcv) == 0:
                break
            
            since = ohlcv[-1][0] + 1
            all_ohlcv.extend(ohlcv)

        return [[int(candle[0] / 1000)] + candle[1:] for candle in all_ohlcv[:-1]]

    def get_timestamp_days_ago(self, days):
        return int((datetime.now() - timedelta(days=days)).timestamp() * 1000)