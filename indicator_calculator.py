import pandas as pd
import numpy as np
from ta.trend import MACD
from ta.momentum import RSIIndicator, StochRSIIndicator

class IndicatorCalculator:
    def __init__(self, price_data, config):
        self.df = pd.DataFrame(price_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        self.df.set_index('timestamp', inplace=True)
        self.df.sort_index(inplace=True)
        self.config = config['indicators']

    def calculate_indicators(self):
        indicators = {}

        if self.config['rsi']['enabled']:
            indicators.update(self.calculate_rsi())
        
        if self.config['macd']['enabled']:
            indicators.update(self.calculate_macd())
        
        if self.config['stoch_rsi']['enabled']:
            indicators.update(self.calculate_stoch_rsi())
        
        if self.config['ema']['enabled']:
            indicators.update(self.calculate_ema())

        indicators.update(self.calculate_buy_sell_volume())

        self.df = pd.concat([self.df, pd.DataFrame(indicators, index=self.df.index)], axis=1)

        result = self.df[list(indicators.keys())].reset_index()
        return result.values.tolist()

    def calculate_rsi(self):
        period = self.config['rsi']['period']
        rsi = RSIIndicator(close=self.df['close'], window=period)
        return {f'rsi_{period}': rsi.rsi()}

    def calculate_macd(self):
        fast = self.config['macd']['fast_period']
        slow = self.config['macd']['slow_period']
        signal = self.config['macd']['signal_period']
        macd = MACD(
            close=self.df['close'], 
            window_fast=fast,
            window_slow=slow,
            window_sign=signal
        )
        return {
            f'macd_{fast}_{slow}': macd.macd(),
            f'macd_signal_{signal}': macd.macd_signal(),
            f'macd_histogram_{fast}_{slow}_{signal}': macd.macd_diff()
        }

    def calculate_stoch_rsi(self):
        period = self.config['stoch_rsi']['period']
        smooth_k = self.config['stoch_rsi']['smooth_k']
        smooth_d = self.config['stoch_rsi']['smooth_d']
        stoch_rsi = StochRSIIndicator(
            close=self.df['close'],
            window=period,
            smooth1=smooth_k,
            smooth2=smooth_d
        )
        return {
            f'stoch_rsi_k_{period}_{smooth_k}': stoch_rsi.stochrsi_k(),
            f'stoch_rsi_d_{period}_{smooth_d}': stoch_rsi.stochrsi_d()
        }

    def calculate_ema(self):
        short_period = self.config['ema']['short_period']
        long_period = self.config['ema']['long_period']
        return {
            f'ema_{short_period}': self.df['close'].ewm(span=short_period, adjust=False).mean(),
            f'ema_{long_period}': self.df['close'].ewm(span=long_period, adjust=False).mean()
        }

    def calculate_buy_sell_volume(self):
        total_move = self.df['high'] - self.df['low']
        buy_pressure = self.df['close'] - self.df['low']
        buy_ratio = buy_pressure / total_move
        buy_volume = self.df['volume'] * buy_ratio
        sell_volume = self.df['volume'] * (1 - buy_ratio)

        # Handle cases where high and low are the same (no price movement)
        no_move_mask = total_move == 0
        buy_volume[no_move_mask] = self.df.loc[no_move_mask, 'volume'] / 2
        sell_volume[no_move_mask] = self.df.loc[no_move_mask, 'volume'] / 2

        return {
            'buy_volume': buy_volume,
            'sell_volume': sell_volume
        }