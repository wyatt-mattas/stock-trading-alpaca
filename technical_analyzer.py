# technical_analyzer.py
"""Technical analysis calculations"""
import talib._ta_lib as ta
import pandas as pd
from config import Config

class TechnicalAnalyzer:
    @staticmethod
    def initialize_indicators(dataframe):
        close = dataframe['close'].values
        high = dataframe['high'].values
        low = dataframe['low'].values
        
        dataframe['ema5'] = ta.EMA(close, timeperiod=5)
        dataframe['ema10'] = ta.EMA(close, timeperiod=10)
        dataframe['ema20'] = ta.EMA(close, timeperiod=20)
        dataframe['rsi'] = ta.RSI(close, timeperiod=Config.RSI_PERIOD)
        macd, signal, _ = ta.MACD(close, 
                                 fastperiod=Config.MACD_FAST,
                                 slowperiod=Config.MACD_SLOW,
                                 signalperiod=Config.MACD_SIGNAL)
        dataframe['macd'] = macd
        dataframe['macd_signal'] = signal
        dataframe['atr'] = ta.ATR(high, low, close, timeperiod=14)
        
        return dataframe

    @staticmethod
    def update_indicators(dataframe, new_row):
        dataframe = dataframe.append(new_row, ignore_index=True)
        dataframe = TechnicalAnalyzer.initialize_indicators(dataframe)
        return dataframe.tail(Config.INDICATOR_LOOKBACK)