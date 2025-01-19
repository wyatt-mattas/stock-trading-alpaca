"""Technical and sentiment analysis functionality"""
from config import Config
import talib._ta_lib as ta
import pandas as pd
from textblob import TextBlob
from newsapi import NewsApiClient
import yfinance as yf
import numpy as np
from datetime import datetime, timedelta
import time

class Analysis:
    def __init__(self, newsapi_key):
        self.newsapi = NewsApiClient(api_key=newsapi_key)
        self.sentiment_cache = {}
        self.cache_expiry = 3600  # 1 hour
        
    def initialize_indicators(self, dataframe):
        """Calculate technical indicators"""
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
        dataframe['volume_ma20'] = dataframe['volume'].rolling(window=20).mean()
        
        return dataframe

    def update_indicators(self, dataframe, new_row):
        """Update indicators with new data"""
        dataframe = dataframe.append(new_row, ignore_index=True)
        dataframe = self.initialize_indicators(dataframe)
        return dataframe.tail(Config.INDICATOR_LOOKBACK)

    def get_company_news(self, symbol, days_back=2):
        """Get recent news articles for a company"""
        try:
            company = yf.Ticker(symbol)
            company_name = company.info.get('longName', symbol)
            
            from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            articles = self.newsapi.get_everything(
                q=f'"{company_name}" OR ${symbol}',
                from_param=from_date,
                language='en',
                sort_by='relevancy'
            )
            
            return articles.get('articles', [])
        except Exception as e:
            print(f"Error fetching news for {symbol}: {str(e)}")
            return []

    def analyze_sentiment(self, symbol):
        """Analyze sentiment from news articles"""
        current_time = time.time()
        
        if symbol in self.sentiment_cache:
            cached_sentiment, cache_time = self.sentiment_cache[symbol]
            if current_time - cache_time < self.cache_expiry:
                return cached_sentiment
        
        articles = self.get_company_news(symbol)
        if not articles:
            return 0.0
        
        sentiments = []
        for article in articles:
            text = f"{article['title']} {article['description']}"
            blob = TextBlob(text)
            sentiments.append(blob.sentiment.polarity)
        
        avg_sentiment = np.mean(sentiments) if sentiments else 0.0
        self.sentiment_cache[symbol] = (avg_sentiment, current_time)
        
        return avg_sentiment

    def calculate_score(self, row, symbol):
        """Calculate trading score based on technical and sentiment indicators"""
        tech_score = (
            20 * (row['ema5'] > row['ema10'] > row['ema20']) - 
            20 * (row['ema5'] < row['ema10'] < row['ema20']) +
            30 * (row['rsi'] < Config.RSI_OVERSOLD) - 
            30 * (row['rsi'] > Config.RSI_OVERBOUGHT) +
            20 * (row['macd'] > row['macd_signal']) - 
            20 * (row['macd'] <= row['macd_signal']) +
            15 * ((row['close'] - row['open']) > (1.5 * row['atr'])) - 
            15 * ((row['open'] - row['close']) > (1.5 * row['atr'])) +
            15 * (row['volume'] > row['volume_ma20']) - 
            15 * (row['volume'] <= row['volume_ma20'])
        )
        
        sentiment_score = self.analyze_sentiment(symbol) * 100
        return (tech_score * 0.7) + (sentiment_score * 0.3)