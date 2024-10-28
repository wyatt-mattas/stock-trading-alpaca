# sentiment_analyzer.py
"""Sentiment analysis functionality"""
from textblob import TextBlob
from newsapi import NewsApiClient
import yfinance as yf
import time
import numpy as np
from datetime import datetime, timedelta

class SentimentAnalyzer:
    def __init__(self, newsapi_key):
        self.newsapi = NewsApiClient(api_key=newsapi_key)
        self.sentiment_cache = {}
        self.cache_expiry = 3600
    
    def get_company_news(self, symbol, days_back=2):
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
        current_time = time.time()
        
        if symbol in self.sentiment_cache:
            cached_sentiment, cache_time = self.sentiment_cache[symbol]
            if current_time - cache_time < self.cache_expiry:
                return cached_sentiment
        
        articles = self.get_company_news(symbol)
        if not articles:
            return 0.0
        
        sentiments = [TextBlob(f"{article['title']} {article['description']}").sentiment.polarity 
                     for article in articles]
        
        avg_sentiment = np.mean(sentiments) if sentiments else 0.0
        self.sentiment_cache[symbol] = (avg_sentiment, current_time)
        
        return avg_sentiment