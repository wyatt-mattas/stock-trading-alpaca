"""Configuration settings and API keys"""
import os

class Config:
    # API Keys
    API_KEY_ID = open('C:\\Account IDs\\AlpacaAPIIDOrig.txt', 'r').read()
    API_SECRET = open('C:\\Account IDs\\AlpacaAPISecretOrig.txt', 'r').read()
    NEWS_API_KEY = open('C:\\Account IDs\\NewsAPIKey.txt', 'r').read()
    TWILIO_SID = open('C:\\Account IDs\\SID.txt', 'r').read()
    TWILIO_AUTH_TOKEN = open('C:\\Account IDs\\token.txt', 'r').read()
    
    # Trading Parameters
    MIN_SHARE_PRICE = 1.00
    MAX_SHARE_PRICE = 20.00
    MIN_LAST_DV = 500000
    RISK_PERCENTAGE = 0.015
    
    # Technical Indicators
    RSI_PERIOD = 14
    RSI_OVERBOUGHT = 70
    RSI_OVERSOLD = 30
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9
    
    # Scoring Thresholds
    BUY_THRESHOLD = 70
    SELL_THRESHOLD = -70
    INDICATOR_LOOKBACK = 100