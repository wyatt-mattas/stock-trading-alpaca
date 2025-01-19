"""Configuration settings"""

class Config:
    # API Keys (to be loaded from environment/files)
    API_KEY_ID = None  # Load from file
    API_SECRET = None  # Load from file
    NEWS_API_KEY = None  # Load from file
    TWILIO_SID = None  # Load from file
    TWILIO_AUTH_TOKEN = None  # Load from file
    
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
    
    # Trading Thresholds
    BUY_THRESHOLD = 70
    SELL_THRESHOLD = -70
    INDICATOR_LOOKBACK = 100
    MIN_PRICE_CHANGE = 3.5  # Minimum price change percentage for consideration
    
    # Risk Management
    MAX_POSITION_LOSS = -0.03  # -3% max loss per position
    DAILY_LOSS_THRESHOLD = -0.02  # -2% max daily loss
    
    @classmethod
    def load_config(cls):
        """Load configuration from files"""
        try:
            cls.API_KEY_ID = open('C:\\Account IDs\\AlpacaAPIIDOrig.txt', 'r').read()
            cls.API_SECRET = open('C:\\Account IDs\\AlpacaAPISecretOrig.txt', 'r').read()
            cls.NEWS_API_KEY = open('C:\\Account IDs\\NewsAPIKey.txt', 'r').read()
            cls.TWILIO_SID = open('C:\\Account IDs\\SID.txt', 'r').read()
            cls.TWILIO_AUTH_TOKEN = open('C:\\Account IDs\\token.txt', 'r').read()
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration: {str(e)}")