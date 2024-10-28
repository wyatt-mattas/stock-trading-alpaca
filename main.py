# main.py
"""Main trading application"""
import asyncio
import multiprocessing
import traceback
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from twilio.rest import Client

from calculations import Calculations
from config import Config
from market_operations import MarketOperations
from trade_manager import TradeManager
from technical_analyzer import TechnicalAnalyzer
from sentiment_analyzer import SentimentAnalyzer

async def main():
    try:
        # Initialize clients
        trading_client = TradingClient(Config.API_KEY_ID, Config.API_SECRET, paper=True)
        data_client = StockHistoricalDataClient(Config.API_KEY_ID, Config.API_SECRET)
        stream = StockDataStream(Config.API_KEY_ID, Config.API_SECRET)
        
        # Initialize components
        technical_analyzer = TechnicalAnalyzer()
        sentiment_analyzer = SentimentAnalyzer(Config.NEWS_API_KEY)
        trade_manager = TradeManager(trading_client, data_client)
        twilio_client = Client(Config.TWILIO_SID, Config.TWILIO_AUTH_TOKEN)
        
        # Initialize calculations with both analyzers
        calc = Calculations(
            trading_client, 
            data_client,
            technical_analyzer,
            sentiment_analyzer
        )
        
        # Initialize market operations
        market_ops = MarketOperations(
            trading_client,
            data_client,
            stream,
            calc,
            twilio_client
        )
        
        while True:
            try:
                print('Waiting for market to open...')
                await market_ops.wait_for_market_open()
                print('Market opened.')
                
                # Run trading day
                await market_ops.run_trading_day()
                
                # Sleep before next trading day
                await asyncio.sleep(60)
                
            except Exception as e:
                print(f'Trading day error: {str(e)}')
                print(traceback.format_exc())
                twilio_client.messages.create(
                    from_='+13343732933',
                    to='+16207578055',
                    body=f'Trading day error: {str(e)}'
                )
                await asyncio.sleep(60)
                
    except KeyboardInterrupt:
        print('Shutting down gracefully...')
        stream.stop()
    except Exception as e:
        print(f'Fatal error: {str(e)}')
        print(traceback.format_exc())
        twilio_client.messages.create(
            from_='+13343732933',
            to='+16207578055',
            body=f'Fatal error: {str(e)}'
        )
    finally:
        stream.stop()

if __name__ == '__main__':
    asyncio.run(main())