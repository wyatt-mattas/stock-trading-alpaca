"""Market data handling and streaming"""
from alpaca.data.timeframe import TimeFrame
from alpaca.data.requests import StockBarsRequest, StockSnapshotRequest
import pandas as pd
import asyncio

class MarketData:
    def __init__(self, trading_client, data_client, stream, sentiment_analyzer):
        self.trading_client = trading_client
        self.data_client = data_client
        self.stream = stream
        self.sentiment_analyzer = sentiment_analyzer
        
    async def get_tradable_universe(self):
        assets = self.trading_client.get_all_assets()
        tradable_symbols = [asset.symbol for asset in assets if asset.tradable]
        
        ticker_data = []
        for symbol in tradable_symbols:
            try:
                snapshot_request = StockSnapshotRequest(symbol_or_symbols=symbol)
                snapshot = self.data_client.get_stock_snapshot(snapshot_request)
                
                if snapshot and snapshot.latest_trade:
                    price = snapshot.latest_trade.price
                    volume = snapshot.latest_quote.volume
                    
                    if (price >= Config.MIN_SHARE_PRICE and
                        price <= Config.MAX_SHARE_PRICE and
                        volume * price > Config.MIN_LAST_DV):
                        
                        ticker_data.append({
                            'symbol': symbol,
                            'price': price,
                            'volume': volume
                        })
                        
            except Exception as e:
                print(f"Error processing {symbol}: {str(e)}")
                continue
                
            if len(ticker_data) % 200 == 0:
                await asyncio.sleep(61)
        
        return [ticker['symbol'] for ticker in ticker_data[:50]]