# market_operations.py
"""Market operations and trading loop functionality"""
import asyncio
import time
from datetime import datetime, timedelta
import pandas as pd
from alpaca.data.timeframe import TimeFrame
from alpaca.data.requests import StockBarsRequest, StockSnapshotRequest
import csv

class MarketOperations:
    def __init__(self, trading_client, data_client, stream, calc, twilio_client):
        self.trading_client = trading_client
        self.data_client = data_client
        self.stream = stream
        self.calc = calc
        self.twilio_client = twilio_client
        self.df_ticker_list = {}
        self.ticker_list = []
        
    async def wait_for_market_open(self):
        """Wait for market to open and prepare pre-market data"""
        time_list = []

        while True:
            clock = self.trading_client.get_clock()
            if clock.is_open:
                break
                
            opening_time = clock.next_open.timestamp()
            curr_time = clock.timestamp.timestamp()
            time_to_open = int((opening_time - curr_time) / 60)
            
            if time_to_open not in time_list:
                print(f'{time_to_open} minutes til market open.')
                time_list.clear()
                time_list.append(time_to_open)
                
            # Get data 5 minutes before market open
            if time_to_open == 5:
                try:
                    self.df_ticker_list, self.ticker_list = await self.calc.grab_data()
                    await self.setup_streaming(self.ticker_list)
                except Exception as e:
                    print(f'Pre-market data gathering error: {str(e)}')
                    self.twilio_client.messages.create(
                        from_='+13343732933',
                        to='+16207578055',
                        body=f'Pre-market data gathering error: {str(e)}'
                    )
                    
            await asyncio.sleep(3)

    async def get_tradable_universe(self):
        """Get list of tradable stocks meeting criteria with sentiment analysis"""
        print('Getting current ticker data and sentiment...')
        
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
                    
                    if (price >= self.calc.min_share_price and
                        price <= self.calc.max_share_price and
                        volume * price > self.calc.min_last_dv):
                        
                        # Get daily change
                        bars_request = StockBarsRequest(
                            symbol_or_symbols=symbol,
                            timeframe=TimeFrame.Day,
                            limit=2
                        )
                        bars = self.data_client.get_stock_bars(bars_request)
                        
                        if bars and symbol in bars:
                            daily_bars = bars[symbol]
                            if len(daily_bars) >= 2:
                                prev_close = daily_bars[-2].close
                                change_percent = ((price - prev_close) / prev_close) * 100
                                
                                if change_percent >= 3.5:
                                    sentiment_score = self.calc.sentiment_analyzer.analyze_sentiment(symbol)
                                    
                                    ticker_data.append({
                                        'symbol': symbol,
                                        'price': price,
                                        'volume': volume,
                                        'change_percent': change_percent,
                                        'sentiment': sentiment_score
                                    })
                
            except Exception as e:
                print(f"Error processing {symbol}: {str(e)}")
                continue
            
            if len(ticker_data) % 200 == 0:
                await asyncio.sleep(61)  # Rate limiting
        
        # Sort by combined score
        ticker_data.sort(key=lambda x: (x['change_percent'] * 0.7 + x['sentiment'] * 30), 
                        reverse=True)
        
        # Save to CSV
        selected_symbols = [ticker['symbol'] for ticker in ticker_data[:50]]
        if selected_symbols:
            with open('ticker_list_liquid.csv', 'w') as myfile:
                wr = csv.writer(myfile)
                wr.writerow(selected_symbols)
        
        return ticker_data[:50]

    async def setup_streaming(self, symbols):
        """Setup real-time data streaming"""
        async def handle_bar(bar):
            try:
                bar_data = {
                    'timestamp': bar.timestamp,
                    'open': bar.open,
                    'high': bar.high,
                    'low': bar.low,
                    'close': bar.close,
                    'volume': bar.volume
                }
                
                if bar.symbol not in self.df_ticker_list:
                    self.df_ticker_list[bar.symbol] = pd.DataFrame([bar_data])
                    self.df_ticker_list[bar.symbol] = self.calc.initialize_indicators(
                        self.df_ticker_list[bar.symbol]
                    )
                else:
                    self.df_ticker_list[bar.symbol] = self.calc.update_indicators(
                        self.df_ticker_list[bar.symbol], 
                        bar_data
                    )
                
                self.calc.buy_sell_calc(self.df_ticker_list[bar.symbol], {'symbol': bar.symbol})
                
            except Exception as e:
                print(f"Error handling bar data: {str(e)}")

        try:
            self.stream.subscribe_bars(handle_bar, *symbols)
            print(f"Subscribed to {len(symbols)} symbols")
        except Exception as e:
            print(f"Error setting up stream: {str(e)}")
            raise

    async def check_risk_management(self):
        """Perform periodic risk management checks"""
        try:
            account = self.trading_client.get_account()
            equity = float(account.equity)
            initial_equity = float(account.last_equity)
            
            daily_loss_threshold = -0.02
            daily_return = (equity - initial_equity) / initial_equity
            
            if daily_return < daily_loss_threshold:
                print(f'Daily loss threshold exceeded: {daily_return:.2%}')
                self.trading_client.close_all_positions()
                self.twilio_client.messages.create(
                    from_='+13343732933',
                    to='+16207578055',
                    body=f'Risk Management: Daily loss threshold exceeded ({daily_return:.2%}). All positions closed.'
                )
                
            positions = self.trading_client.get_all_positions()
            for position in positions:
                unrealized_plpc = float(position.unrealized_plpc)
                if unrealized_plpc < -0.03:
                    self.trading_client.close_position(position.symbol)
                    print(f'Closed position {position.symbol} due to excessive loss: {unrealized_plpc:.2%}')
                    
        except Exception as e:
            print(f'Risk management check error: {str(e)}')
            self.twilio_client.messages.create(
                from_='+13343732933',
                to='+16207578055',
                body=f'Risk management check error: {str(e)}'
            )

    async def run_trading_day(self):
        """Execute trading operations during market hours"""
        try:
            # Send initial equity notification
            account = self.trading_client.get_account()
            equity = float(account.equity)
            self.twilio_client.messages.create(
                from_='+13343732933',
                to='+16207578055',
                body=f'Market Open!\nCurrent Equity: ${equity}'
            )
            
            while self.trading_client.get_clock().is_open:
                try:
                    clock = self.trading_client.get_clock()
                    closing_time = clock.next_close.timestamp()
                    current_time = clock.timestamp.timestamp()
                    time_to_close = int((closing_time - current_time) / 60)
                    
                    # End of day optimization
                    if time_to_close <= 15:
                        print('Starting end-of-day portfolio optimization...')
                        await self.calc.end_of_day_optimization()
                        
                        account = self.trading_client.get_account()
                        equity = float(account.equity)
                        last_equity = float(account.last_equity)
                        price_change = round(equity - last_equity, 2)
                        
                        self.twilio_client.messages.create(
                            from_='+13343732933',
                            to='+16207578055',
                            body=f'Market Closing\nFinal Equity: ${equity}\nDay Change: ${price_change}'
                        )
                        break

                    # Initialize streaming if not already done
                    if not self.ticker_list:
                        universe = await self.get_tradable_universe()
                        self.ticker_list = [ticker['symbol'] for ticker in universe]
                        await self.setup_streaming(self.ticker_list)
                    
                    # Market closing procedures
                    if time_to_close <= 5:
                        try:
                            print('Closing all positions...')
                            self.trading_client.close_all_positions()
                            
                            account = self.trading_client.get_account()
                            equity = float(account.equity)
                            last_equity = float(account.last_equity)
                            price_change = round(equity - last_equity, 2)
                            
                            self.twilio_client.messages.create(
                                from_='+13343732933',
                                to='+16207578055',
                                body=f'Market Closed\nFinal Equity: ${equity}\nDay Change: ${price_change}'
                            )
                            
                            self.stream.unsubscribe_bars(*self.ticker_list)
                            break
                                
                        except Exception as e:
                            print(f'Error closing positions: {str(e)}')
                            self.twilio_client.messages.create(
                                from_='+13343732933',
                                to='+16207578055',
                                body=f'Error closing positions: {str(e)}'
                            )
                    
                    # Regular risk management check
                    if int(time.time()) % 300 == 0:
                        await self.check_risk_management()
                    
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    print(f'Trading loop error: {str(e)}')
                    self.twilio_client.messages.create(
                        from_='+13343732933',
                        to='+16207578055',
                        body=f'Trading loop error: {str(e)}'
                    )
                    await asyncio.sleep(30)
            
            # Clean up for next trading day
            self.df_ticker_list.clear()
            self.ticker_list.clear()
            
        except Exception as e:
            print(f'Error in run_trading_day: {str(e)}')
            self.twilio_client.messages.create(
                from_='+13343732933',
                to='+16207578055',
                body=f'Error in run_trading_day: {str(e)}'
            )