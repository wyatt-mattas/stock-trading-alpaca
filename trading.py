"""Trading operations and risk management"""
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.requests import StockBarsRequest, StockSnapshotRequest
from alpaca.data.timeframe import TimeFrame
import asyncio
import pandas as pd
import csv
from datetime import datetime, timedelta

from circuit_breaker import CircuitBreaker
from config import Config

class TradingSystem:
    def __init__(self, trading_client, data_client, stream, analysis, twilio_client):
        self.trading_client = trading_client
        self.data_client = data_client
        self.stream = stream
        self.analysis = analysis
        self.twilio_client = twilio_client
        self.df_ticker_list = {}
        self.ticker_list = []
        self.circuit_breaker = CircuitBreaker(trading_client)
        self.price_history = {}

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
                
            if time_to_open == 5:
                try:
                    self.df_ticker_list, self.ticker_list = await self.get_initial_data()
                    await self.setup_streaming(self.ticker_list)
                except Exception as e:
                    error_msg = f'Pre-market data gathering error: {str(e)}'
                    print(error_msg)
                    self.notify(error_msg)
                    
            await asyncio.sleep(3)

    async def get_tradable_universe(self):
        """Get list of tradable stocks meeting criteria"""
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
                    
                    if (price >= Config.MIN_SHARE_PRICE and
                        price <= Config.MAX_SHARE_PRICE and
                        volume * price > Config.MIN_LAST_DV):
                        
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
                                
                                if change_percent >= Config.MIN_PRICE_CHANGE:
                                    sentiment_score = self.analysis.analyze_sentiment(symbol)
                                    
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
                    self.df_ticker_list[bar.symbol] = self.analysis.initialize_indicators(
                        self.df_ticker_list[bar.symbol]
                    )
                else:
                    self.df_ticker_list[bar.symbol] = self.analysis.update_indicators(
                        self.df_ticker_list[bar.symbol], 
                        bar_data
                    )
                
                await self.process_trading_signals(self.df_ticker_list[bar.symbol], bar.symbol)
                
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
            daily_return = (equity - initial_equity) / initial_equity
            
            if daily_return < Config.DAILY_LOSS_THRESHOLD:
                message = f'Daily loss threshold exceeded: {daily_return:.2%}'
                print(message)
                self.trading_client.close_all_positions()
                self.notify(message)
                
            positions = self.trading_client.get_all_positions()
            for position in positions:
                unrealized_plpc = float(position.unrealized_plpc)
                if unrealized_plpc < Config.MAX_POSITION_LOSS:
                    message = f'Closed position {position.symbol} due to excessive loss: {unrealized_plpc:.2%}'
                    print(message)
                    self.trading_client.close_position(position.symbol)
                    
        except Exception as e:
            error_msg = f'Risk management check error: {str(e)}'
            print(error_msg)
            self.notify(error_msg)

    def notify(self, message):
        """Send notification via Twilio"""
        try:
            self.twilio_client.messages.create(
                from_='+13343732933',
                to='+16207578055',
                body=message
            )
        except Exception as e:
            print(f"Error sending notification: {str(e)}")

    async def process_trading_signals(self, dataframe, symbol):
        """Process trading signals and execute trades"""
        try:
            positions = self.trading_client.get_all_positions()
            score = self.analysis.calculate_score(dataframe.iloc[-1], symbol)
            
            if score > Config.BUY_THRESHOLD:
                # Check if we can buy
                if not any(p.symbol == symbol for p in positions):
                    # Calculate position size
                    portfolio_value = float(self.trading_client.get_account().buying_power)
                    rough_number = (portfolio_value * Config.RISK_PERCENTAGE) / dataframe['close'].iloc[-1]
                    quantity = max(1, round(rough_number))
                    
                    # Set stop loss
                    stop_price = round(dataframe['close'].iloc[-1] * 0.97, 2)
                    
                    # Create and submit order
                    order_data = MarketOrderRequest(
                        symbol=symbol,
                        qty=quantity,
                        side=OrderSide.BUY,
                        time_in_force=TimeInForce.DAY,
                        stop_loss=dict(stop_price=str(stop_price))
                    )
                    
                    await self.submit_order(order_data)
                    
            elif score < Config.SELL_THRESHOLD:
                # Check if we have a position to sell
                position = next((p for p in positions if p.symbol == symbol), None)
                if position:
                    order_data = MarketOrderRequest(
                        symbol=symbol,
                        qty=abs(int(float(position.qty))),
                        side=OrderSide.SELL,
                        time_in_force=TimeInForce.DAY
                    )
                    
                    await self.submit_order(order_data)
                    
        except Exception as e:
            print(f"Error processing signals for {symbol}: {str(e)}")

    async def submit_order(self, order_data):
        """Submit trading order with error handling"""
        try:
            order = self.trading_client.submit_order(order_data)
            print(f'Market order of {order_data.qty} {order_data.symbol} {order_data.side} completed.')
            return order
        except Exception as e:
            print(f'Order failed: {str(e)}')
            return None

    async def optimize_portfolio(self):
        """Optimize portfolio before market close"""
        try:
            positions = self.trading_client.get_all_positions()
            position_analysis = []
            
            for position in positions:
                # Get recent data
                bars_request = StockBarsRequest(
                    symbol_or_symbols=position.symbol,
                    timeframe=TimeFrame.Minute,
                    limit=100
                )
                bars = self.data_client.get_stock_bars(bars_request)
                
                if bars and position.symbol in bars:
                    df = pd.DataFrame([bar.dict() for bar in bars[position.symbol]])
                    if not df.empty:
                        df = self.analysis.initialize_indicators(df)
                        score = self.analysis.calculate_score(df.iloc[-1], position.symbol)
                        
                        position_analysis.append({
                            'symbol': position.symbol,
                            'quantity': int(position.qty),
                            'score': score,
                            'unrealized_plpc': float(position.unrealized_plpc)
                        })
            
            # Sort positions by score
            position_analysis.sort(key=lambda x: x['score'])
            
            # Close bottom 30% of positions
            positions_to_close = position_analysis[:int(len(position_analysis) * 0.3)]
            for position in positions_to_close:
                if position['unrealized_plpc'] < -0.02:
                    order_data = MarketOrderRequest(
                        symbol=position['symbol'],
                        qty=position['quantity'],
                        side=OrderSide.SELL,
                        time_in_force=TimeInForce.DAY
                    )
                    await self.submit_order(order_data)
                    print(f"Closed position in {position['symbol']} due to poor performance")
            
            return [p['symbol'] for p in position_analysis[int(len(position_analysis) * 0.3):]]
            
        except Exception as e:
            print(f"Error in portfolio optimization: {str(e)}")
            return []