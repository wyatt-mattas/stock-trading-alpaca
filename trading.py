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
            # Update price history for volatility checking
            current_price = dataframe['close'].iloc[-1]
            if symbol not in self.price_history:
                self.price_history[symbol] = []
            self.price_history[symbol].append(current_price)
            self.price_history[symbol] = self.price_history[symbol][-10:]  # Keep last 10 prices
            
            # Check circuit breakers
            breaker_type, triggered = await self.circuit_breaker.check_volatility_circuit_breaker(
                symbol, current_price, self.price_history[symbol]
            )
            if triggered:
                self.circuit_breaker.handle_breaker_trigger(breaker_type, self.notify)
                return
                
            if not await self.circuit_breaker.can_trade():
                return
                
            # Process trading signals
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
        """Optimize portfolio before market close and decide on overnight positions"""
        try:
            positions = self.trading_client.get_all_positions()
            position_analysis = []
            
            for position in positions:
                try:
                    # Get both intraday and daily data for comprehensive analysis
                    intraday_request = StockBarsRequest(
                        symbol_or_symbols=position.symbol,
                        timeframe=TimeFrame.Minute,
                        limit=390  # Full trading day
                    )
                    daily_request = StockBarsRequest(
                        symbol_or_symbols=position.symbol,
                        timeframe=TimeFrame.Day,
                        limit=10  # Last 10 days
                    )
                    
                    intraday_bars = self.data_client.get_stock_bars(intraday_request)
                    daily_bars = self.data_client.get_stock_bars(daily_request)
                    
                    if intraday_bars and daily_bars and position.symbol in intraday_bars and position.symbol in daily_bars:
                        # Analyze intraday momentum
                        intraday_df = pd.DataFrame([bar.dict() for bar in intraday_bars[position.symbol]])
                        daily_df = pd.DataFrame([bar.dict() for bar in daily_bars[position.symbol]])
                        
                        if not intraday_df.empty and not daily_df.empty:
                            # Technical analysis
                            intraday_df = self.analysis.initialize_indicators(intraday_df)
                            daily_df = self.analysis.initialize_indicators(daily_df)
                            
                            # Calculate key metrics
                            current_price = float(position.current_price)
                            entry_price = float(position.avg_entry_price)
                            unrealized_plpc = float(position.unrealized_plpc)
                            
                            # Analyze end of day momentum
                            last_hour_change = (current_price - intraday_df['close'].iloc[-60]) / intraday_df['close'].iloc[-60]
                            volume_surge = intraday_df['volume'].iloc[-30:].mean() > intraday_df['volume'].iloc[:-30].mean() * 1.2
                            
                            # Daily trend analysis
                            daily_trend = (daily_df['ema5'].iloc[-1] > daily_df['ema20'].iloc[-1])
                            extended_hours_interest = await self.check_after_hours_interest(position.symbol)
                            
                            # Get sentiment and overnight holding score
                            sentiment = self.analysis.analyze_sentiment(position.symbol)
                            overnight_score = await self.calculate_overnight_score(
                                daily_trend=daily_trend,
                                last_hour_momentum=last_hour_change,
                                volume_surge=volume_surge,
                                sentiment=sentiment,
                                extended_hours_interest=extended_hours_interest,
                                unrealized_plpc=unrealized_plpc,
                                technical_score=self.analysis.calculate_score(intraday_df.iloc[-1], position.symbol)
                            )
                            
                            position_analysis.append({
                                'symbol': position.symbol,
                                'quantity': int(position.qty),
                                'overnight_score': overnight_score,
                                'unrealized_plpc': unrealized_plpc,
                                'current_price': current_price,
                                'entry_price': entry_price,
                                'last_hour_change': last_hour_change,
                                'volume_surge': volume_surge,
                                'sentiment': sentiment
                            })
                            
                except Exception as e:
                    print(f"Error analyzing position {position.symbol}: {str(e)}")
                    continue
            
            if position_analysis:
                # Sort positions by overnight score
                position_analysis.sort(key=lambda x: x['overnight_score'])
                
                # Analyze which positions to close
                for position in position_analysis:
                    hold_overnight = await self.should_hold_overnight(position)
                    
                    if not hold_overnight:
                        order_data = MarketOrderRequest(
                            symbol=position['symbol'],
                            qty=position['quantity'],
                            side=OrderSide.SELL,
                            time_in_force=TimeInForce.DAY
                        )
                        await self.submit_order(order_data)
                        
                        message = (
                            f"Closing position in {position['symbol']}:\n"
                            f"Entry: ${position['entry_price']:.2f}\n"
                            f"Exit: ${position['current_price']:.2f}\n"
                            f"P&L: {position['unrealized_plpc']:.2%}\n"
                            f"Reason: Low overnight potential"
                        )
                        print(message)
                        self.notify(message)
                
                # Keep track of positions we're holding overnight
                return [p['symbol'] for p in position_analysis if await self.should_hold_overnight(p)]
            
            return []
            
        except Exception as e:
            print(f"Error in portfolio optimization: {str(e)}")
            return []

    async def check_after_hours_interest(self, symbol):
        """Check for after-hours trading interest"""
        try:
            snapshot_request = StockSnapshotRequest(symbol_or_symbols=symbol)
            snapshot = self.data_client.get_stock_snapshot(snapshot_request)
            
            if snapshot and snapshot.latest_trade and snapshot.latest_quote:
                regular_volume = snapshot.latest_trade.volume
                current_volume = snapshot.latest_quote.volume
                
                # Calculate after-hours volume ratio
                after_hours_ratio = (current_volume - regular_volume) / regular_volume
                return after_hours_ratio > 0.1  # More than 10% additional volume
            
            return False
        except Exception as e:
            print(f"Error checking after-hours interest for {symbol}: {str(e)}")
            return False

    async def calculate_overnight_score(self, **kwargs):
        """Calculate score for overnight holding potential"""
        try:
            score = 0
            
            # Technical and trend factors (40%)
            if kwargs.get('daily_trend', False):
                score += 20
            if kwargs.get('last_hour_momentum', 0) > 0.01:  # 1% uptick
                score += 10
            if kwargs.get('volume_surge', False):
                score += 10
                
            # Sentiment and interest factors (30%)
            sentiment = kwargs.get('sentiment', 0)
            score += sentiment * 20  # Convert -1 to 1 scale to -20 to 20
            if kwargs.get('extended_hours_interest', False):
                score += 10
                
            # Current position performance (30%)
            technical_score = kwargs.get('technical_score', 0)
            score += technical_score * 0.3
            
            # Penalty for losing positions
            unrealized_plpc = kwargs.get('unrealized_plpc', 0)
            if unrealized_plpc < -0.02:  # More than 2% loss
                score -= 20
            
            return score
            
        except Exception as e:
            print(f"Error calculating overnight score: {str(e)}")
            return 0

    async def should_hold_overnight(self, position):
        """Determine if a position should be held overnight"""
        try:
            # Basic criteria for overnight holdings
            if position['unrealized_plpc'] < -0.03:  # Don't hold significant losses
                return False
                
            if position['overnight_score'] < 50:  # Below threshold score
                return False
                
            # Check if we're in a strong uptrend
            if position['last_hour_change'] < -0.02:  # Significant end-of-day weakness
                return False
                
            # Volume confirmation
            if not position['volume_surge'] and position['sentiment'] < 0:
                return False
                
            return True
            
        except Exception as e:
            print(f"Error evaluating overnight holding for {position['symbol']}: {str(e)}")
            return False