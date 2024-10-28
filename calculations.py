"""Trading calculations and analysis"""
from datetime import datetime, timedelta
import pandas as pd
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.timeframe import TimeFrame
from alpaca.data.requests import StockBarsRequest
import csv
import time
from config import Config

class Calculations:
    def __init__(self, trading_client, data_client, technical_analyzer, sentiment_analyzer):
        self.trading_client = trading_client
        self.data_client = data_client
        self.technical_analyzer = technical_analyzer
        self.sentiment_analyzer = sentiment_analyzer
        
        # Trading parameters
        self.min_share_price = Config.MIN_SHARE_PRICE
        self.max_share_price = Config.MAX_SHARE_PRICE
        self.min_last_dv = Config.MIN_LAST_DV
        self.risk = Config.RISK_PERCENTAGE
        
        # Technical parameters
        self.rsi_period = Config.RSI_PERIOD
        self.rsi_overbought = Config.RSI_OVERBOUGHT
        self.rsi_oversold = Config.RSI_OVERSOLD
        self.macd_fast = Config.MACD_FAST
        self.macd_slow = Config.MACD_SLOW
        self.macd_signal = Config.MACD_SIGNAL
        self.buy_threshold = Config.BUY_THRESHOLD
        self.sell_threshold = Config.SELL_THRESHOLD
        self.indicator_lookback = Config.INDICATOR_LOOKBACK

    async def prev_weekday(self, adate):
        """Get previous weekday"""
        adate -= timedelta(days=1)
        while adate.weekday() > 4:  # Mon-Fri are 0-4
            adate -= timedelta(days=1)
        return adate

    async def grab_data(self):
        """Get historical data for tracked symbols"""
        ticker_list = await self.get_tickers()
        
        if ticker_list:
            with open('ticker_list_liquid.csv', 'w') as myfile:
                wr = csv.writer(myfile)
                wr.writerow(ticker_list)

        with open('ticker_list_liquid.csv', newline='') as f:
            reader = csv.reader(f)
            ticker_list = list(reader)[0]
            print(ticker_list)

        prev_date = await self.prev_weekday(datetime.today())
        
        df_list = {}
        for ticker in ticker_list:
            try:
                bars_request = StockBarsRequest(
                    symbol_or_symbols=ticker,
                    timeframe=TimeFrame.Minute,
                    start=prev_date,
                    limit=50
                )
                bars = self.data_client.get_stock_bars(bars_request)
                
                if bars and ticker in bars:
                    df = pd.DataFrame([bar.dict() for bar in bars[ticker]])
                    if not df.empty:
                        df_list[ticker] = self.calc_indicators(df)
            
            except Exception as e:
                print(f"Error getting data for {ticker}: {str(e)}")
                continue
            
            if len(df_list) % 200 == 0:
                time.sleep(61)  # Rate limiting
                
        return df_list, ticker_list

    async def get_tickers(self):
        """Get list of tradable tickers meeting criteria"""
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
                    
                    if (price >= self.min_share_price and
                        price <= self.max_share_price and
                        volume * price > self.min_last_dv):
                        
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
                                    sentiment_score = self.sentiment_analyzer.analyze_sentiment(symbol)
                                    
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
        
        # Sort by combined score
        ticker_data.sort(key=lambda x: (x['change_percent'] * 0.7 + x['sentiment'] * 30), 
                        reverse=True)
        
        return [ticker['symbol'] for ticker in ticker_data[:50]]

    def calculate_score(self, row):
        """Calculate trading score based on technical and sentiment indicators"""
        # Technical indicators score (70% weight)
        tech_score = (
            20 * (row['ema5'] > row['ema10'] > row['ema20']) - 
            20 * (row['ema5'] < row['ema10'] < row['ema20']) +
            30 * (row['rsi'] < self.rsi_oversold) - 
            30 * (row['rsi'] > self.rsi_overbought) +
            20 * (row['macd'] > row['macd_signal']) - 
            20 * (row['macd'] <= row['macd_signal']) +
            15 * ((row['close'] - row['open']) > (1.5 * row['atr'])) - 
            15 * ((row['open'] - row['close']) > (1.5 * row['atr'])) +
            15 * (row['volume'] > row['volume_ma20']) - 
            15 * (row['volume'] <= row['volume_ma20'])
        )
        
        # Sentiment score (30% weight)
        sentiment_score = self.sentiment_analyzer.analyze_sentiment(row.name) * 100
        
        return (tech_score * 0.7) + (sentiment_score * 0.3)

    def buy_sell_calc(self, dataframe, ticker):
        """Calculate buy/sell signals and execute trades"""
        ticker_symbol = ticker['symbol']
        positions = self.trading_client.get_all_positions()
        
        dataframe['volume_ma20'] = dataframe['volume'].rolling(window=20).mean()
        dataframe['score'] = dataframe.apply(self.calculate_score, axis=1)
        current_score = dataframe['score'].iloc[-1]

        if current_score > self.buy_threshold:
            can_buy = self.get_positions_buy(ticker_symbol, positions)
            if can_buy:
                quantity = self.calc_num_of_stocks(dataframe)
                stop_price = round(dataframe['close'].iloc[-1] * 0.97, 2)
                
                order_data = MarketOrderRequest(
                    symbol=ticker_symbol,
                    qty=quantity,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY,
                    stop_loss=dict(stop_price=str(stop_price))
                )
                
                self.submit_order(order_data)

        elif current_score < self.sell_threshold:
            position_data = self.get_positions_sell(ticker_symbol, positions)
            if position_data:
                qty, orderside = position_data
                if orderside == 'long':
                    order_data = MarketOrderRequest(
                        symbol=ticker_symbol,
                        qty=qty,
                        side=OrderSide.SELL,
                        time_in_force=TimeInForce.DAY
                    )
                    self.submit_order(order_data)

    def submit_order(self, order_data):
        """Submit trading order"""
        try:
            order = self.trading_client.submit_order(order_data)
            print(f'Market order of {order_data.qty} {order_data.symbol} {order_data.side} completed.')
            return order
        except Exception as e:
            print(f'Order failed: {str(e)}')
            return None

    def calc_num_of_stocks(self, dataframe):
        """Calculate number of shares to trade"""
        portfolio_value = float(self.trading_client.get_account().buying_power)
        rough_number = (portfolio_value * self.risk) / dataframe['low'][dataframe.index[-1]]
        quantity = max(1, round(rough_number))
        return quantity

    def get_positions_sell(self, ticker, position):
        """Check if position can be sold"""
        positions = position
        if positions:
            for position in positions:
                if position.symbol == ticker:
                    if position.side == 'long':
                        return abs(int(float(position.qty))), 'long'
        return None

    def get_positions_buy(self, ticker, position):
        """Check if new position can be opened"""
        positions = position
        if not positions:
            return True
            
        for position in positions:
            if position.symbol == ticker:
                return False
            
        return True

    async def evaluate_positions(self):
        """Evaluate current positions for end-of-day decisions"""
        positions = self.trading_client.list_positions()
        position_analysis = []
        
        for position in positions:
            try:
                bars_request = StockBarsRequest(
                    symbol_or_symbols=position.symbol,
                    timeframe=TimeFrame.Minute,
                    limit=100
                )
                bars = self.data_client.get_stock_bars(bars_request)
                
                if bars and position.symbol in bars:
                    df = pd.DataFrame([bar.dict() for bar in bars[position.symbol]])
                    if not df.empty:
                        df = self.technical_analyzer.initialize_indicators(df)
                        current_score = self.calculate_score(df.iloc[-1])
                        
                        sentiment = self.sentiment_analyzer.analyze_sentiment(position.symbol)
                        
                        position_analysis.append({
                            'symbol': position.symbol,
                            'quantity': int(position.qty),
                            'technical_score': current_score,
                            'sentiment_score': sentiment,
                            'unrealized_pl': float(position.unrealized_pl),
                            'unrealized_plpc': float(position.unrealized_plpc),
                            'current_price': float(position.current_price)
                        })
                        
            except Exception as e:
                print(f"Error evaluating position {position.symbol}: {str(e)}")
                continue
        
        return position_analysis

    async def end_of_day_optimization(self):
        """Optimize portfolio before market close"""
        try:
            positions = await self.evaluate_positions()
            
            positions.sort(key=lambda x: (x['technical_score'] * 0.7 + x['sentiment_score'] * 30))
            
            num_positions = len(positions)
            positions_to_sell = positions[:int(num_positions * 0.3)]
            
            for position in positions_to_sell:
                if position['unrealized_plpc'] < -0.02 or position['sentiment_score'] < -0.2:
                    try:
                        order_data = MarketOrderRequest(
                            symbol=position['symbol'],
                            qty=position['quantity'],
                            side=OrderSide.SELL,
                            time_in_force=TimeInForce.DAY
                        )
                        self.submit_order(order_data)
                        print(f"Closed position in {position['symbol']} due to poor performance/sentiment")
                    except Exception as e:
                        print(f"Error closing position in {position['symbol']}: {str(e)}")
            
            return [p['symbol'] for p in positions[int(num_positions * 0.3):]]
            
        except Exception as e:
            print(f"Error in end of day optimization: {str(e)}")
            return []