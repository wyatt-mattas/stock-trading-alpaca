import csv
import talib._ta_lib as ta
from datetime import timedelta, date, datetime
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockSnapshotRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.live import StockDataStream
import time
import asyncio
from calendar import monthrange
from twilio.rest import Client
import sys
import traceback
import numpy as np
import pandas as pd
import multiprocessing
from textblob import TextBlob
from newsapi import NewsApiClient
import yfinance as yf

# VERSION SENTIMENT
# ALPACA API KEYS
api_key_id = open('C:\\Account IDs\\AlpacaAPIIDOrig.txt', 'r').read()
api_secret = open('C:\\Account IDs\\AlpacaAPISecretOrig.txt', 'r').read()
newsapi_key = open('C:\\Account IDs\\NewsAPIKey.txt', 'r').read()

# KEYS FOR TWILIO
account_sid = open('C:\\Account IDs\\SID.txt', 'r').read()
auth_token = open('C:\\Account IDs\\token.txt', 'r').read()

# VERSION SENTIMENT
# ALPACA API KEYS
base_url = 'https://paper-api.alpaca.markets'
api_key_id = open('C:\\Account IDs\\AlpacaAPIIDOrig.txt', 'r').read()
api_secret = open('C:\\Account IDs\\AlpacaAPISecretOrig.txt', 'r').read()
newsapi_key = open('C:\\Account IDs\\NewsAPIKey.txt', 'r').read()  # Add News API key
# KEYS FOR TWILIO
account_sid = open('C:\\Account IDs\\SID.txt', 'r').read()
auth_token = open('C:\\Account IDs\\token.txt', 'r').read()

# VERSION LIQUID
# ALPACA API KEYS
base_url = 'https://paper-api.alpaca.markets'
api_key_id = open('C:\\Account IDs\\AlpacaAPIIDOrig.txt', 'r').read()
api_secret = open('C:\\Account IDs\\AlpacaAPISecretOrig.txt', 'r').read()
# KEYS FOR TWILIO
account_sid = open('C:\\Account IDs\\SID.txt', 'r').read()
auth_token = open('C:\\Account IDs\\token.txt', 'r').read()

class SentimentAnalysis:
    def __init__(self, newsapi_key):
        self.newsapi = NewsApiClient(api_key=newsapi_key)
        self.sentiment_cache = {}
        self.cache_expiry = 3600  # 1 hour cache expiry
        
    def get_company_news(self, symbol, days_back=2):
        try:
            # Get company name from symbol using yfinance
            company = yf.Ticker(symbol)
            company_name = company.info.get('longName', symbol)
            
            # Get news articles
            from_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime('%Y-%m-%d')
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
        
        # Check cache first
        if symbol in self.sentiment_cache:
            cached_sentiment, cache_time = self.sentiment_cache[symbol]
            if current_time - cache_time < self.cache_expiry:
                return cached_sentiment
        
        articles = self.get_company_news(symbol)
        if not articles:
            return 0.0
        
        # Analyze sentiment of headlines and descriptions
        sentiments = []
        for article in articles:
            text = f"{article['title']} {article['description']}"
            blob = TextBlob(text)
            sentiments.append(blob.sentiment.polarity)
        
        # Calculate weighted average sentiment
        avg_sentiment = np.mean(sentiments) if sentiments else 0.0
        
        # Cache the result
        self.sentiment_cache[symbol] = (avg_sentiment, current_time)
        
        return avg_sentiment


class Calculations:
    def __init__(self, trading_client, data_client):
        self.trading_client = trading_client
        self.data_client = data_client
        self.min_share_price = 1.00
        self.max_share_price = 20.00
        self.min_last_dv = 500000
        self.risk = 0.015
        self.rsi_period = 14
        self.rsi_overbought = 70
        self.rsi_oversold = 30
        self.macd_fast = 12
        self.macd_slow = 26
        self.macd_signal = 9
        self.buy_threshold = 70
        self.sell_threshold = -70
        self.indicator_lookback = 100
        self.sentiment_analyzer = SentimentAnalysis(newsapi_key)

    def initialize_indicators(self, dataframe):
        close = dataframe['close'].values
        high = dataframe['high'].values
        low = dataframe['low'].values
        
        dataframe['ema5'] = ta.EMA(close, timeperiod=5)
        dataframe['ema10'] = ta.EMA(close, timeperiod=10)
        dataframe['ema20'] = ta.EMA(close, timeperiod=20)
        dataframe['rsi'] = ta.RSI(close, timeperiod=self.rsi_period)
        macd, signal, _ = ta.MACD(close, fastperiod=self.macd_fast, 
                                 slowperiod=self.macd_slow, 
                                 signalperiod=self.macd_signal)
        dataframe['macd'] = macd
        dataframe['macd_signal'] = signal
        dataframe['atr'] = ta.ATR(high, low, close, timeperiod=14)
        
        return dataframe

    def update_indicators(self, dataframe, new_row):
        # Append the new row
        dataframe = dataframe.append(new_row, ignore_index=True)
        
        # Vectorized update of indicators
        close = dataframe['close'].values
        high = dataframe['high'].values
        low = dataframe['low'].values
        
        dataframe['ema5'] = ta.EMA(close, timeperiod=5)
        dataframe['ema10'] = ta.EMA(close, timeperiod=10)
        dataframe['ema20'] = ta.EMA(close, timeperiod=20)
        dataframe['rsi'] = ta.RSI(close, timeperiod=self.rsi_period)
        macd, signal, _ = ta.MACD(close, fastperiod=self.macd_fast, slowperiod=self.macd_slow, signalperiod=self.macd_signal)
        dataframe['macd'] = macd
        dataframe['macd_signal'] = signal
        dataframe['atr'] = ta.ATR(high, low, close, timeperiod=14)
        
        # Keep only the last 'indicator_lookback' periods
        dataframe = dataframe.tail(self.indicator_lookback)
        
        return dataframe

    # get a list of tickers that meet a certain criteria
    async def get_tickers(self):
        print('Getting current ticker data and sentiment...')
        
        # Get all assets
        assets = self.trading_client.get_all_assets()
        tradable_symbols = [asset.symbol for asset in assets if asset.tradable]
        
        # Get current snapshots for tradable symbols
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

    async def prev_weekday(self, adate):
        # get previous open day
        adate -= timedelta(days=1)
        while adate.weekday() > 4: # Mon-Fri are 0-4
            adate -= timedelta(days=1)
        return adate

    # grab data for each stock that is in the list
    async def grab_data(self):
        ticker_list = await self.get_tickers()
        
        if ticker_list:
            with open('ticker_list_liquid.csv', 'w') as myfile:
                wr = csv.writer(myfile)
                wr.writerow(ticker_list)

        with open('ticker_list_liquid.csv', newline='') as f:
            reader = csv.reader(f)
            ticker_list = list(reader)[0]
            print(ticker_list)

        # Get historical data
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

    # calculate the moving average for the dataframe
    def calc_indicators(self, dataframe):
        dataframe['ema5'] = ta.EMA(dataframe['close'], timeperiod=5)
        dataframe['ema10'] = ta.EMA(dataframe['close'], timeperiod=10)
        dataframe['ema20'] = ta.EMA(dataframe['close'], timeperiod=20)
        dataframe['rsi'] = ta.RSI(dataframe['close'], timeperiod=self.rsi_period)
        macd, signal, _ = ta.MACD(dataframe['close'], fastperiod=self.macd_fast, slowperiod=self.macd_slow, signalperiod=self.macd_signal)
        dataframe['macd'] = macd
        dataframe['macd_signal'] = signal
        dataframe['atr'] = ta.ATR(dataframe['high'], dataframe['low'], dataframe['close'], timeperiod=14)
        return dataframe

    def calculate_score(self, row):
        score = 0
        
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
                
                # Create market order with stop loss
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
        try:
            order = self.trading_client.submit_order(order_data)
            print(f'Market order of {order_data.qty} {order_data.symbol} {order_data.side} completed.')
            return order
        except Exception as e:
            print(f'Order failed: {str(e)}')
            return None

    # calculate the number of stocks that we want to buy
    def calc_num_of_stocks(self, dataframe):
        portfolio_value = float(self._api.get_account().buying_power) # TODO might make this equity or actual cash, might be screwing up buying power after market closes with negative cash and open positions
        rough_number = (portfolio_value * self.risk) / dataframe['low'][dataframe.index[-1]]
        quantity = round(rough_number) # make sure it is an even number
        if quantity < 1:
            quantity = 1 # want to buy at least one stock
        return quantity

    # get stock to sell based on if there is already a position
    def get_positions_sell(self, ticker, position):
        positions = position
        if positions != []:
            for position in positions:
                if position.symbol == ticker:
                    if position.side == 'long':
                        orderSide = 'long'
                        qty = abs(int(float(position.qty)))
                        return qty, orderSide

    # see if there is a position already there, if so we do not want to buy
    def get_positions_buy(self, ticker, position):
        positions = position
        can_buy = False
        if positions == []:
            can_buy = True
            return can_buy
        elif positions != []:
            for position in positions:
                if position.symbol != ticker:
                    can_buy = True
                elif position.symbol == ticker:
                    can_buy = False
                    break
            return can_buy

    async def evaluate_positions(self):
        """Evaluate current positions for end-of-day decisions"""
        positions = self._api.list_positions()
        position_analysis = []
        
        for position in positions:
            # Get recent price data
            barset = self._api.get_barset(position.symbol, 'minute', limit=100).df[position.symbol]
            
            # Calculate technical indicators
            df = self.initialize_indicators(barset)
            current_score = self.calculate_score(df.iloc[-1])
            
            # Get sentiment score
            sentiment = self.sentiment_analyzer.analyze_sentiment(position.symbol)
            
            # Calculate unrealized P&L
            unrealized_pl = float(position.unrealized_pl)
            unrealized_plpc = float(position.unrealized_plpc)
            
            position_analysis.append({
                'symbol': position.symbol,
                'quantity': int(position.qty),
                'technical_score': current_score,
                'sentiment_score': sentiment,
                'unrealized_pl': unrealized_pl,
                'unrealized_plpc': unrealized_plpc,
                'current_price': float(position.current_price)
            })
        
        return position_analysis

    async def end_of_day_optimization(self):
        """Optimize portfolio before market close"""
        try:
            # Get current positions analysis
            positions = await self.evaluate_positions()
            
            # Sort positions by combined score (technical + sentiment)
            positions.sort(key=lambda x: (x['technical_score'] * 0.7 + x['sentiment_score'] * 30))
            
            # Identify poor performing positions (bottom 30%)
            num_positions = len(positions)
            positions_to_sell = positions[:int(num_positions * 0.3)]
            
            # Close poor performing positions
            for position in positions_to_sell:
                if position['unrealized_plpc'] < -0.02 or position['sentiment_score'] < -0.2:
                    try:
                        self._api.submit_order(
                            symbol=position['symbol'],
                            qty=position['quantity'],
                            side='sell',
                            type='market',
                            time_in_force='day'
                        )
                        print(f"Closed position in {position['symbol']} due to poor performance/sentiment")
                    except Exception as e:
                        print(f"Error closing position in {position['symbol']}: {str(e)}")
            
            # Keep strong positions for next day
            return [p['symbol'] for p in positions[int(num_positions * 0.3):]]
            
        except Exception as e:
            print(f"Error in end of day optimization: {str(e)}")
            return []

# create our connection to the api and streamconn for our up to date data
trading_client = TradingClient(api_key_id, api_secret, paper=True)
data_client = StockHistoricalDataClient(api_key_id, api_secret)
stream = StockDataStream(api_key_id, api_secret)

# Initialize other components
calc = Calculations(trading_client, data_client)
twilio_client = Client(account_sid, auth_token)
data_queue = multiprocessing.Queue()
data = []
# df_ticker_list = []
ticker_list = []
channels = []

df_ticker_list = {}
data_queue = multiprocessing.Queue()

async def process_stock_data(symbol, bar_data):
    """Process real-time stock data for a given symbol"""
    try:
        if symbol not in df_ticker_list:
            df_ticker_list[symbol] = pd.DataFrame([bar_data])
            df_ticker_list[symbol] = calc.initialize_indicators(df_ticker_list[symbol])
        else:
            df_ticker_list[symbol] = calc.update_indicators(df_ticker_list[symbol], bar_data)
        
        calc.buy_sell_calc(df_ticker_list[symbol], {'symbol': symbol})
    except Exception as e:
        print(f"Error processing data for {symbol}: {str(e)}")

def worker():
    """Worker process for handling stock data"""
    while True:
        item = data_queue.get()
        if item is None:
            break
        symbol, data = item
        asyncio.run(process_stock_data(symbol, data))

def start_workers(num_workers):
    """Start worker processes"""
    workers = []
    for _ in range(num_workers):
        p = multiprocessing.Process(target=worker)
        p.start()
        workers.append(p)
    return workers

def stop_workers(workers):
    """Stop worker processes"""
    for _ in workers:
        data_queue.put(None)
    for w in workers:
        w.join()

async def await_market_open():
    """Wait for market to open"""
    timeList = []
    global df_ticker_list
    global ticker_list

    while True:
        clock = trading_client.get_clock()
        if clock.is_open:
            break
            
        opening_time = clock.next_open.timestamp()
        curr_time = clock.timestamp.timestamp()
        time_to_open = int((opening_time - curr_time) / 60)
        
        if time_to_open not in timeList:
            print(f'{time_to_open} minutes til market open.')
            timeList.clear()
            timeList.append(time_to_open)
            
        # Get data 5 minutes before market open
        if time_to_open == 5:
            try:
                df_ticker_list, ticker_list = await calc.grab_data()
                await setup_streaming(ticker_list)
            except Exception as e:
                print(f'Pre-market data gathering error: {str(e)}')
                twilio_client.messages.create(
                    from_='+13343732933',
                    to='+16207578055',
                    body=f'Pre-market data gathering error: {str(e)}'
                )
                
        await asyncio.sleep(3)

async def setup_streaming(symbols):
    """Setup real-time data streaming"""
    async def handle_bar(bar):
        """Handle incoming bar data"""
        try:
            bar_data = {
                'timestamp': bar.timestamp,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume
            }
            data_queue.put((bar.symbol, bar_data))
        except Exception as e:
            print(f"Error handling bar data: {str(e)}")

    try:
        # Subscribe to minute bar updates for all symbols
        stream.subscribe_bars(handle_bar, *symbols)
        print(f"Subscribed to {len(symbols)} symbols")
    except Exception as e:
        print(f"Error setting up stream: {str(e)}")
        raise

async def check_risk_management():
    """Perform periodic risk management checks"""
    try:
        account = trading_client.get_account()
        equity = float(account.equity)
        initial_equity = float(account.last_equity)
        
        # Check for significant daily losses
        daily_loss_threshold = -0.02
        daily_return = (equity - initial_equity) / initial_equity
        
        if daily_return < daily_loss_threshold:
            print(f'Daily loss threshold exceeded: {daily_return:.2%}')
            # Close all positions
            trading_client.close_all_positions()
            twilio_client.messages.create(
                from_='+13343732933',
                to='+16207578055',
                body=f'Risk Management: Daily loss threshold exceeded ({daily_return:.2%}). All positions closed.'
            )
            
        # Check individual position losses
        positions = trading_client.get_all_positions()
        for position in positions:
            unrealized_plpc = float(position.unrealized_pl_pc)
            if unrealized_plpc < -0.03:
                trading_client.close_position(position.symbol)
                print(f'Closed position {position.symbol} due to excessive loss: {unrealized_plpc:.2%}')
                
    except Exception as e:
        print(f'Risk management check error: {str(e)}')
        twilio_client.messages.create(
            from_='+13343732933',
            to='+16207578055',
            body=f'Risk management check error: {str(e)}'
        )

async def run_trading():
    """Main trading loop"""
    num_workers = multiprocessing.cpu_count()
    workers = start_workers(num_workers)
    
    try:
        while True:
            print('Waiting for market to open...')
            await await_market_open()
            print('Market opened.')
            
            # Send initial equity notification
            account = trading_client.get_account()
            equity = float(account.equity)
            twilio_client.messages.create(
                from_='+13343732933',
                to='+16207578055',
                body=f'Market Open!\nCurrent Equity: ${equity}'
            )
            
            # Trading day loop
            while trading_client.get_clock().is_open:
                try:
                    clock = trading_client.get_clock()
                    closing_time = clock.next_close.timestamp()
                    current_time = clock.timestamp.timestamp()
                    time_to_close = int((closing_time - current_time) / 60)
                    
                    # End of day optimization
                    if time_to_close <= 15:
                        print('Starting end-of-day portfolio optimization...')
                        await calc.end_of_day_optimization()
                        
                        # Send end-of-day notification
                        account = trading_client.get_account()
                        equity = float(account.equity)
                        last_equity = float(account.last_equity)
                        price_change = round(equity - last_equity, 2)
                        
                        twilio_client.messages.create(
                            from_='+13343732933',
                            to='+16207578055',
                            body=f'Market Closing\nFinal Equity: ${equity}\nDay Change: ${price_change}'
                        )
                        break

                    # Initialize streaming if not already done
                    if not ticker_list:
                        df_ticker_list, ticker_list = await calc.grab_data()
                        await setup_streaming(ticker_list)
                    
                    # Market closing procedures
                    if time_to_close <= 5:
                        try:
                            print('Closing all positions...')
                            trading_client.close_all_positions()
                            
                            # Final notification
                            account = trading_client.get_account()
                            equity = float(account.equity)
                            last_equity = float(account.last_equity)
                            price_change = round(equity - last_equity, 2)
                            twilio_client.messages.create(
                                from_='+13343732933',
                                to='+16207578055',
                                body=f'Market Closed\nFinal Equity: ${equity}\nDay Change: ${price_change}'
                            )
                            
                            # Clean up streaming
                            stream.unsubscribe_bars(*ticker_list)
                            break
                                
                        except Exception as e:
                            print(f'Error closing positions: {str(e)}')
                            twilio_client.messages.create(
                                from_='+13343732933',
                                to='+16207578055',
                                body=f'Error closing positions: {str(e)}'
                            )
                    
                    # Regular risk management check
                    if int(time.time()) % 300 == 0:
                        await check_risk_management()
                    
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    print(f'Trading loop error: {str(e)}')
                    print(traceback.format_exc())
                    twilio_client.messages.create(
                        from_='+13343732933',
                        to='+16207578055',
                        body=f'Trading loop error: {str(e)}'
                    )
                    await asyncio.sleep(30)
            
            print('Market Closed')
            
            # Clean up for next trading day
            df_ticker_list.clear()
            ticker_list.clear()
            
            await asyncio.sleep(60)
            
    except Exception as e:
        print(f'Fatal error in run_trading: {str(e)}')
        print(traceback.format_exc())
        twilio_client.messages.create(
            from_='+13343732933',
            to='+16207578055',
            body=f'Fatal error in run_trading: {str(e)}'
        )
    finally:
        stop_workers(workers)
        stream.stop()

if __name__ == '__main__':
    try:
        # Initialize global variables
        df_ticker_list = {}
        ticker_list = []
        
        # Run the trading loop
        asyncio.run(run_trading())
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