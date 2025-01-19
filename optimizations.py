"""Performance optimizations for trading system"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import pandas as pd
import numpy as np
from collections import deque
from typing import Dict, List, Set
import time

class PerformanceOptimizations:
    def __init__(self):
        # Create thread pool for CPU-intensive operations
        self.thread_pool = ThreadPoolExecutor(max_workers=4)
        
        # Caching and data structures
        self.price_cache = {}
        self.sentiment_cache = {}
        self.indicator_cache = {}
        self.data_buffer = {}
        self.active_symbols: Set[str] = set()
        
        # Use deque for fixed-size price history (more efficient than list)
        self.price_history: Dict[str, deque] = {}
        self.MAX_HISTORY = 100
        
        # Batch processing settings
        self.batch_size = 50
        self.batch_buffer = []
        self.last_batch_time = time.time()
        
    def initialize_symbol(self, symbol: str) -> None:
        """Initialize data structures for a new symbol"""
        if symbol not in self.price_history:
            self.price_history[symbol] = deque(maxlen=self.MAX_HISTORY)
            self.data_buffer[symbol] = []
            self.active_symbols.add(symbol)

    async def process_data_batch(self, batch_data: List[dict]) -> None:
        """Process multiple data points in parallel"""
        tasks = []
        for data in batch_data:
            symbol = data['symbol']
            self.initialize_symbol(symbol)
            tasks.append(self.process_single_data(symbol, data))
        
        await asyncio.gather(*tasks)
        
    async def process_single_data(self, symbol: str, data: dict) -> None:
        """Process single data point with optimizations"""
        # Update price history efficiently
        self.price_history[symbol].append(data['price'])
        
        # Only calculate indicators if enough data
        if len(self.price_history[symbol]) >= 20:  # Minimum required for most indicators
            loop = asyncio.get_event_loop()
            # Run CPU-intensive calculations in thread pool
            indicators = await loop.run_in_executor(
                self.thread_pool,
                self.calculate_indicators,
                symbol,
                list(self.price_history[symbol])
            )
            self.indicator_cache[symbol] = indicators

    @lru_cache(maxsize=1000)
    def calculate_indicators(self, symbol: str, prices: tuple) -> dict:
        """Calculate technical indicators with caching"""
        prices_array = np.array(prices)
        return {
            'sma': np.mean(prices_array[-20:]),
            'std': np.std(prices_array[-20:]),
            'momentum': prices_array[-1] - prices_array[-20] if len(prices_array) >= 20 else 0
        }

    def optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame for better performance"""
        # Use smaller data types where possible
        df['volume'] = df['volume'].astype('int32')
        df['price'] = df['price'].astype('float32')
        
        # Drop unnecessary columns
        necessary_columns = ['timestamp', 'price', 'volume']
        df = df[necessary_columns]
        
        # Sort once and set index for faster operations
        df.sort_values('timestamp', inplace=True)
        df.set_index('timestamp', inplace=True)
        
        return df

    async def batch_process_stream(self, data):
        """Process streaming data in batches"""
        self.batch_buffer.append(data)
        current_time = time.time()
        
        # Process batch if size threshold or time threshold reached
        if (len(self.batch_buffer) >= self.batch_size or 
            current_time - self.last_batch_time >= 1.0):  # 1 second time threshold
            
            await self.process_data_batch(self.batch_buffer)
            self.batch_buffer = []
            self.last_batch_time = current_time

class OptimizedAnalysis:
    def __init__(self):
        self.perf = PerformanceOptimizations()
        self._cached_data = {}
        self.update_interval = 60  # Update cache every 60 seconds
        self.last_update = {}
    
    @lru_cache(maxsize=1000)
    def calculate_score(self, technical_data: tuple, sentiment: float) -> float:
        """Optimized score calculation with caching"""
        return 0.7 * sum(technical_data) + 0.3 * sentiment

    async def process_market_data(self, symbols: List[str]) -> None:
        """Process market data in parallel"""
        chunks = [symbols[i:i + self.perf.batch_size] 
                 for i in range(0, len(symbols), self.perf.batch_size)]
        
        for chunk in chunks:
            tasks = [self.process_symbol(symbol) for symbol in chunk]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.1)  # Rate limiting

    async def process_symbol(self, symbol: str) -> None:
        """Process single symbol with optimizations"""
        current_time = time.time()
        
        # Check if cached data is still valid
        if (symbol in self._cached_data and 
            current_time - self.last_update.get(symbol, 0) < self.update_interval):
            return self._cached_data[symbol]
        
        # Process new data
        try:
            technical_data = await self.get_technical_data(symbol)
            sentiment = await self.get_cached_sentiment(symbol)
            
            result = {
                'technical': technical_data,
                'sentiment': sentiment,
                'score': self.calculate_score(
                    tuple(technical_data.values()),
                    sentiment
                )
            }
            
            self._cached_data[symbol] = result
            self.last_update[symbol] = current_time
            
            return result
            
        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
            return None

    @lru_cache(maxsize=100)
    async def get_cached_sentiment(self, symbol: str) -> float:
        """Get cached sentiment with periodic updates"""
        current_time = time.time()
        if (symbol not in self.perf.sentiment_cache or 
            current_time - self.perf.sentiment_cache[symbol]['time'] > 3600):
            # Update sentiment every hour
            sentiment = await self.calculate_sentiment(symbol)
            self.perf.sentiment_cache[symbol] = {
                'value': sentiment,
                'time': current_time
            }
        return self.perf.sentiment_cache[symbol]['value']

class OptimizedPortfolio:
    def __init__(self, trading_client):
        self.trading_client = trading_client
        self.position_cache = {}
        self.cache_duration = 5  # Cache positions for 5 seconds
        self.last_cache_update = 0
        
    async def get_positions(self) -> dict:
        """Get positions with caching"""
        current_time = time.time()
        if (current_time - self.last_cache_update > self.cache_duration):
            self.position_cache = {
                p.symbol: p for p in self.trading_client.get_all_positions()
            }
            self.last_cache_update = current_time
        return self.position_cache

    async def optimize_end_of_day(self, analysis: OptimizedAnalysis) -> None:
        """Optimized end-of-day portfolio analysis"""
        positions = await self.get_positions()
        
        # Process all positions in parallel
        tasks = [
            analysis.process_symbol(symbol) 
            for symbol in positions.keys()
        ]
        results = await asyncio.gather(*tasks)
        
        # Process results in memory
        decisions = self.analyze_position_results(positions, results)
        
        # Execute trades in parallel
        trade_tasks = [
            self.execute_decision(symbol, decision)
            for symbol, decision in decisions.items()
        ]
        await asyncio.gather(*trade_tasks)

    def analyze_position_results(self, positions: dict, results: List[dict]) -> dict:
        """Analyze results in memory for better performance"""
        decisions = {}
        for symbol, result in zip(positions.keys(), results):
            if result and result['score'] < 50:
                decisions[symbol] = 'close'
            else:
                decisions[symbol] = 'hold'
        return decisions