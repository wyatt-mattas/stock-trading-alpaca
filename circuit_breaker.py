import time
from twilio.rest import Client

class CircuitBreaker:
    def __init__(self, trading_client):
        self.trading_client = trading_client
        # Market-wide circuit breaker levels (standard levels used by exchanges)
        self.level1_threshold = -0.07  # 7% decline
        self.level2_threshold = -0.13  # 13% decline
        self.level3_threshold = -0.20  # 20% decline
        
        # Portfolio-specific circuit breakers
        self.portfolio_loss_threshold = -0.05  # 5% portfolio decline
        self.position_loss_threshold = -0.08   # 8% individual position decline
        self.volatility_threshold = 0.03       # 3% price movement in 5 minutes
        
        # Trading limits
        self.max_trades_per_minute = 5
        self.trade_count = 0
        self.last_trade_reset = time.time()
        
        # Tracking
        self.triggered_breakers = set()
        self.breaker_cooldown = {}
        self.cooldown_period = 900  # 15 minutes
        
    async def check_market_circuit_breakers(self, spy_price_change):
        """Check market-wide circuit breakers based on SPY movement"""
        if spy_price_change <= self.level3_threshold:
            return "level3", True
        elif spy_price_change <= self.level2_threshold:
            return "level2", True
        elif spy_price_change <= self.level1_threshold:
            return "level1", True
        return None, False

    async def check_portfolio_circuit_breakers(self):
        """Check portfolio-specific circuit breakers"""
        try:
            account = self.trading_client.get_account()
            equity = float(account.equity)
            last_equity = float(account.last_equity)
            portfolio_return = (equity - last_equity) / last_equity
            
            if portfolio_return <= self.portfolio_loss_threshold:
                return "portfolio", True
            
            # Check individual positions
            positions = self.trading_client.get_all_positions()
            for position in positions:
                unrealized_plpc = float(position.unrealized_plpc)
                if unrealized_plpc <= self.position_loss_threshold:
                    return f"position_{position.symbol}", True
                    
            return None, False
            
        except Exception as e:
            print(f"Error checking portfolio circuit breakers: {str(e)}")
            return "error", True

    async def check_volatility_circuit_breaker(self, symbol, current_price, price_history):
        """Check for excessive volatility in a symbol"""
        try:
            if len(price_history) >= 5:  # Need at least 5 minutes of data
                five_min_change = abs((current_price - price_history[-5]) / price_history[-5])
                if five_min_change >= self.volatility_threshold:
                    return f"volatility_{symbol}", True
            return None, False
            
        except Exception as e:
            print(f"Error checking volatility circuit breaker: {str(e)}")
            return None, False

    async def can_trade(self):
        """Check if trading is allowed based on frequency limits"""
        current_time = time.time()
        
        # Reset counter every minute
        if current_time - self.last_trade_reset >= 60:
            self.trade_count = 0
            self.last_trade_reset = current_time
            
        if self.trade_count >= self.max_trades_per_minute:
            return False
            
        self.trade_count += 1
        return True

    async def handle_circuit_breaker(self, breaker_type):
        """Handle circuit breaker activation"""
        try:
            # Check cooldown period
            current_time = time.time()
            if breaker_type in self.breaker_cooldown:
                if current_time - self.breaker_cooldown[breaker_type] < self.cooldown_period:
                    return  # Still in cooldown
                    
            self.triggered_breakers.add(breaker_type)
            self.breaker_cooldown[breaker_type] = current_time
            
            # Different actions based on breaker type
            if breaker_type.startswith("level"):
                await self.handle_market_breaker(breaker_type)
            elif breaker_type == "portfolio":
                await self.handle_portfolio_breaker()
            elif breaker_type.startswith("position"):
                symbol = breaker_type.split("_")[1]
                await self.handle_position_breaker(symbol)
            elif breaker_type.startswith("volatility"):
                symbol = breaker_type.split("_")[1]
                await self.handle_volatility_breaker(symbol)
                
        except Exception as e:
            print(f"Error handling circuit breaker: {str(e)}")
            
    async def handle_market_breaker(self, level, twilio_client):
        """Handle market-wide circuit breaker"""
        try:
            message = f"Market circuit breaker {level} triggered. Closing all positions."
            print(message)
            
            # Close all positions
            self.trading_client.close_all_positions()
            
            # Send notification
            twilio_client.messages.create(
                from_='+13343732933',
                to='+16207578055',
                body=message
            )
            
        except Exception as e:
            print(f"Error handling market breaker: {str(e)}")

    async def handle_portfolio_breaker(self, twilio_client):
        """Handle portfolio-wide circuit breaker"""
        try:
            message = "Portfolio circuit breaker triggered. Reducing exposure."
            print(message)
            
            # Close worst performing positions
            positions = self.trading_client.get_all_positions()
            positions.sort(key=lambda x: float(x.unrealized_plpc))
            
            # Close bottom 50% of positions
            for position in positions[:len(positions)//2]:
                self.trading_client.close_position(position.symbol)
                
            twilio_client.messages.create(
                from_='+13343732933',
                to='+16207578055',
                body=message
            )
            
        except Exception as e:
            print(f"Error handling portfolio breaker: {str(e)}")

    async def handle_position_breaker(self, symbol, twilio_client):
        """Handle individual position circuit breaker"""
        try:
            message = f"Position circuit breaker triggered for {symbol}. Closing position."
            print(message)
            
            self.trading_client.close_position(symbol)
            
            twilio_client.messages.create(
                from_='+13343732933',
                to='+16207578055',
                body=message
            )
            
        except Exception as e:
            print(f"Error handling position breaker: {str(e)}")

    async def handle_volatility_breaker(self, symbol, twilio_client):
        """Handle volatility circuit breaker"""
        try:
            message = f"Volatility circuit breaker triggered for {symbol}. Pausing trading."
            print(message)
            
            # Add to cooldown for longer period
            self.breaker_cooldown[f"volatility_{symbol}"] = time.time() + 1800  # 30 minute cooldown
            
            twilio_client.messages.create(
                from_='+13343732933',
                to='+16207578055',
                body=message
            )
            
        except Exception as e:
            print(f"Error handling volatility breaker: {str(e)}")