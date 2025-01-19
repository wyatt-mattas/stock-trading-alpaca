"""Circuit breaker functionality for risk management"""
import time

class CircuitBreaker:
    def __init__(self, trading_client):
        self.trading_client = trading_client
        # Market-wide circuit breaker levels
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
        
        # Circuit breaker state
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
            if len(price_history) >= 5:
                five_min_change = abs((current_price - price_history[-5]) / price_history[-5])
                if five_min_change >= self.volatility_threshold:
                    return f"volatility_{symbol}", True
            return None, False
            
        except Exception as e:
            print(f"Error checking volatility circuit breaker: {str(e)}")
            return None, False

    async def can_trade(self):
        """Check if trading is allowed based on all circuit breakers"""
        current_time = time.time()
        
        # Reset trade counter every minute
        if current_time - self.last_trade_reset >= 60:
            self.trade_count = 0
            self.last_trade_reset = current_time
            
        if self.trade_count >= self.max_trades_per_minute:
            return False
            
        # Check cooldown periods
        for breaker_type, cooldown_time in list(self.breaker_cooldown.items()):
            if current_time >= cooldown_time:
                del self.breaker_cooldown[breaker_type]
                self.triggered_breakers.discard(breaker_type)
                
        if self.triggered_breakers:
            return False
            
        self.trade_count += 1
        return True

    def handle_breaker_trigger(self, breaker_type, notification_callback):
        """Handle circuit breaker activation"""
        current_time = time.time()
        self.triggered_breakers.add(breaker_type)
        self.breaker_cooldown[breaker_type] = current_time + self.cooldown_period
        
        messages = {
            "level1": "Market circuit breaker Level 1 triggered. Pausing trading.",
            "level2": "Market circuit breaker Level 2 triggered. Closing all positions.",
            "level3": "Market circuit breaker Level 3 triggered. Market closed for the day.",
            "portfolio": "Portfolio circuit breaker triggered. Reducing exposure.",
        }
        
        message = messages.get(breaker_type, f"Circuit breaker triggered: {breaker_type}")
        notification_callback(message)
        
        if breaker_type in ["level2", "level3", "portfolio"]:
            self.trading_client.close_all_positions()
        elif breaker_type.startswith("position_"):
            symbol = breaker_type.split("_")[1]
            self.trading_client.close_position(symbol)