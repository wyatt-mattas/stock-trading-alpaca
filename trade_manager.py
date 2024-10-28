"""Trade execution and management"""
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from error_recovery import ErrorRecovery
from circuit_breaker import CircuitBreaker
from config import Config

class TradeManager:
    def __init__(self, trading_client, data_client):
        self.trading_client = trading_client
        self.data_client = data_client
        self.error_recovery = ErrorRecovery(trading_client, data_client)
        self.circuit_breaker = CircuitBreaker(trading_client)
        
    async def submit_order(self, order_data, context=None):
        try:
            # Check circuit breakers
            if not await self.circuit_breaker.can_trade():
                return None
                
            order = self.trading_client.submit_order(order_data)
            print(f'Market order of {order_data.qty} {order_data.symbol} {order_data.side} completed.')
            return order
            
        except Exception as e:
            error_context = {'order_data': order_data, **(context or {})}
            if await self.error_recovery.handle_error(e, error_context):
                return await self.submit_order(order_data, context)
            return None

    def calc_position_size(self, current_price):
        try:
            portfolio_value = float(self.trading_client.get_account().buying_power)
            rough_number = (portfolio_value * Config.RISK_PERCENTAGE) / current_price
            quantity = max(1, round(rough_number))
            return quantity
        except Exception as e:
            print(f"Error calculating position size: {str(e)}")
            return 1