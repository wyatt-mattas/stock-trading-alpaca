import asyncio
import time

class ErrorRecovery:
    def __init__(self, trading_client, data_client):
        self.trading_client = trading_client
        self.data_client = data_client
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        self.error_counts = {}
        self.error_cooldown = {}
        self.cooldown_period = 300  # 5 minutes
        
    async def handle_error(self, error, context):
        """Handle different types of errors with appropriate recovery strategies"""
        try:
            error_type = type(error).__name__
            
            # Update error counts
            self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
            
            # Check if in cooldown
            current_time = time.time()
            if error_type in self.error_cooldown:
                if current_time < self.error_cooldown[error_type]:
                    return False
                    
            # Handle specific error types
            if isinstance(error, (ConnectionError, TimeoutError)):
                return await self.handle_connection_error(error, context)
            elif isinstance(error, RateLimitError): # TODO add RateLimitError
                return await self.handle_rate_limit_error(error, context)
            elif isinstance(error, AccountError): # TODO add AccountError
                return await self.handle_account_error(error, context)
            elif isinstance(error, OrderError): # TODO add OrderError
                return await self.handle_order_error(error, context)
            else:
                return await self.handle_generic_error(error, context)
                
        except Exception as e:
            print(f"Error in error recovery: {str(e)}")
            return False

    async def handle_connection_error(self, error, context):
        """Handle connection-related errors"""
        for attempt in range(self.max_retries):
            try:
                print(f"Connection error recovery attempt {attempt + 1}/{self.max_retries}")
                
                # Wait before retry
                await asyncio.sleep(self.retry_delay * (attempt + 1))
                
                # Attempt to reconnect
                if context.get('stream'):
                    await self.reconnect_stream(context['stream'])
                if context.get('data_client'):
                    await self.reconnect_data_client()
                    
                return True
                
            except Exception as e:
                print(f"Error during connection recovery: {str(e)}")
                
        # If all retries failed, enter cooldown
        self.error_cooldown[type(error).__name__] = time.time() + self.cooldown_period
        return False

    async def handle_rate_limit_error(self, error, context):
        """Handle rate limit errors"""
        try:
            # Calculate appropriate backoff time
            backoff_time = min(60, self.retry_delay * (2 ** self.error_counts.get('RateLimitError', 0)))
            print(f"Rate limit reached. Backing off for {backoff_time} seconds")
            
            await asyncio.sleep(backoff_time)
            return True
            
        except Exception as e:
            print(f"Error handling rate limit: {str(e)}")
            return False

    async def handle_account_error(self, twilio_client):
        """Handle account-related errors"""
        try:
            print("Account error detected. Verifying account status...")
            
            # Verify account status
            account = self.trading_client.get_account()
            
            if account.trading_suspended:
                message = "Trading suspended. Closing all positions."
                print(message)
                self.trading_client.close_all_positions()
                
                twilio_client.messages.create(
                    from_='+13343732933',
                    to='+16207578055',
                    body=message
                )
                return False
                
            return True
            
        except Exception as e:
            print(f"Error handling account error: {str(e)}")
            return False

    async def handle_order_error(self, error, context, df_ticker_list, ticker_list):
        """Handle order-related errors"""
        try:
            order_data = context.get('order_data')
            if not order_data:
                return False
                
            # Retry with modified order
            if "insufficient buying power" in str(error).lower():
                # Reduce order size by 25%
                order_data.qty = int(order_data.qty * 0.75)
                if order_data.qty > 0:
                    self.trading_client.submit_order(order_data)
                    return True
                    
            elif "invalid symbol" in str(error).lower():
                # Remove symbol from tracking
                symbol = order_data.symbol
                if symbol in df_ticker_list:
                    del df_ticker_list[symbol]
                if symbol in ticker_list:
                    ticker_list.remove(symbol)
                    
            return False
            
        except Exception as e:
            print(f"Error handling order error: {str(e)}")
            return False

    async def handle_generic_error(self, error, context):
        """Handle unknown errors"""
        try:
            print(f"Unhandled error: {str(error)}")
            
            # If error count is too high, enter cooldown
            if self.error_counts.get(type(error).__name__, 0) >= self.max_retries:
                self.error_cooldown[type(error).__name__] = time.time() + self.cooldown_period
                return False
                
            # Wait before retry
            await asyncio.sleep(self.retry_delay)
            return True
            
        except Exception as e:
            print(f"Error handling generic error: {str(e)}")
            return False

    async def reconnect_stream(self, stream):
        """Attempt to reconnect streaming connection"""
        try:
            stream.stop()
            await asyncio.sleep(1)
            stream.run()
            return True
        except Exception as e:
            print(f"Error reconnecting stream: {str(e)}")
            return False

    async def reconnect_data_client(self, data_client):
        """Attempt to reconnect data client"""
        try:
            self.data_client = data_client
            return True
        except Exception as e:
            print(f"Error reconnecting data client: {str(e)}")
            return False