# error_recovery.py
"""Enhanced error recovery and resilience system"""
import asyncio
import time
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

class ErrorSeverity(Enum):
    LOW = 1      # Recoverable without intervention
    MEDIUM = 2   # Requires retry with modified parameters
    HIGH = 3     # Requires system pause and reset
    CRITICAL = 4 # Requires complete shutdown and restart

@dataclass
class ErrorContext:
    error_type: str
    component: str
    timestamp: float
    details: Dict[str, Any]
    attempts: int = 0

class ErrorRecovery:
    def __init__(self, trading_client, data_client, stream_client):
        self.trading_client = trading_client
        self.data_client = data_client
        self.stream_client = stream_client
        
        # Error tracking
        self.error_history = {}
        self.error_counts = {}
        self.error_cooldown = {}
        
        # Recovery settings
        self.max_retries = 3
        self.base_delay = 5  # seconds
        self.cooldown_period = 300  # 5 minutes
        self.error_window = 3600  # 1 hour for error rate calculation
        
        # Circuit breaker settings
        self.error_threshold = 5  # errors per hour
        self.circuit_breaker_cooldown = 900  # 15 minutes
        
    async def handle_error(self, error: Exception, context: Dict[str, Any]) -> bool:
        """Main error handling entry point"""
        try:
            error_type = type(error).__name__
            component = context.get('component', 'unknown')
            
            # Create error context
            error_ctx = ErrorContext(
                error_type=error_type,
                component=component,
                timestamp=time.time(),
                details=context
            )
            
            # Update error history
            self.update_error_history(error_ctx)
            
            # Check error rate and circuit breakers
            if not await self.check_error_rate(error_type):
                return False
                
            # Determine error severity
            severity = self.classify_error(error, context)
            
            # Handle based on severity
            if severity == ErrorSeverity.LOW:
                return await self.handle_low_severity(error_ctx)
            elif severity == ErrorSeverity.MEDIUM:
                return await self.handle_medium_severity(error_ctx)
            elif severity == ErrorSeverity.HIGH:
                return await self.handle_high_severity(error_ctx)
            else:  # CRITICAL
                return await self.handle_critical_error(error_ctx)
                
        except Exception as e:
            print(f"Error in error recovery system: {str(e)}")
            return False

    def classify_error(self, error: Exception, context: Dict[str, Any]) -> ErrorSeverity:
        """Classify error severity based on type and context"""
        if isinstance(error, (ConnectionError, TimeoutError)):
            return ErrorSeverity.MEDIUM
        elif "rate limit" in str(error).lower():
            return ErrorSeverity.LOW
        elif "insufficient funds" in str(error).lower():
            return ErrorSeverity.HIGH
        elif "invalid symbol" in str(error).lower():
            return ErrorSeverity.LOW
        elif "account blocked" in str(error).lower():
            return ErrorSeverity.CRITICAL
        elif context.get('component') == 'stream':
            return ErrorSeverity.HIGH
        else:
            return ErrorSeverity.MEDIUM

    async def handle_low_severity(self, error_ctx: ErrorContext) -> bool:
        """Handle low severity errors with simple retry"""
        if error_ctx.attempts >= self.max_retries:
            return False
            
        delay = self.base_delay * (2 ** error_ctx.attempts)
        await asyncio.sleep(delay)
        error_ctx.attempts += 1
        return True

    async def handle_medium_severity(self, error_ctx: ErrorContext) -> bool:
        """Handle medium severity errors with recovery actions"""
        try:
            if error_ctx.attempts >= self.max_retries:
                return False
                
            # Attempt recovery based on component
            if error_ctx.component == 'data':
                await self.recover_data_client()
            elif error_ctx.component == 'stream':
                await self.recover_stream()
            elif error_ctx.component == 'trading':
                await self.recover_trading_client()
                
            delay = self.base_delay * (2 ** error_ctx.attempts)
            await asyncio.sleep(delay)
            error_ctx.attempts += 1
            return True
            
        except Exception as e:
            print(f"Error in medium severity recovery: {str(e)}")
            return False

    async def handle_high_severity(self, error_ctx: ErrorContext) -> bool:
        """Handle high severity errors with system pause"""
        try:
            # Close all positions if trading-related
            if error_ctx.component == 'trading':
                await self.close_all_positions()
            
            # Reset connections
            await self.reset_connections()
            
            # Cooldown period
            await asyncio.sleep(self.circuit_breaker_cooldown)
            
            return error_ctx.attempts < 1  # Only try once
            
        except Exception as e:
            print(f"Error in high severity recovery: {str(e)}")
            return False

    async def handle_critical_error(self, error_ctx: ErrorContext) -> bool:
        """Handle critical errors requiring shutdown"""
        try:
            # Emergency position closing
            await self.close_all_positions()
            
            # Stop all streams and connections
            await self.shutdown_connections()
            
            # Signal for complete restart
            raise SystemExit("Critical error triggered shutdown")
            
        except Exception as e:
            print(f"Error in critical error handling: {str(e)}")
            return False

    async def recover_data_client(self) -> bool:
        """Attempt to recover data client connection"""
        try:
            # Reset data client connection
            self.data_client = type(self.data_client)(
                self.data_client.api_key,
                self.data_client.secret_key
            )
            return True
        except Exception as e:
            print(f"Error recovering data client: {str(e)}")
            return False

    async def recover_stream(self) -> bool:
        """Attempt to recover streaming connection"""
        try:
            # Stop existing stream
            self.stream_client.stop()
            await asyncio.sleep(1)
            
            # Create new stream connection
            self.stream_client = type(self.stream_client)(
                self.stream_client.api_key,
                self.stream_client.secret_key
            )
            return True
        except Exception as e:
            print(f"Error recovering stream: {str(e)}")
            return False

    async def recover_trading_client(self) -> bool:
        """Attempt to recover trading client connection"""
        try:
            # Reset trading client connection
            self.trading_client = type(self.trading_client)(
                self.trading_client.api_key,
                self.trading_client.secret_key
            )
            return True
        except Exception as e:
            print(f"Error recovering trading client: {str(e)}")
            return False

    async def close_all_positions(self) -> bool:
        """Emergency close all positions"""
        try:
            return bool(self.trading_client.close_all_positions())
        except Exception as e:
            print(f"Error closing positions: {str(e)}")
            return False

    async def reset_connections(self) -> None:
        """Reset all client connections"""
        await asyncio.gather(
            self.recover_data_client(),
            self.recover_stream(),
            self.recover_trading_client()
        )

    async def shutdown_connections(self) -> None:
        """Shutdown all connections"""
        try:
            self.stream_client.stop()
            # Add any additional cleanup needed
        except Exception as e:
            print(f"Error in shutdown: {str(e)}")

    def update_error_history(self, error_ctx: ErrorContext) -> None:
        """Update error tracking history"""
        current_time = time.time()
        error_type = error_ctx.error_type
        
        if error_type not in self.error_history:
            self.error_history[error_type] = []
        
        self.error_history[error_type].append(current_time)
        
        # Clean old errors
        self.error_history[error_type] = [
            t for t in self.error_history[error_type]
            if current_time - t <= self.error_window
        ]

    async def check_error_rate(self, error_type: str) -> bool:
        """Check if error rate exceeds threshold"""
        current_time = time.time()
        
        if error_type in self.error_cooldown:
            if current_time < self.error_cooldown[error_type]:
                return False
                
        error_count = len(self.error_history.get(error_type, []))
        
        if error_count >= self.error_threshold:
            self.error_cooldown[error_type] = current_time + self.circuit_breaker_cooldown
            return False
            
        return True