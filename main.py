"""Main application entry point"""
import asyncio
import time
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from analysis import Analysis
from config import Config
from trading import TradingSystem
from twilio.rest import Client

async def main():
    # Load configuration
    Config.load_config()
    
    # Initialize clients
    trading_client = TradingClient(Config.API_KEY_ID, Config.API_SECRET, paper=True)
    data_client = StockHistoricalDataClient(Config.API_KEY_ID, Config.API_SECRET)
    stream = StockDataStream(Config.API_KEY_ID, Config.API_SECRET)
    twilio_client = Client(Config.TWILIO_SID, Config.TWILIO_AUTH_TOKEN)
    
    # Initialize analysis and trading system
    analysis = Analysis(Config.NEWS_API_KEY)
    trading_system = TradingSystem(
        trading_client,
        data_client,
        stream,
        analysis,
        twilio_client
    )
    
    while True:
        try:
            print('Waiting for market to open...')
            await trading_system.wait_for_market_open()
            print('Market opened.')
            
            # Send initial equity notification
            account = trading_client.get_account()
            equity = float(account.equity)
            trading_system.notify(f'Market Open!\nCurrent Equity: ${equity}')
            
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
                        await trading_system.optimize_portfolio()
                        
                        # Send end-of-day notification
                        account = trading_client.get_account()
                        equity = float(account.equity)
                        last_equity = float(account.last_equity)
                        price_change = round(equity - last_equity, 2)
                        
                        trading_system.notify(
                            f'Market Closing\nFinal Equity: ${equity}\nDay Change: ${price_change}'
                        )
                        break

                    # Initialize streaming if not already done
                    if not trading_system.ticker_list:
                        universe = await trading_system.get_tradable_universe()
                        trading_system.ticker_list = [ticker['symbol'] for ticker in universe]
                        await trading_system.setup_streaming(trading_system.ticker_list)
                    
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
                            trading_system.notify(
                                f'Market Closed\nFinal Equity: ${equity}\nDay Change: ${price_change}'
                            )
                            
                            # Clean up streaming
                            stream.unsubscribe_bars(*trading_system.ticker_list)
                            break
                                
                        except Exception as e:
                            error_msg = f'Error closing positions: {str(e)}'
                            print(error_msg)
                            trading_system.notify(error_msg)
                    
                    # Regular risk management check
                    if int(time.time()) % 300 == 0:
                        await trading_system.check_risk_management()
                    
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    error_msg = f'Trading loop error: {str(e)}'
                    print(error_msg)
                    trading_system.notify(error_msg)
                    await asyncio.sleep(30)
            
            print('Market Closed')
            
            # Clean up for next trading day
            trading_system.df_ticker_list.clear()
            trading_system.ticker_list.clear()
            
            await asyncio.sleep(60)
            
        except KeyboardInterrupt:
            print('Shutting down gracefully...')
            stream.stop()
            break
        except Exception as e:
            error_msg = f'Fatal error: {str(e)}'
            print(error_msg)
            trading_system.notify(error_msg)
        finally:
            stream.stop()

if __name__ == '__main__':
    asyncio.run(main())