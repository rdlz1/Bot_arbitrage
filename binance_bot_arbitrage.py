
import os
import math
import asyncio
import logging
from dotenv import load_dotenv
from typing import List, Tuple, Dict
from binance.client import Client
from binance.exceptions import BinanceAPIException
from binance_websocket import BinanceWebSocketClient

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('arbitrage.log', mode='a')
    ]
)

# Load environment variables
load_dotenv('.env_template')

pairs_env = os.getenv("BINANCE_SYMBOL")
if not pairs_env:
    raise ValueError("BINANCE_SYMBOL environment variable is not set.")
pairs = pairs_env.split(',')

api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

if not api_key or not api_secret:
    raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET must be set in the environment.")

# Initialize the Binance REST client
binance_client = Client(api_key, api_secret, testnet=True)

RATE_LIMIT = int(os.getenv("RATE_LIMIT"))
rate_limit_semaphore = asyncio.Semaphore(RATE_LIMIT)

DEBOUNCE_SECONDS = float(os.getenv("DEBOUNCE_SECONDS"))
PRICE_DIFFERENCE_THRESHOLD = float(os.getenv("PRICE_DIFFERENCE_THRESHOLD"))
STABLECOIN_AMOUNT= float(os.getenv("STABLECOIN_AMOUNT"))

last_prices: Dict[str, float] = {}
arbitrage_in_progress = False
last_arbitrage_time = 0.0

async def handle_trade_update_async(data_queue: asyncio.Queue):
    while True:
        data = await data_queue.get()
        try:
            symbol = data['data']['s']
            price = float(data['data']['p'])
            last_prices[symbol] = price

            # Compare all stored prices against each other but queue possible opportunities
            symbols_list = list(last_prices.keys())
            for i in range(len(symbols_list)):
                for j in range(i + 1, len(symbols_list)):
                    sym1, sym2 = symbols_list[i], symbols_list[j]
                    p1, p2 = last_prices[sym1], last_prices[sym2]
                    diff = abs(p1 - p2) / ((p1 + p2) / 2)
                    if diff >= PRICE_DIFFERENCE_THRESHOLD:
                        await execute_arbitrage(sym1, sym2, p1, p2, diff)
        except asyncio.CancelledError:
            logging.info("Trade update handler has been cancelled.")
            break
        except KeyError as e:
            logging.error(f"Missing key in data: {e}")
        except Exception as e:
            logging.error(f"Error handling trade update: {e}")

def get_balance(asset: str) -> float:
    try:
        account_info = binance_client.get_account()
        balances = account_info['balances']
        for balance in balances:
            if balance['asset'] == asset:
                free_amount = float(balance['free'])
                return free_amount
        logging.error(f"Asset {asset} not found in account balances.")
        return 0.0
    except BinanceAPIException as e:
        logging.error(f"Binance API Exception while fetching balance for {asset}: {e}")
        return 0.0
    except Exception as e:
        logging.error(f"Unexpected error while fetching balance for {asset}: {e}")
        return 0.0

async def execute_arbitrage(sym1: str, sym2: str, p1: float, p2: float, diff: float):
    global arbitrage_in_progress, last_arbitrage_time
    current_time = asyncio.get_event_loop().time()

    if not arbitrage_in_progress and (current_time - last_arbitrage_time) >= DEBOUNCE_SECONDS:
        arbitrage_in_progress = True
        last_arbitrage_time = current_time
        logging.info(f"***************************************** \n Executing arbitrage between {sym1} and {sym2} (diff={diff:.2%})")

        try:
            usdt_balance = get_balance("USDT")
            if usdt_balance < 10.0:
                logging.warning("Insufficient USDT balance to execute arbitrage.")
                arbitrage_in_progress = False
                return

            # Buy sym1
            buy_order = await place_order(sym1, 'BUY', STABLECOIN_AMOUNT)
            if buy_order:
                await track_order(buy_order)

            # Buy sym2
            sell_order = await place_order(sym2, 'SELL', STABLECOIN_AMOUNT)
            if sell_order:
                await track_order(sell_order)

            if buy_order and sell_order:
                logging.info(f"Arbitrage executed successfully between {sym1} and {sym2}.")
            else:
                logging.error("Failed to place one or both orders; skipping arbitrage.")
        except Exception as e:
            logging.error(f"Error executing arbitrage: {e}")
        finally:
            arbitrage_in_progress = False

async def track_order(order_response: Dict):
    try:
        order_id = order_response.get('orderId')
        symbol = order_response.get('symbol')
        while True:
            order_status = binance_client.get_order(symbol=symbol, orderId=order_id)
            if order_status['status'] == 'FILLED':
                logging.info(f"Order {order_id} for {symbol} filled.")
                break
            elif order_status['status'] in ['CANCELED', 'REJECTED']:
                logging.warning(f"Order {order_id} for {symbol} {order_status['status']}.")
                break
            await asyncio.sleep(1)  # Wait before polling again
    except asyncio.CancelledError:
        logging.info(f"Order tracking for {order_id} has been cancelled.")
    except Exception as e:
        logging.error(f"Error tracking order {order_id} for {symbol}: {e}")

async def place_order(symbol: str, side: str, usdt_amount: float) -> Dict:
    async with rate_limit_semaphore:
        try:
            # Fetch current price
            price_data = binance_client.get_symbol_ticker(symbol=symbol.upper())
            if 'price' not in price_data:
                logging.error(f"Unable to fetch price for {symbol}.")
                return {}
            current_price = float(price_data['price'])

            # Calculate quantity to trade
            quantity = usdt_amount / current_price

            # Get lot size and step size
            min_qty, step_size = get_lot_size(symbol, is_futures=False)
            if min_qty is None or step_size is None:
                logging.error(f"Cannot determine lot size for {symbol}.")
                return {}

            # Adjust quantity to comply with step size
            quantity = adjust_to_step_size(quantity, step_size)
            if quantity < min_qty:
                logging.warning(f"Calculated quantity {quantity} for {symbol} is below the minimum lot size {min_qty}.")
                return {}

            # Place the market order
            order = binance_client.create_order(
                symbol=symbol.upper(),
                side=side.upper(),
                type='MARKET',
                quantity=quantity
            )
            logging.info(f"{side.upper()} order placed for {symbol}: {quantity} units at price ~ {current_price}")
            logging.info(f"{order['symbol']} order response: {order['fills']} ")
            return order
        except BinanceAPIException as e:
            if e.status_code == 429:
                # Rate limit hit; implement exponential backoff
                logging.warning("Rate limit exceeded. Backing off for 60 seconds.")
                await asyncio.sleep(60)
                return await place_order(symbol, side, usdt_amount)
            else:
                logging.error(f"Binance API Exception while placing {side} order for {symbol}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error while placing {side} order for {symbol}: {e}")
        return {}

def get_lot_size(symbol: str, is_futures: bool) -> Tuple[float, float]:
    try:
        exchange_info = binance_client.futures_exchange_info() if is_futures else binance_client.get_exchange_info()
        if exchange_info is None:
            logging.error(f"Exchange info not found for symbol {symbol}.")
            return None, None

        for s in exchange_info['symbols']:
            if s['symbol'] == symbol.upper():
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        min_qty = float(f['minQty'])
                        step_size = float(f['stepSize'])
                        return min_qty, step_size
        logging.error(f"LOT_SIZE filter not found for symbol {symbol}.")
    except BinanceAPIException as e:
        logging.error(f"Binance API Exception while fetching exchange info for {symbol}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while fetching exchange info for {symbol}: {e}")
    return None, None

def adjust_to_step_size(quantity: float, step_size: float) -> float:
    try:
        precision = int(round(-math.log(step_size, 10), 0))
        return round(quantity, precision)
    except ValueError as e:
        logging.error(f"Error calculating precision for step size {step_size}: {e}")
        return 0.0
    except Exception as e:
        logging.error(f"Unexpected error adjusting to step size: {e}")
        return 0.0

async def main():
    data_queue = asyncio.Queue()
    
    # Initialize the WebSocket client with the data queue
    wsclient = BinanceWebSocketClient(pairs, data_queue)
    
    # Create asynchronous tasks
    ws_task = asyncio.create_task(wsclient.connect())
    consumer_task = asyncio.create_task(handle_trade_update_async(data_queue))
    
    try:
        # Wait for both tasks to complete
        await asyncio.gather(ws_task, consumer_task)
    except asyncio.CancelledError:
        # Handle task cancellation if necessary
        logging.info("Tasks have been cancelled.")
    except KeyboardInterrupt:
        # Handle Ctrl+C interruption
        logging.info("Received exit signal (Ctrl+C). Shutting down...")
    finally:
        # Initiate shutdown sequence
        ws_task.cancel()
        consumer_task.cancel()
        
        # Attempt to gracefully stop the WebSocket client
        await wsclient.stop()
        
        # Await the cancellation of tasks
        await asyncio.gather(ws_task, consumer_task, return_exceptions=True)
        
        logging.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Program terminated by user.")