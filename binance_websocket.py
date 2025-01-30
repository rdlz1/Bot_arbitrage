import asyncio
import json
import websockets
import logging
from dotenv import load_dotenv
import os
from typing import Callable, Any, List

# Load environment variables from .env file
load_dotenv('.env_template')

pairs_env = os.getenv("BINANCE_SYMBOL")
if not pairs_env:
    raise ValueError("BINANCE_SYMBOL environment variable is not set.")
pairs = pairs_env.split(',')

class BinanceWebSocketClient:
    def __init__(self, symbols: List[str], data_queue: asyncio.Queue):
        self.symbols = [symbol.lower() for symbol in symbols]
        self.data_queue = data_queue
        streams = '/'.join([f"{symbol}@trade" for symbol in self.symbols])
        self.ws_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        self.running = True
        self.websocket = None  # To keep track of the WebSocket connection

    async def connect(self):
        while self.running:
            try:
                async with websockets.connect(self.ws_url) as websocket:
                    self.websocket = websocket
                    logging.info(f"Connected to {self.ws_url}")
                    async for message in websocket:
                        data = json.loads(message)
                        await self.data_queue.put(data)  # Correct usage
            except asyncio.CancelledError:
                # Handle task cancellation
                logging.info("WebSocket connection task cancelled.")
                break
            except Exception as e:
                logging.error(f"Connection error: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    async def stop(self):
        self.running = False
        if self.websocket:
            await self.websocket.close()
            logging.info("WebSocket connection closed.")

    def start(self) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connect())

def handle_trade_update(data: dict):
    pass  # This should be overridden by the main script's callback

if __name__ == "__main__":
    client = BinanceWebSocketClient(pairs, handle_trade_update)
    try:
        client.start()
    except KeyboardInterrupt:
        client.stop()