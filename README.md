# Binance Arbitrage Bot

## Project Description
A Python-based trading bot that utilizes Binance’s REST and WebSocket APIs to identify and capitalize on arbitrage opportunities across multiple trading pairs.

## Features
- Asynchronous data streaming using Python’s `asyncio` and `websockets`.  
- Real-time detection of market price discrepancies.  
- Configurable threshold to trigger arbitrage trades.  
- Detailed logging of trade actions.  

## Setup

1. **Clone this Repository**  
   ```bash
   git clone https://github.com/rdlz1/Bot_arbitrage.git
   cd Bot_arbitrage

2. **Install Dependencies**  
   ```bash
   pip install -r requirements.txt
    
3. **Configure Environment**  
Copy .env_template to .env.
Update the required fields (API keys, symbol pairs, thresholds).

4. **Run the Bot**  
   ```bash
   python binance_bot_arbitrage.py