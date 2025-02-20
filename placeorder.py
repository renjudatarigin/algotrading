import os
import datetime
import csv
import pandas as pd
from pyotp import TOTP
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from azure.storage.blob import BlobServiceClient
import io 
from dotenv import load_dotenv
import pytz
load_dotenv() 
# -----------------------------------------------
# API and WebSocket Setup
# -----------------------------------------------

KEY_PATH = os.path.dirname(os.path.abspath(__file__))
os.chdir(KEY_PATH)

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")  # Replace with your actual connection string
CONTAINER_NAME = "stockdata" 


# Read API credentials
with open("key.txt", "r") as f:
    key_secret = f.read().split()

# Initialize API session
api = SmartConnect(api_key=key_secret[0])
session_data = api.generateSession(key_secret[2], key_secret[3], TOTP(key_secret[4]).now())
feed_token = api.getfeedToken()

# Initialize WebSocket connection
sws = SmartWebSocketV2(session_data["data"]["jwtToken"], key_secret[0], key_secret[2], feed_token)

# -----------------------------------------------
# Token-to-Symbol Mapping
# -----------------------------------------------

# TOKEN_SYMBOL_MAP = {
#     "14977": "POWERGRID-EQ",
#     "4306": "SHRIRAMFIN-EQ",
#     "11630": "NTPC-EQ",
#     "2475": "ONGC-EQ",
#     "1363": "HINDALCO-EQ"
#     # Add more as needed
# }

TOKEN_SYMBOL_MAP = {
    # "14977": "POWERGRID-EQ",
    # "4306": "SHRIRAMFIN-EQ",
    "11630": "NTPC-EQ",
    # Add more as needed
}
# -----------------------------------------------
# Indicator Parameters
# -----------------------------------------------

EMA_SHORT_PERIOD = 9
EMA_LONG_PERIOD = 21
BOLLINGER_PERIOD = 20
BOLLINGER_STD_DEV = 2
RSI_PERIOD = 14

# Trade Settings
TRADE_COOLDOWN_PERIOD = datetime.timedelta(seconds=90)
MIN_HOLD_TIME = datetime.timedelta(minutes=3)
TRADE_CLOSE_TIME = datetime.time(15, 5)

# Data Buffers
price_data = {}
trade_positions = {}
trade_cooldowns = {}

# -----------------------------------------------
# Indicator Calculations
# -----------------------------------------------
def calculate_ema(prices, period):
    return pd.Series(prices).ewm(span=period, adjust=False).mean().iloc[-1] if len(prices) >= period else None

def calculate_bollinger_bands(prices, period, std_dev):
    if len(prices) < period:
        return None, None
    rolling_mean = pd.Series(prices).rolling(window=period).mean()
    rolling_std = pd.Series(prices).rolling(window=period).std()
    upper_band = rolling_mean.iloc[-1] + (rolling_std.iloc[-1] * std_dev)
    lower_band = rolling_mean.iloc[-1] - (rolling_std.iloc[-1] * std_dev)
    return upper_band, lower_band

def calculate_rsi(prices, period=RSI_PERIOD):
    if len(prices) < period:
        return None
    delta = pd.Series(prices).diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs)).iloc[-1]

def calculate_percentage_difference(entry_price, current_price):
    return ((current_price - entry_price) / entry_price) * 100

def get_profit_threshold(timestamp):
    market_open_time = datetime.time(9, 15)
    high_profit_time = datetime.time(9, 45)
    return 0.5 if market_open_time <= timestamp.time() <= high_profit_time else 0.4

# Initialize Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

def log_trade(timestamp, token, trade_type, price):
    """Log trade details into a CSV file."""
    blob_name = f"log_trade_signals{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

    # Prepare data as CSV format
    csv_data = f"{timestamp},{token},{trade_type},{price}\n"

    try:
        # Check if the file already exists
        if blob_client.exists():
            # Download existing file
            existing_blob = blob_client.download_blob().readall().decode()
            csv_data = existing_blob + csv_data  # Append new row

        # Upload updated CSV to Azure Storage
        blob_client.upload_blob(csv_data, overwrite=True)
        print(f"Trade logged to Azure Storage: {blob_name}")

    except Exception as e:
        print(f"Error writing to Azure Blob Storage: {e}")


# -----------------------------------------------
# Force Close Trades (Updated)
# -----------------------------------------------
def force_close_trades(timestamp):
    """Force close all open trades at 3:05 PM by placing market orders."""
    global trade_positions, trade_cooldowns

    for token, trade_info in list(trade_positions.items()):
        current_price = price_data[token][-1] if token in price_data and price_data[token] else None
        if not current_price:
            continue

        last_trade = trade_info["type"]
        order_type = "SELL" if last_trade == "BUY" else "BUY"
        
        # Place a market order to exit the trade
        order_id = place_order(token, order_type)

        if order_id:
            log_order(timestamp, token, order_type, current_price, order_id)  # Log forced exit order
            print(f"Force closed {order_type} order for {TOKEN_SYMBOL_MAP[token]} at {current_price}")

        del trade_positions[token]  # Remove from active trades
        trade_cooldowns[token] = timestamp  # Apply cooldown

def place_order(token, trade_type, quantity=1):
    """Place a buy or sell order using correct trading symbol."""
    if token not in TOKEN_SYMBOL_MAP:
        print(f"Error: Token {token} not found in TOKEN_SYMBOL_MAP")
        return None

    trading_symbol = TOKEN_SYMBOL_MAP[token]  # Get correct trading symbol

    try:
        order_params = {
            "variety": "NORMAL",
            "tradingsymbol": trading_symbol,
            "symboltoken": token,
            "transactiontype": trade_type,
            "exchange": "NSE",
            "ordertype": "MARKET",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "quantity": quantity
        }
        order_response = api.placeOrder(order_params)
        
        print(f"API Response: {order_response}")  # DEBUG: Print raw response
        
        # If the response is a string (order ID), assume order is successful
        if isinstance(order_response, str):
            order_id = order_response.strip()  # Ensure no unwanted spaces
            print(f"Order placed successfully: {order_id} for {trading_symbol}")
            return order_id
        
        print(f"Order placement failed: Unexpected response format: {order_response}")
        return None
    except Exception as e:
        print(f"Error placing order: {e}")
        return None

def log_order(timestamp, token, trade_type, price, order_id):
    """Log trade details into a CSV file."""
    blob_name = f"log_order{datetime.datetime.now().strftime('%Y-%m-%d')}.csv"
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

    # Prepare data as CSV format
    csv_data = f"{timestamp},{token},{trade_type},{price},{order_id}\n"

    try:
        # Check if the file already exists
        if blob_client.exists():
            # Download existing file
            existing_blob = blob_client.download_blob().readall().decode()
            csv_data = existing_blob + csv_data  # Append new row

        # Upload updated CSV to Azure Storage
        blob_client.upload_blob(csv_data, overwrite=True)
        print(f"order logged to Azure Storage: {blob_name}")

    except Exception as e:
        print(f"Error writing to Azure Blob Storage: {e}")


# -----------------------------------------------
# WebSocket Handlers
# -----------------------------------------------

def on_data(wsapp, message):
    """Handle incoming WebSocket data."""
    try:
        token = str(message.get("token"))
        close_price = float(message.get("last_traded_price"))
        timestamp = datetime.datetime.now(datetime.UTC).astimezone(pytz.timezone("Asia/Kolkata"))

        if token not in price_data:
            price_data[token] = []
        price_data[token].append(close_price)
        
        if len(price_data[token]) > max(EMA_LONG_PERIOD, BOLLINGER_PERIOD):
            price_data[token].pop(0)
        
        decide_trade(token, price_data[token], timestamp)

    except Exception as e:
        print(f"Error in on_data: {e}")

# -----------------------------------------------
# Trade Decision Logic (Updated)
# -----------------------------------------------
def decide_trade(token, prices, timestamp):
    """Decide whether to buy or sell and place orders while logging signals."""
    global trade_positions, trade_cooldowns

    if token not in TOKEN_SYMBOL_MAP:
        print(f"Skipping token {token}: Not found in TOKEN_SYMBOL_MAP")
        return  

    if timestamp.time() >= TRADE_CLOSE_TIME:
        force_close_trades(timestamp)  # Ensure trades are closed at 3:05 PM
        return  

    if len(prices) < max(EMA_LONG_PERIOD, BOLLINGER_PERIOD):
        return
    
    current_price = prices[-1]
    ema_short = calculate_ema(prices, EMA_SHORT_PERIOD)
    ema_long = calculate_ema(prices, EMA_LONG_PERIOD)
    upper_band, lower_band = calculate_bollinger_bands(prices, BOLLINGER_PERIOD, BOLLINGER_STD_DEV)
    rsi = calculate_rsi(prices)
    profit_threshold = get_profit_threshold(timestamp)
    
    if None in [rsi, ema_short, ema_long, upper_band, lower_band]:
        return
    
    last_trade = trade_positions.get(token, {}).get("type", None)

    if token in trade_cooldowns and (timestamp - trade_cooldowns[token]) < TRADE_COOLDOWN_PERIOD:
        return  

    # Exit Trade Logic
    if token in trade_positions:
        entry_price = trade_positions[token]["entry_price"]
        entry_time = trade_positions[token]["entry_time"]
        price_diff = calculate_percentage_difference(entry_price, current_price)
        time_diff = timestamp - entry_time

        if time_diff >= MIN_HOLD_TIME:
            if last_trade == "BUY" and price_diff >= profit_threshold:
                log_trade(timestamp, token, "SELL", current_price)
                order_id = place_order(token, "SELL")
                if order_id:
                    log_order(timestamp, token, "SELL", current_price, order_id)
                    del trade_positions[token]
                    trade_cooldowns[token] = timestamp
            elif last_trade == "SELL" and price_diff <= -profit_threshold:
                log_trade(timestamp, token, "BUY", current_price)
                order_id = place_order(token, "BUY")
                if order_id:
                    log_order(timestamp, token, "BUY", current_price, order_id)
                    del trade_positions[token]
                    trade_cooldowns[token] = timestamp
        return  

    # Enter Trade Logic
    if ema_short > ema_long and current_price <= lower_band and rsi < 40:
        log_trade(timestamp, token, "BUY", current_price)
        order_id = place_order(token, "BUY")
        if order_id:
            trade_positions[token] = {"entry_price": current_price, "entry_time": timestamp, "type": "BUY"}
            log_order(timestamp, token, "BUY", current_price, order_id)

    elif ema_short < ema_long and current_price >= upper_band and rsi > 60:
        log_trade(timestamp, token, "SELL", current_price)
        order_id = place_order(token, "SELL")
        if order_id:
            trade_positions[token] = {"entry_price": current_price, "entry_time": timestamp, "type": "SELL"}
            log_order(timestamp, token, "SELL", current_price, order_id)# -----------------------------------------------
# Start WebSocket
# -----------------------------------------------

def on_open(wsapp):
    """Subscribe to token streams when WebSocket opens."""
    tokens = list(TOKEN_SYMBOL_MAP.keys())  # Subscribe to all mapped tokens
    print("WebSocket Connected, Subscribing to Tokens...")
    sws.subscribe("stream_1", 3, [{"exchangeType": 1, "tokens": tokens}])

if __name__ == "__main__":
    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = lambda wsapp, error: print(f"WebSocket error: {error}")
    
    try:
        sws.connect()
    except Exception as e:
        print(f"WebSocket connection failed: {e}")
