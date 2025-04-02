from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from app.utils.utils import initialize_smart_api
import json
from logzero import logger
from datetime import datetime, timedelta
import redis
from collections import defaultdict

# ✅ Convert exchange timestamp from milliseconds to IST
def convert_to_ist(exchange_timestamp):
    """Converts UTC timestamp to IST."""
    utc_time = datetime.utcfromtimestamp(exchange_timestamp / 1000)  # Convert to UTC datetime
    ist_time = utc_time + timedelta(hours=5, minutes=30)  # Convert to IST (UTC +5:30)
    return ist_time.strftime('%Y-%m-%d %H:%M:%S')

# ✅ Initialize SmartAPI session
smart_api, auth_token, feed_token, client_code, api_key = initialize_smart_api()

# ✅ WebSocket Global Settings (Keep Correlation ID & Mode Fixed)
correlation_id = "abc123"
exchange_type = 1  # 1 = NSE Index
mode = 1  # LTP Mode (Change to "FULL" if needed)

# ✅ Token list (modifiable dynamically)
subscribed_tokens = ["99926000"]  # Default empty list


# ✅ Redis Connection (Pub/Sub)
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# ✅ Function to update tokens dynamically
def update_tokens(new_tokens):
    """Dynamically updates the token list."""
    global subscribed_tokens
    subscribed_tokens = new_tokens
    logger.info(f"🔄 Updated Token List: {subscribed_tokens}")

# ✅ Function to return token list in required format
def get_token_list():
    """Returns formatted token list for WebSocket."""
    return [{"exchangeType": exchange_type, "tokens": subscribed_tokens}]

# ✅ Initialize WebSocket
sws = SmartWebSocketV2(auth_token, api_key, client_code, feed_token)

# ✅ Fetch Live Market Data Instead of Historical Data
def get_market_data(token_list):
    """
    Fetches live market data for multiple tokens using getMarketData().
    """
    params = {
        "mode": "FULL",  # Options: "LTP", "QUOTE", "FULL"
        "exchangeTokens": [{"exchangeType": 1, "tokens": token_list}]
    }

    try:
        response = smart_api.getMarketData(params)
        if response and "data" in response:
            return response["data"]
        else:
            logger.error(f"❌ No live market data received for tokens: {token_list}")
            return []
    except Exception as e:
        logger.error(f"❌ Error fetching live market data: {e}")
        return []

# ✅ Store candles for multiple timeframes
live_candles = {}
last_logged_time = {}

def update_live_candle(data):
    """
    Updates the live candle based on tick data in real-time (millisecond level).
    """
    global last_logged_time

    # ✅ Ensure required fields exist
    if not data or "token" not in data or "last_traded_price" not in data:
        logger.warning(f"⚠️ Skipping update due to missing fields: {data}")
        return None

    token = data["token"]
    ltp = data["last_traded_price"] / 100  # Convert paise to ₹
    exchange_timestamp = data.get("exchange_timestamp", 0)

    # ✅ Ensure exchange_timestamp is valid
    if not exchange_timestamp:
        logger.warning(f"⚠️ Skipping update due to missing timestamp: {data}")
        return None

    timestamp_ist = convert_to_ist(exchange_timestamp)

    # ✅ Extract minute-level timestamp for checking (ensures Open resets every minute)
    current_minute = timestamp_ist[:-3]  # "YYYY-MM-DD HH:MM" → Strips seconds

    if token not in live_candles or last_logged_time.get(token) != current_minute:
        # ✅ Initialize a new candle if a new minute starts
        live_candles[token] = {
            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp,
            "timestamp": timestamp_ist
        }
        logger.info(f"🆕 New Candle Started for Token {token} at {timestamp_ist} | O: {ltp}")
    else:
        # ✅ Update existing candle dynamically
        candle = live_candles[token]
        candle["high"] = max(candle["high"], ltp)  # ✅ Update High if new LTP is higher
        candle["low"] = min(candle["low"], ltp)    # ✅ Update Low if new LTP is lower
        candle["close"] = ltp  # ✅ Always update Close
        candle["timestamp"] = timestamp_ist  # ✅ Keep latest timestamp

    # ✅ Log updated OHLC immediately
    last_logged_time[token] = current_minute  # Update last logged time

    redis_client.publish(f"market_data:{token}", json.dumps(live_candles[token]))

    return live_candles[token]  # ✅ Return updated OHLC

def close_connection():
    """Closes WebSocket only if it's still open."""
    if sws and sws.wsapp and sws.wsapp.sock and sws.wsapp.sock.connected:
        logger.info("❌ Unsubscribing and Closing WebSocket Connection...")
        try:
            sws.unsubscribe(correlation_id, mode, get_token_list())
        except Exception as e:
            logger.warning(f"⚠️ Unsubscribe failed: {e}")

        try:
            sws.disconnect()
            logger.info("✅ WebSocket Disconnected Successfully.")
        except Exception as e:
            logger.error(f"❌ WebSocket disconnection error: {e}")
    else:
        logger.info("⚠️ WebSocket already closed.")

def on_data(wsapp, message):
    """Handles tick data from WebSocket and logs real-time OHLC updates."""
    try:
        if isinstance(message, str):
            data = json.loads(message)
        else:
            data = message

        # ✅ Ensure the data contains expected keys before processing
        if not data or "token" not in data or "last_traded_price" not in data:
            logger.warning(f"⚠️ Received invalid data: {data}")
            return

        # ✅ Update live candle and log immediately
        updated_candle = update_live_candle(data)
        
        if updated_candle:  # ✅ Prevent NoneType access
            logger.info(f"📊 Token: {data['token']} | 🕰 {updated_candle['timestamp']} | "
                        f"📈 O: {updated_candle['open']} H: {updated_candle['high']} "
                        f"L: {updated_candle['low']} C: {updated_candle['close']}")

    except Exception as e:
        logger.error(f"❌ Error processing tick data: {e}")

def on_open(wsapp):
    """Fetches real-time market data and starts WebSocket streaming."""
    logger.info("✅ Fetching real-time market data before WebSocket subscription...")

    market_data = get_market_data(subscribed_tokens)  # ✅ Fetch live data
    if market_data:
        for token_data in market_data:
            token = token_data["token"]
            logger.info(f"📊 LIVE Market Data | Token: {token} | "
                        f"📈 O: {token_data['open']} H: {token_data['high']} "
                        f"L: {token_data['low']} C: {token_data['close']}")

    logger.info("✅ WebSocket Connection Opened. Sending Subscription Request...")
    sws.subscribe(correlation_id, mode, get_token_list())  # ✅ Subscribe to live data

def on_error(wsapp, error):
    """Handles WebSocket errors and closes connection if necessary."""
    logger.error(f"❌ WebSocket Error: {error}")
    if sws.wsapp and sws.wsapp.sock and sws.wsapp.sock.connected:
        close_connection()
    else:
        logger.info("⚠️ WebSocket is already closed.")

def on_close(wsapp):
    """Handles WebSocket disconnection events."""
    logger.info("✅ WebSocket Connection Closed Successfully.")

sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close

def start_market_data_feed():
    """Starts WebSocket connection for live tick data."""
    logger.info(f"🚀 Starting WebSocket for Tokens: {subscribed_tokens}")
    sws.connect()