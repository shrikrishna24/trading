from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from app.utils.utils import initialize_smart_api
import json
from logzero import logger
from datetime import datetime, timedelta

# ✅ Convert exchange timestamp from milliseconds to IST
def convert_to_ist(exchange_timestamp):
    """Converts UTC timestamp to IST."""
    utc_time = datetime.utcfromtimestamp(exchange_timestamp / 1000)  # Convert to UTC datetime
    ist_time = utc_time + timedelta(hours=5, minutes=30)  # Convert to IST (UTC +5:30)
    return ist_time.strftime('%Y-%m-%d %H:%M:%S')  # Format as string

# ✅ Initialize SmartAPI session
smart_api, auth_token, feed_token, client_code, api_key = initialize_smart_api()

# ✅ Default settings (correlation_id & mode stay the same)
correlation_id = "abc123"
exchange_type = 1  # 1 = NSE Index
mode = 1  # LTP Mode

# ✅ Token list - Can be changed anywhere in the code
subscribed_tokens = ["99926000"]  # Default: Nifty 50 (Modify this list dynamically)

# ✅ Function to update token list dynamically
def update_tokens(new_tokens):
    """Dynamically updates the token list for subscription."""
    global subscribed_tokens
    subscribed_tokens = new_tokens
    logger.info(f"🔄 Updated Token List: {subscribed_tokens}")

# ✅ Function to get the formatted token list for subscription
def get_token_list():
    """Returns token list in the required WebSocket format."""
    return [{"exchangeType": exchange_type, "tokens": subscribed_tokens}]

# ✅ Initialize WebSocket
sws = SmartWebSocketV2(auth_token, api_key, client_code, feed_token)

def close_connection():
    """Gracefully closes the WebSocket connection only if it's still open."""
    if sws and sws.wsapp and sws.wsapp.sock and sws.wsapp.sock.connected:
        logger.info("❌ Unsubscribing and Closing WebSocket Connection...")
        try:
            sws.unsubscribe(correlation_id, mode, get_token_list())  # Unsubscribe only if connection is open
        except Exception as e:
            logger.warning(f"⚠️ Unsubscribe failed: {e}")

        try:
            sws.close_connection()  # Properly disconnect WebSocket
            logger.info("✅ WebSocket Disconnected Successfully.")
        except Exception as e:
            logger.error(f"❌ WebSocket disconnection error: {e}")
    else:
        logger.info("⚠️ WebSocket already closed. No action needed.")


def on_data(wsapp, message):
    if isinstance(message, str):
        data = json.loads(message)
    else:
        data = message

    exchange_timestamp = data.get('exchange_timestamp', 0)
    readable_time_ist = convert_to_ist(exchange_timestamp)

    last_traded_price = data.get('last_traded_price', 0) / 100  # Convert from paise to ₹

    token = data.get('token', 'Unknown')
    logger.info(f" 📅 Timestamp (IST): {readable_time_ist} | 💰 LTP: ₹{last_traded_price}")

    close_connection()

def on_open(wsapp):
    """Sends a subscription request to WebSocket."""
    logger.info("✅ WebSocket Connection Opened. Sending Subscription Request...")
    sws.subscribe(correlation_id, mode, get_token_list())  # ✅ Subscribe to updated tokens

def on_error(wsapp, error):
    """Handles WebSocket errors and closes connection if necessary."""
    logger.error(f"❌ WebSocket Error: {error}")

    # ✅ Check if WebSocket is already closed before calling close_connection()
    if sws.wsapp and sws.wsapp.sock and sws.wsapp.sock.connected:
        close_connection()
    else:
        logger.info("⚠️ WebSocket is already closed. No need to close again.")

def on_close(wsapp):
    """Handles WebSocket disconnection events."""
    logger.info("✅ WebSocket Connection Closed Successfully.")

sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close

# ✅ Function to start WebSocket streaming
def start_market_data_feed():
    """Starts WebSocket connection for live tick data."""
    logger.info(f"🚀 Starting SmartWebSocketV2 for Tokens: {subscribed_tokens}")
    sws.connect()
