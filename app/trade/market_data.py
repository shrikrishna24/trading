from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from app.utils.utils import initialize_smart_api
import json

# ✅ Initialize SmartAPI session
smart_api, auth_token, feed_token, client_code, api_key = initialize_smart_api()

# ✅ Define correct token and exchange for Nifty 50
nifty_token = "99926000"  # Token for Nifty 50 Index
exchange_type = 1  # 1 = NSE Index

# ✅ Callback function to handle incoming market tick data
def on_ticks(wsapp, tick):
    """Handles tick data received from WebSocket."""
    try:
        print(f"📩 Received Tick Data: {tick}")
    except Exception as e:
        print(f"❌ Error processing Tick Data: {e}")

# ✅ Callback function to handle WebSocket errors
def on_error(wsapp, error):
    print(f"❌ WebSocket Error: {error}")

# ✅ Callback function to handle WebSocket closure
def on_close(wsapp, close_status_code, close_msg):
    print("❌ WebSocket connection closed")

# ✅ Callback function to send subscription request
def on_open(wsapp):
    print("✅ WebSocket Connection Opened. Sending Subscription Request...")

    # ✅ Subscription request
    subscribe_message = {
        "correlationID": "nifty_index",
        "action": "subscribe",
        "params": {
            "mode": "FULL",  # "LTP" = Last Traded Price, "FULL" = Complete Market Data
            "tokenList": [{"exchangeType": exchange_type, "tokens": [nifty_token]}]
        }
    }

    print(f"📡 Sending Subscription Request: {json.dumps(subscribe_message)}")
    wsapp.send(json.dumps(subscribe_message))

# ✅ Initialize WebSocket
sws = SmartWebSocketV2(auth_token, api_key, client_code, feed_token)

# ✅ Assign callback functions
sws.on_open = on_open
sws.on_ticks = on_ticks  # ✅ Now using `on_ticks` to fetch tick data
sws.on_error = on_error
sws.on_close = on_close

# ✅ Function to start WebSocket streaming
def start_market_data_feed():
    print("🚀 Starting SmartWebSocketV2 for Nifty 50 Live Tick Data...")
    sws.connect()
