import json
import requests
import pandas as pd
from datetime import datetime
from logzero import logger
from collections import defaultdict
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from app.utils.utils import initialize_smart_api
from app.config import Config

# ✅ Initialize SmartAPI session
smart_api, auth_token, feed_token = initialize_smart_api()

# ✅ WebSocket Setup
correlation_id = "nifty_options"
mode = 1  # LTP mode
exchange_type = 2  # NFO (NSE Futures & Options)

# ✅ Live Data Storage
option_chain_live_data = defaultdict(dict)

# ✅ Load Scrip Master JSON from Angel One API
SCRIP_MASTER_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"

def load_scrip_master():
    """Fetch and load the scrip master JSON from Angel One."""
    try:
        response = requests.get(SCRIP_MASTER_URL)
        response.raise_for_status()
        return response.json()  # ✅ Parse and return JSON response
    except Exception as e:
        logger.error(f"❌ Error Loading Scrip Master: {e}")
        return []  # ✅ Return an empty list if request fails

def get_nifty_option_chain(expiry=None):
    """Fetches and filters Nifty 50 option chain data."""
    scrip_data = load_scrip_master()
    
    # ✅ Filter only Nifty 50 options (CE & PE)
    options = [
        s for s in scrip_data
        if s.get("instrumenttype") == "OPTIDX" and 
        s.get("name") == "NIFTY" and 
        s.get("exch_seg") == "NFO"
    ]

    if not options:
        logger.error("❌ No Nifty 50 options found.")
        return []

    # ✅ Get nearest expiry if not provided
    if not expiry:
        expiry_dates = sorted(set(s["expiry"] for s in options))
        expiry = expiry_dates[0]  # Select closest expiry

    # ✅ Filter options by expiry
    expiry_options = [s for s in options if s["expiry"] == expiry]

    # ✅ Extract Tokens
    tokens = [s["token"] for s in expiry_options]

    logger.info(f"📊 Loaded {len(expiry_options)} Nifty 50 Options for Expiry: {expiry}")

    return tokens

# ✅ Initialize WebSocket
sws = SmartWebSocketV2(auth_token, Config.ANGEL_API_KEY, Config.ANGEL_CLIENT_ID, feed_token)

# ✅ Subscribe to Live Option Chain Data
def subscribe_option_chain(expiry=None):
    """Subscribes WebSocket to Nifty 50 Option Chain for live updates."""
    tokens = get_nifty_option_chain(expiry)
    
    if not tokens:
        return

    # ✅ Subscribe to WebSocket
    token_list = [{"exchangeType": exchange_type, "tokens": tokens}]
    sws.subscribe(correlation_id, mode, token_list)

    logger.info(f"📡 Subscribed to {len(tokens)} Nifty 50 Option Contracts for Expiry: {expiry}")

# ✅ WebSocket Handlers
def on_data(wsapp, message):
    """Processes WebSocket tick data and updates market info."""
    try:
        data = json.loads(message) if isinstance(message, str) else message
        token = data.get("token")
        if not token:
            return

        # ✅ Update Market Data
        option_chain_live_data[token].update({
            "ltp": float(data.get("last_traded_price", 0)) / 100,
            "bid": float(data.get("best_bid_price", 0)) / 100,
            "ask": float(data.get("best_ask_price", 0)) / 100,
            "volume": data.get("total_traded_volume", 0),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # ✅ Log Compact Live Update
        logger.info(f"📊 Live Update | Token: {token} | LTP: {option_chain_live_data[token]['ltp']} "
                    f"Bid: {option_chain_live_data[token]['bid']} Ask: {option_chain_live_data[token]['ask']}")

    except Exception as e:
        logger.error(f"❌ Error processing tick data: {e}")

def on_open(wsapp):
    """Triggers WebSocket subscription when connection opens."""
    logger.info("✅ WebSocket Connected. Subscribing to Option Chain...")
    subscribe_option_chain()

def on_error(wsapp, error):
    """Handles WebSocket errors."""
    logger.error(f"❌ WebSocket Error: {error}")

def on_close(wsapp):
    """Handles WebSocket disconnection events."""
    logger.info("✅ WebSocket Connection Closed.")

# ✅ Assign WebSocket Handlers
sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close

# ✅ Start WebSocket Connection
def start_option_chain_ws():
    """Starts the WebSocket connection for Nifty 50 option chain live data."""
    logger.info("🚀 Starting WebSocket for Nifty 50 Option Chain Live Data...")
    # sws.connect()
