import pyotp
from SmartApi import SmartConnect
from app.config import Config
from datetime import datetime, timedelta  # ✅ Missing Imports

# Global session storage
smart_api = None
auth_token = None
feed_token = None

def initialize_smart_api():
    """
    Initializes SmartAPI session if not already logged in.
    Reuses session if already authenticated.
    """
    global smart_api, auth_token, feed_token

    if smart_api is not None and auth_token is not None and feed_token is not None:
        print("✅ Reusing existing SmartAPI session.")
        return smart_api, auth_token, feed_token

    try:
        smart_api = SmartConnect(api_key=Config.ANGEL_API_KEY)

        # Generate TOTP for login
        totp = pyotp.TOTP(Config.ANGEL_TOTP_SECRET).now()

        # Generate session
        session_data = smart_api.generateSession(Config.ANGEL_CLIENT_ID, Config.ANGEL_CLIENT_PASSWORD, totp)

        if session_data.get("status"):
            auth_token = session_data["data"]["jwtToken"]
            feed_token = smart_api.getfeedToken()
            client_code = Config.ANGEL_CLIENT_ID 
            api_key = Config.ANGEL_API_KEY
            
            print("✅ SmartAPI session established successfully")
            return smart_api, auth_token, feed_token, client_code, api_key
        else:
            raise Exception(f"❌ Login failed: {session_data.get('message')}")

    except Exception as e:
        print(f"❌ Error initializing SmartAPI: {e}")
        smart_api = None  # Reset session to avoid reuse of invalid state
        return None, None, None


def get_historical_data(symbol_token, exchange="NFO", interval="ONE_MINUTE", days=5):
    """
    Fetch historical data for the given symbol token.

    :param symbol_token: Token for the instrument
    :param exchange: "NFO" for options, "NSE" for stocks
    :param interval: Timeframe (ONE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE, ONE_DAY)
    :param days: Number of past days to fetch data for
    :return: List of candles (timestamp, open, high, low, close, volume) or None on failure
    """
    smart_api, _, _ = initialize_smart_api()
    if smart_api is None:
        print("❌ SmartAPI not initialized. Cannot fetch historical data.")
        return None

    to_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M")

    params = {
        "exchange": exchange,
        "symboltoken": symbol_token,
        "interval": interval,
        "fromdate": from_date,
        "todate": to_date
    }

    try:
        response = smart_api.getCandleData(params)
        if response and "data" in response:
            print(f"✅ Historical data fetched successfully for {symbol_token}")
            return response["data"]
        else:
            print(f"⚠️ No historical data found for {symbol_token}: {response}")
            return None
    except Exception as e:
        print(f"❌ Error fetching historical data: {e}")
        return None
