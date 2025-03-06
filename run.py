from app import app
from app.trade.market_data import start_market_data_feed
import threading

if __name__ == "__main__":
    # Run market data feed in a separate thread
    threading.Thread(target=start_market_data_feed, daemon=True).start()

    # Start Flask app
    app.run(debug=True)
