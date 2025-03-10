import threading
from app.trade.market_data import start_market_data_feed
from app.trade.option_chain import start_option_chain_ws
from app.trade.api_controller import app
import uvicorn

if __name__ == "__main__":
    # Start both WebSockets in parallel threads
    threading.Thread(target=start_market_data_feed, daemon=True).start()
    threading.Thread(target=start_option_chain_ws, daemon=True).start()

    # Keep the script running
    uvicorn.run("app.trade.api_controller:app", host="0.0.0.0", port=8000, reload=True)
