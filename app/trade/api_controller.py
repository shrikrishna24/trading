import json
import asyncio
import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from logzero import logger

# ✅ Initialize FastAPI
app = FastAPI()

# ✅ Redis Connection (Pub/Sub)
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# ✅ Store Active WebSocket Connections
active_connections = set()

class WebSocketManager:
    """Handles WebSocket connections."""
    async def connect(self, websocket: WebSocket):
        """Accepts a new WebSocket connection."""
        await websocket.accept()
        active_connections.add(websocket)
        logger.info(f"✅ New WebSocket Connected | Active Connections: {len(active_connections)}")

    async def disconnect(self, websocket: WebSocket):
        """Removes WebSocket connection on disconnect."""
        active_connections.remove(websocket)
        logger.info(f"❌ WebSocket Disconnected | Active Connections: {len(active_connections)}")

    async def send_message(self, message: str):
        """Sends message to all connected clients."""
        if active_connections:
            await asyncio.gather(*(ws.send_text(message) for ws in active_connections))

# ✅ Initialize WebSocket Manager
ws_manager = WebSocketManager()

@app.websocket("/ws/market_data")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for live market data streaming."""
    await ws_manager.connect(websocket)
    
    try:
        pubsub = redis_client.pubsub()
        pubsub.subscribe("market_data:99926000")  # ✅ Subscribe to live market data channel
        
        while True:
            message = pubsub.get_message(ignore_subscribe_messages=True)
            if message:
                await ws_manager.send_message(message["data"])  # ✅ Send data to all clients
            await asyncio.sleep(0.1)  # ✅ Prevents excessive CPU usage
        
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"❌ WebSocket Error: {e}")
        await ws_manager.disconnect(websocket)
