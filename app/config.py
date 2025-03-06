import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "fallback_secret_key")  # ✅ Load from .env
    ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
    ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
    ANGEL_CLIENT_PASSWORD = os.getenv("ANGEL_CLIENT_PASSWORD")
    ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")
