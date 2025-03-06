from flask import Flask
from .config import Config

# Initialize Flask app
app = Flask(__name__)
app.config.from_object(Config)

# Register Blueprints
# from .auth.routes import auth_bp
# app.register_blueprint(auth_bp, url_prefix="/auth")

# from .trade.routes import trade_bp
# app.register_blueprint(trade_bp, url_prefix="/trade")
