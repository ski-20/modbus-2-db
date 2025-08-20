# webapp/__init__.py
from flask import Flask
from .routes_ui import ui_bp
from .routes_api import api_bp

def create_app():
    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.register_blueprint(ui_bp)
    app.register_blueprint(api_bp, url_prefix="/api")

    @app.route("/healthz")
    def healthz():
        return "ok", 200

    return app
