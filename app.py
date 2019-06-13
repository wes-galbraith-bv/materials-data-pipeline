from flask import Flask

from db import DatabaseManager

def create_app():
    app = Flask(__name__)

    from resources.line_items import line_items
    app.register_blueprint(line_items)

    return app
