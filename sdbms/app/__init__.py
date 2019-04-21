from flask import Flask


def create_app():
    from .views import main_api

    app = Flask(__name__)
    
    app.config.from_pyfile('../../config.py')
    
    app.register_blueprint(main_api)

    return app
