from flask import Flask
from dotenv import load_dotenv
from .routes import zip_data_routes
from .routes import input_data_routes
from .routes import preprocess_data_routes
from .routes import train_data_routes
from .routes import deploy_model_routes
from .routes import predict_routes
from .routes import health_routes
from flask_cors import CORS


def create_app(config_class='app.config.TestConfig'):
    app = Flask(__name__)

    # Load environment variables from .env file
    load_dotenv()

    app.config.from_object(config_class)

    # Enable CORS
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    # Print the configuration settings
    # print("Configuration settings:")
    # for key, value in app.config.items():
    #     print(f"{key}: {value}")

    # Register blueprints
    app.register_blueprint(zip_data_routes.bp, url_prefix='/api/zip-data')
    app.register_blueprint(input_data_routes.bp, url_prefix='/api/input-data')
    app.register_blueprint(preprocess_data_routes.bp,
                           url_prefix='/api/preprocess')
    app.register_blueprint(train_data_routes.bp, url_prefix='/api/train')
    app.register_blueprint(deploy_model_routes.bp, url_prefix='/api/deploy')
    app.register_blueprint(predict_routes.bp,
                           url_prefix='/api/predict')
    app.register_blueprint(health_routes.bp, url_prefix='/api/health')

    return app
