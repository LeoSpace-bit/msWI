#config.py
import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'your-secret-key-here'
    SQLALCHEMY_DATABASE_URI = 'sqlite:///app.db'
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'  # используйте 'localhost:9092' или если WI в Docker 'host.docker.internal:9092'
    KAFKA_PRODUCT_TOPIC = 'products'
    KAFKA_STOCK_TOPIC = 'warehouse_stock_updates'
    KAFKA_INVOICE_TOPIC = 'invoice_requests'