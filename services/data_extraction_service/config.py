# data_extraction_service/config.py

"""
Configuration file for the Data Extraction Service.
"""

# Kraken API configuration
KRAKEN_API_URL = 'https://api.kraken.com/0/public'

# List of cryptocurrency pairs to fetch data for
CRYPTO_PAIRS = [
    'SOL/USD',
    'DOGE/USD',
    'BTC/USD',
    'ETH/USD',
    'AVAX/USD',
    'COMP/USD',
    'AAVE/USD',
    'MKR/USD',
    'LINK/USD',
    'QNT/USD'
]

# RabbitMQ configuration
RABBITMQ_HOST = 'localhost'  # Update if RabbitMQ is on a different host
EXCHANGE_NAME = 'crypto_data_exchange'
ROUTING_KEY = 'raw_data'
QUEUE_NAME = 'raw_data'

# Logging configuration
LOG_LEVEL = 'INFO'  # Can be set to 'DEBUG' for more detailed logs
