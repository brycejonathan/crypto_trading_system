# data_transformation_service/utils.py

import logging
from datetime import datetime

def validate_data(data):
    """
    Validates the raw data for missing or inconsistent fields.
    """
    required_fields = ['timestamp', 'symbol', 'open', 'close', 'high', 'low', 'volume']

    for field in required_fields:
        if field not in data or data[field] is None:
            logging.error(f"Missing required field: {field}")
            return False

    # Additional validation logic can be added here
    return True

def normalize_data(data):
    """
    Normalizes the data into consistent units and formats.
    """
    try:
        # Convert timestamp to ISO format date string
        data['date'] = datetime.utcfromtimestamp(data['timestamp']).strftime('%Y-%m-%d')
        # Ensure numerical fields are floats
        data['open'] = float(data['open'])
        data['close'] = float(data['close'])
        data['high'] = float(data['high'])
        data['low'] = float(data['low'])
        data['volume'] = float(data['volume'])
        # Standardize symbol format
        data['symbol'] = data['symbol'].upper()
        return data
    except Exception as e:
        logging.error(f"Error normalizing data: {e}")
        return None

def enrich_data(data):
    """
    Enriches the data by adding calculated fields.
    """
    try:
        # Calculate price change percentage
        data['price_change_pct'] = ((data['close'] - data['open']) / data['open']) * 100
        return data
    except Exception as e:
        logging.error(f"Error enriching data: {e}")
        return None
