# data_extraction_service/main.py

import logging
import sys
import time
from datetime import datetime, timedelta

import pika
import requests

from config import (
    KRAKEN_API_URL,
    CRYPTO_PAIRS,
    RABBITMQ_HOST,
    EXCHANGE_NAME,
    ROUTING_KEY,
    QUEUE_NAME,
    LOG_LEVEL
)

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('DataExtractionService')


class KrakenAPIClient:
    """
    Client for interacting with the Kraken Public API.
    """

    def __init__(self, base_url):
        self.base_url = base_url

    def get_tradable_asset_pairs(self, pairs):
        """
        Retrieves tradable asset pairs information.
        """
        try:
            url = f"{self.base_url}/AssetPairs"
            params = {'pair': ','.join(pairs)}
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if data.get('error'):
                logger.error(f"API error: {data['error']}")
                return None
            # Map the field names to verbose ones
            asset_pairs_info = {}
            for key, value in data['result'].items():
                asset_pairs_info[key] = self._map_asset_pair_info(value)
            return asset_pairs_info
        except requests.RequestException as e:
            logger.error(f"RequestException while fetching asset pairs: {e}")
            return None

    def get_ticker_information(self, pairs):
        """
        Retrieves ticker information for specified pairs.
        """
        try:
            url = f"{self.base_url}/Ticker"
            params = {'pair': ','.join(pairs)}
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if data.get('error'):
                logger.error(f"API error: {data['error']}")
                return None
            # Map the field names to verbose ones
            ticker_info = {}
            for key, value in data['result'].items():
                ticker_info[key] = self._map_ticker_info(value)
            return ticker_info
        except requests.RequestException as e:
            logger.error(f"RequestException while fetching ticker information: {e}")
            return None

    def get_order_book(self, pair, count=100):
        """
        Retrieves the order book for a specified pair.
        """
        try:
            url = f"{self.base_url}/Depth"
            params = {'pair': pair, 'count': count}
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if data.get('error'):
                logger.error(f"API error: {data['error']}")
                return None
            # Map the field names to verbose ones
            order_book_data = {}
            for key, value in data['result'].items():
                order_book_data[key] = self._map_order_book(value)
            return order_book_data
        except requests.RequestException as e:
            logger.error(f"RequestException while fetching order book: {e}")
            return None

    def _map_asset_pair_info(self, data):
        """
        Maps the asset pair info fields to verbose names.
        """
        # The field names are already verbose, but ensure consistency
        return data

    def _map_ticker_info(self, data):
        """
        Maps the ticker info fields to verbose names.
        """
        mapped_data = {
            'ask': {
                'price': data['a'][0],
                'whole_lot_volume': data['a'][1],
                'lot_volume': data['a'][2]
            },
            'bid': {
                'price': data['b'][0],
                'whole_lot_volume': data['b'][1],
                'lot_volume': data['b'][2]
            },
            'last_trade_closed': {
                'price': data['c'][0],
                'lot_volume': data['c'][1]
            },
            'volume': {
                'today': data['v'][0],
                'last_24_hours': data['v'][1]
            },
            'volume_weighted_average_price': {
                'today': data['p'][0],
                'last_24_hours': data['p'][1]
            },
            'number_of_trades': {
                'today': data['t'][0],
                'last_24_hours': data['t'][1]
            },
            'low_price': {
                'today': data['l'][0],
                'last_24_hours': data['l'][1]
            },
            'high_price': {
                'today': data['h'][0],
                'last_24_hours': data['h'][1]
            },
            'opening_price': data['o']
        }
        return mapped_data

    def _map_order_book(self, data):
        """
        Maps the order book data to verbose field names.
        """
        mapped_data = {
            'asks': [self._map_order_book_entry(entry) for entry in data.get('asks', [])],
            'bids': [self._map_order_book_entry(entry) for entry in data.get('bids', [])]
        }
        return mapped_data

    def _map_order_book_entry(self, entry):
        """
        Maps a single order book entry to verbose field names.
        """
        mapped_entry = {
            'price': entry[0],
            'volume': entry[1],
            'timestamp': int(entry[2])
        }
        return mapped_entry


class DataExtractor:
    """
    Responsible for extracting data from the Kraken API.
    """

    def __init__(self, api_client):
        self.api_client = api_client

    def fetch_and_publish_data(self, publisher):
        """
        Fetches data from the API and publishes it to the message queue.
        """
        logger.info("Fetching tradable asset pairs...")
        asset_pairs_info = self.api_client.get_tradable_asset_pairs(CRYPTO_PAIRS)
        if asset_pairs_info is None:
            logger.error("Failed to fetch asset pairs information.")
            return

        logger.info("Fetching ticker information...")
        ticker_info = self.api_client.get_ticker_information(CRYPTO_PAIRS)
        if ticker_info is None:
            logger.error("Failed to fetch ticker information.")
            return

        for pair in CRYPTO_PAIRS:
            try:
                # Get asset pair key (Kraken uses different keys internally)
                pair_key = next((k for k in asset_pairs_info if asset_pairs_info[k]['altname'] == pair.replace('/', '')), None)
                if not pair_key:
                    logger.warning(f"No asset pair key found for {pair}")
                    continue

                asset_pair_data = asset_pairs_info[pair_key]
                ticker_data = ticker_info.get(pair_key)

                if not ticker_data:
                    logger.warning(f"No ticker data found for {pair}")
                    continue

                # Fetch order book data
                logger.info(f"Fetching order book for {pair}...")
                order_book_data = self.api_client.get_order_book(pair_key)
                if order_book_data is None:
                    logger.error(f"Failed to fetch order book for {pair}")
                    continue

                # Get current timestamp
                timestamp = int(time.time())

                # Convert timestamp to different time zones
                utc_time = datetime.utcfromtimestamp(timestamp)
                timestamp_jamaica = (utc_time + timedelta(hours=-5)).strftime('%Y-%m-%d %H:%M:%S')
                timestamp_europe = (utc_time + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
                timestamp_asia = (utc_time + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')

                # Combine all data into a single payload with verbose field names
                payload = {
                    'timestamp': timestamp,
                    'timestamp_jamaica': timestamp_jamaica,
                    'timestamp_europe': timestamp_europe,
                    'timestamp_asia': timestamp_asia,
                    'trading_pair': pair,
                    'asset_pair_info': asset_pair_data,
                    'ticker_info': ticker_data,
                    'order_book': order_book_data.get(pair_key, {})
                }

                # Publish to message queue
                publisher.publish_message(payload)
                logger.info(f"Published data for {pair}")

            except Exception as e:
                logger.error(f"Error processing data for {pair}: {e}")


class MessageQueuePublisher:
    """
    Responsible for publishing messages to the RabbitMQ message queue.
    """

    def __init__(self, host, exchange, routing_key, queue_name):
        self.host = host
        self.exchange = exchange
        self.routing_key = routing_key
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        """
        Establishes a connection to RabbitMQ and declares the necessary exchange and queue.
        """
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=True)
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.routing_key)
            logger.info("Connected to RabbitMQ")
        except pika.exceptions.AMQPError as e:
            logger.error(f"AMQPError while connecting to RabbitMQ: {e}")
            raise

    def publish_message(self, message):
        """
        Publishes a message to the RabbitMQ exchange.
        """
        try:
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2  # Make message persistent
                )
            )
            logger.debug(f"Message published: {message}")
        except pika.exceptions.AMQPError as e:
            logger.error(f"AMQPError while publishing message: {e}")

    def close(self):
        """
        Closes the connection to RabbitMQ.
        """
        if self.connection:
            self.connection.close()
            logger.info("RabbitMQ connection closed")


def main():
    api_client = KrakenAPIClient(KRAKEN_API_URL)
    data_extractor = DataExtractor(api_client)

    try:
        publisher = MessageQueuePublisher(
            host=RABBITMQ_HOST,
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            queue_name=QUEUE_NAME
        )
    except Exception as e:
        logger.error(f"Failed to initialize MessageQueuePublisher: {e}")
        return

    try:
        data_extractor.fetch_and_publish_data(publisher)
    except Exception as e:
        logger.error(f"Error in data extraction process: {e}")
    finally:
        publisher.close()


if __name__ == '__main__':
    main()
