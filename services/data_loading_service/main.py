# data_loading_service/main.py

import json
import logging
import sys
from datetime import datetime

import pika
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from models import Base, DimCrypto, DimDate, FactPriceHistory

# Configuration
DATABASE_URI = 'postgresql://user:password@localhost:5432/cryptodb'  # Update with actual credentials
RABBITMQ_HOST = 'localhost'  # Update if RabbitMQ is on a different host
QUEUE_NAME = 'transformed_data'

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('DataLoadingService')


class DataLoader:
    """
    Responsible for loading data into the data warehouse.
    """

    def __init__(self, session):
        self.session = session

    def load_data(self, data):
        """
        Load transformed data into the database.
        """
        try:
            # Parse data
            date_str = data['date']
            crypto_symbol = data['symbol']
            crypto_name = data['name']
            market_cap = data.get('market_cap')

            open_price = data['open_price']
            close_price = data['close_price']
            high_price = data['high_price']
            low_price = data['low_price']
            volume = data['volume']

            # Convert date string to date object
            data_date = datetime.strptime(date_str, '%Y-%m-%d').date()

            # Get or create DimDate
            dim_date = self._get_or_create_date(data_date)

            # Get or create DimCrypto
            dim_crypto = self._get_or_create_crypto(crypto_symbol, crypto_name, market_cap)

            # Insert FactPriceHistory
            fact_price_history = FactPriceHistory(
                date_id=dim_date.date_id,
                crypto_id=dim_crypto.crypto_id,
                open_price=open_price,
                close_price=close_price,
                high_price=high_price,
                low_price=low_price,
                volume=volume
            )
            self.session.add(fact_price_history)
            self.session.commit()
            logger.info(f"Inserted data for {crypto_symbol} on {data_date}")

        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"Database error: {e}")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Unexpected error: {e}")

    def _get_or_create_date(self, data_date):
        """
        Retrieve or create a date dimension record.
        """
        dim_date = self.session.query(DimDate).filter_by(date=data_date).first()
        if not dim_date:
            dim_date = DimDate(date=data_date)
            self.session.add(dim_date)
            self.session.commit()
            logger.info(f"Created DimDate for {data_date}")
        return dim_date

    def _get_or_create_crypto(self, symbol, name, market_cap):
        """
        Retrieve or create a cryptocurrency dimension record.
        """
        dim_crypto = self.session.query(DimCrypto).filter_by(symbol=symbol).first()
        if not dim_crypto:
            dim_crypto = DimCrypto(name=name, symbol=symbol, market_cap=market_cap)
            self.session.add(dim_crypto)
            self.session.commit()
            logger.info(f"Created DimCrypto for {symbol}")
        return dim_crypto


def main():
    # Set up database connection
    engine = create_engine(DATABASE_URI)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    data_loader = DataLoader(session)

    # Set up RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    def callback(ch, method, properties, body):
        try:
            message = body.decode('utf-8')
            data = json.loads(message)
            logger.debug(f"Received message: {data}")
            data_loader.load_data(data)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    logger.info('Data Loading Service started. Waiting for messages...')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info('Service interrupted by user')
    except Exception as e:
        logger.error(f"Service error: {e}")
    finally:
        channel.close()
        connection.close()
        session.close()


if __name__ == '__main__':
    main()
