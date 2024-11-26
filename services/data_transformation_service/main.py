# data_transformation_service/main.py

import json
import logging
import sys

import pika

from utils import validate_data, normalize_data, enrich_data

# Configuration
RABBITMQ_HOST = 'localhost'  # Update if RabbitMQ is on a different host
RAW_DATA_QUEUE = 'raw_data'
TRANSFORMED_DATA_QUEUE = 'transformed_data'

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('DataTransformationService')


class DataTransformer:
    """
    Responsible for transforming raw data: validation, normalization, enrichment.
    """

    def process_data(self, raw_data):
        """
        Process the raw data through validation, normalization, and enrichment.
        """
        if not validate_data(raw_data):
            logger.error(f"Validation failed for data: {raw_data}")
            return None

        normalized_data = normalize_data(raw_data)
        if normalized_data is None:
            logger.error(f"Normalization failed for data: {raw_data}")
            return None

        enriched_data = enrich_data(normalized_data)
        if enriched_data is None:
            logger.error(f"Enrichment failed for data: {raw_data}")
            return None

        return enriched_data


def main():
    # Set up RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=RAW_DATA_QUEUE, durable=True)
    channel.queue_declare(queue=TRANSFORMED_DATA_QUEUE, durable=True)

    data_transformer = DataTransformer()

    def callback(ch, method, properties, body):
        try:
            message = body.decode('utf-8')
            raw_data = json.loads(message)
            logger.info(f"Received raw data: {raw_data}")
            transformed_data = data_transformer.process_data(raw_data)
            if transformed_data:
                # Publish to transformed_data queue
                channel.basic_publish(
                    exchange='',
                    routing_key=TRANSFORMED_DATA_QUEUE,
                    body=json.dumps(transformed_data)
                )
                logger.info(f"Published transformed data: {transformed_data}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=RAW_DATA_QUEUE, on_message_callback=callback)

    logger.info('Data Transformation Service started. Waiting for messages...')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info('Service interrupted by user')
    except Exception as e:
        logger.error(f"Service error: {e}")
    finally:
        channel.close()
        connection.close()


if __name__ == '__main__':
    main()
