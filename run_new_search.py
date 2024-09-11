import yaml
import sys
import json
from datetime import datetime, timedelta
from confluent_kafka import Producer, KafkaException
from utils.logger import get_logger

logger = get_logger(logger_name="new_search_logger", log_file='new_search.log')


def load_config(config_file):
    """Load configuration from a YAML file."""
    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        logger.info(f"Loaded config from {config_file}")
        return config
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)


def generate_messages_and_send(producer, config):
    """Generate Kafka messages based on the configuration and send them."""
    keywords = config['keywords']
    num_records = config['num_records']
    countries = config['countries']
    domain = config.get('domain', None)
    domain_exact = config.get('domain_exact', None)
    theme = config.get('theme', None)
    near = config.get('near', None)
    repeat = config.get('repeat', None)

    for keyword in keywords:
        for country in countries:
            start_date = datetime.now() - timedelta(days=365*5)
            start_date = datetime.now() - timedelta(days=2)     # temporarily

            while start_date < datetime.now():
                end_date = start_date + timedelta(days=1)
                start_date_str = start_date.strftime('%Y-%m-%d')
                end_date_str = end_date.strftime('%Y-%m-%d')

                message = {
                    "keyword": keyword,
                    "num_records": num_records,
                    "start_date": start_date_str,
                    "end_date": end_date_str,
                    "country": country,
                    "domain": domain,
                    "domain_exact": domain_exact,
                    "theme": theme,
                    "near": near,
                    "repeat": repeat
                }

                try:
                    producer.produce(topic='search_parameters', value=json.dumps(message))
                    logger.info(f"Produced message: {message}")
                except KafkaException as e:
                    logger.error(f"Error producing message: {e}")

                start_date = end_date

    producer.flush(timeout=10)
    logger.info("All messages flushed to Kafka")


def main():
    if len(sys.argv) != 2:
        logger.error("Usage: python generate_search.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    config = load_config(config_file)

    from config.kafka_client import conf
    producer = Producer(conf)

    generate_messages_and_send(producer, config)


if __name__ == "__main__":
    main()
