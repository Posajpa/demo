import urllib.robotparser
import urllib.request
import urllib.parse
from utils.logger import get_logger
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import json
from config.kafka_client import conf

logger = get_logger(logger_name="checker_logger", log_file='checker.log')


def robots_checker(url, user_agent='*', timeout=5):
    """Check if a URL is allowed to be scraped according to the site's robots.txt file."""
    robots_url = urllib.parse.urljoin(url, '/robots.txt')
    rp = urllib.robotparser.RobotFileParser()

    try:
        with urllib.request.urlopen(robots_url, timeout=timeout) as response:
            robots_txt = response.read().decode('utf-8')
            rp.parse(robots_txt.splitlines())
    except Exception as e:
        logger.error(f"Error checking robots.txt: {e}")
        return False

    return rp.can_fetch(user_agent, url)


def check_article(fetched_article):
    url = fetched_article["url"]
    can_fetch = robots_checker(url)
    return can_fetch


def main():
    # Initialize Kafka Producer and Consumer
    producer = Producer(conf)
    consumer = Consumer({
        **conf,
        'group.id': 'check-consumer-group',
        'auto.offset.reset': 'earliest'
    })

    try:
        logger.info("Starting checking process")
        consumer.subscribe(['fetched_articles'])

        while True:
            msgs = consumer.consume(num_messages=250, timeout=1.0)

            if not msgs:
                continue

            for msg in msgs:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                else:
                    fetched_article = json.loads(json.loads(msg.value().decode('utf-8')))
                    logger.info(f"Received message: {fetched_article}")
                    if check_article(fetched_article):
                        logger.info(f"Scraping for article approved")
                        try:
                            producer.produce(topic='approved_articles', value=json.dumps(fetched_article))
                            logger.info(f"Produced message: {fetched_article}")
                        except Exception as e:
                            logger.error(f"Error producing message: {e}")

                import time
                time.sleep(0.1)

            producer.flush(timeout=10)

    except Exception as e:
        logger.error(f"Error in checking process: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down checking process")

    finally:
        consumer.close()
        producer.flush(timeout=10)


if __name__ == "__main__":
    main()
