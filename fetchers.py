from gdeltdoc import GdeltDoc, Filters
from utils.logger import get_logger
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import json
from datetime import datetime
from config.mongodb_client import uri
from utils.mongodb import save_to_mongodb
from config.kafka_client import conf

logger = get_logger(logger_name="fetcher_logger", log_file='fetcher.log')


def gdelt_fetcher(search_parameters):
    """Fetch articles from GDELT based on search filters."""
    gd = GdeltDoc()
    f = Filters(
        keyword=search_parameters.get('keyword'),
        num_records=search_parameters.get('num_records'),
        start_date=search_parameters.get('start_date'),
        end_date=search_parameters.get('end_date'),
        country=search_parameters.get('country'),
        domain=search_parameters.get('domain'),
        domain_exact=search_parameters.get('domain_exact'),
        theme=search_parameters.get('theme'),
        near=search_parameters.get('near'),
        repeat=search_parameters.get('repeat')
    )
    try:
        fetched_articles = gd.article_search(f)
        return fetched_articles
    except Exception as e:
        logger.error(f"Error fetching articles: {e}")
        return None


def fetch_articles(search_parameters):
    fetched_articles = gdelt_fetcher(search_parameters)
    return fetched_articles


def main():
    producer = Producer(conf)
    consumer = Consumer({
        **conf,
        'group.id': 'fetch-consumer-group',
        'auto.offset.reset': 'earliest'
    })

    try:
        logger.info("Starting fetching process")
        consumer.subscribe(['search_parameters'])

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
                    search_parameters = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"Received message: {search_parameters}")
                    fetched_articles = fetch_articles(search_parameters)
                    logger.info(f"{len(fetched_articles)} articles fetched")

                    for _, row in fetched_articles.iterrows():
                        article_message = row.to_dict()
                        article_message["search_parameters"] = search_parameters
                        article_message["search_date"] = datetime.now().strftime('%Y-%m-%d')
                        article_json = json.dumps(article_message)
                        try:
                            producer.produce(topic='fetched_articles', value=json.dumps(article_json))
                            logger.info(f"Produced message: {article_json}")
                            try:
                                save_to_mongodb(uri, article_json, "fetched", article_message["search_parameters"]["keyword"])
                                logger.info(f"Fetched article saved to MongoDB")
                            except Exception as e:
                                logger.error(f"Error saving article to MongoDB: {e}")
                            producer.flush(timeout=10)
                        except Exception as e:
                            logger.error(f"Error producing message: {e}")

                import time
                time.sleep(0.1)

            producer.flush(timeout=10)

    except Exception as e:
        logger.error(f"Error in fetching process: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down fetching process")

    finally:
        consumer.close()
        producer.flush(timeout=10)


if __name__ == "__main__":
    main()
