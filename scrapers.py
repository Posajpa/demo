from newspaper import Article
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import json
from config.kafka_client import conf
from utils.logger import get_logger
from config.mongodb_client import uri
from utils.mongodb import save_to_mongodb
from utils.image import hash_image
import nltk
nltk.download('punkt_tab')

logger = get_logger(logger_name="scraper_logger", log_file='scraper.log')


def newspaper3k_scraper(url):
    """Scrape article data from a given URL using the newspaper3k library."""
    url = url.strip()
    article = Article(url)
    try:
        article.download()
        article.parse()
        article.nlp()
        return {
            'title': article.title,
            'authors': article.authors,
            'publish_date': article.publish_date.isoformat() if article.publish_date else None,
            'text': article.text,
            'summary': article.summary,
            'keywords': article.keywords,
            'source_url': article.source_url,
            'image_url': article.top_image
        }
    except Exception as e:
        logger.error(f"Error scraping article: {e}")
        return None


def scrape_article(approved_article):
    url = approved_article["url"]
    scraped_articled = newspaper3k_scraper(url)
    return scraped_articled


def main():
    # Initialize Kafka Producer and Consumer
    producer = Producer(conf)
    consumer = Consumer({
        **conf,
        'group.id': 'scrape-consumer-group',
        'auto.offset.reset': 'earliest'
    })

    try:
        logger.info("Starting scraping process")
        consumer.subscribe(['approved_articles'])

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
                    approved_article = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"Received message: {approved_article}")
                    scraped_article = scrape_article(approved_article)
                    if scraped_article:
                        logger.info(f"Article scraped")
                        scraped_article["unscraped_data"] = approved_article
                        try:
                            img_hash = hash_image(scraped_article["image_url"])
                            scraped_article["image_hash"] = img_hash
                            logger.info(f"Hashed image: {img_hash}")
                        except Exception as e:
                            logger.error(f"Error hashing image: {e}")
                        try:
                            save_to_mongodb(uri, scraped_article, "scraped",
                                            scraped_article["unscraped_data"]["search_parameters"]["keyword"])
                            logger.info(f"Scraped article saved to MongoDB: : {scraped_article}")
                        except Exception as e:
                            logger.error(f"Error saving article to MongoDB: {e}")

    except Exception as e:
        logger.error(f"Error in checking process: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down scraping process")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()