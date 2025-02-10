from datetime import datetime
import feedparser
import time
import logging
import json
from kafka import KafkaProducer
from ..processing.mongo_consumer import MongoDBConsumer
from .scraper_rss import HespressScraper

class HespressDataCollector:
    def __init__(self):
        self.scraper = HespressScraper()
        self.connect_kafka_with_retry(3)
        self.mongo_consumer = MongoDBConsumer(max_retries=3)

    def collect_article_comments(self, article):
        comments = self.scraper.get_comments(article['url'], article['title'])
        if comments:
            num_saved = self.mongo_consumer.save_comments(comments)
            logging.info(f"Saved {num_saved} comments for article: {article['title']}")

    def collect_comments(self):
        try:
            feed = feedparser.parse('https://www.hespress.com/feed')
            
            for entry in feed.entries:
                article = {
                    'url': entry.link,
                    'title': entry.title.replace('"', '').replace('"', '').replace('"', '').strip()
                }
                self.collect_article_comments(article)
                time.sleep(1)  # Be nice to the server
                
        except Exception as e:
            logging.error(f"Error collecting comments: {str(e)}")

    def connect_kafka_with_retry(self, max_retries):
        retries = 0
        while retries < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=['kafka:9092'],
                    api_version=(0, 10, 1),
                    request_timeout_ms=30000,
                    max_block_ms=30000,
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
                logging.info("Successfully connected to Kafka")
                return
            except Exception as e:
                retries += 1
                logging.error(f"Failed to connect to Kafka, retrying in 5 seconds... ({retries}/{max_retries})")
                logging.error(f"Error: {str(e)}")
                time.sleep(5)
        
        raise Exception("Failed to connect to Kafka after maximum retries")

    def shutdown(self):
        """Shutdown the data collector."""
        logging.info("Shutting down data collector.")
        self.mongo_consumer.shutdown()  # Ensure MongoDB consumer is shut down

    def __del__(self):
        if hasattr(self, 'producer'):
            self.producer.close()