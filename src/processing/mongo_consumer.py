from kafka import KafkaConsumer
import json
from pymongo import MongoClient
from ..config.settings import Config
from kafka.errors import NoBrokersAvailable
import time
import logging
from datetime import datetime
from .flink_processor import FlinkProcessor
import concurrent.futures
from tensorflow.keras.models import load_model
import numpy as np
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
import pickle

class MongoDBConsumer:
    def __init__(self, max_retries=3):
        self.client = None
        self.consumer = None
        self.connect_mongo_with_retry(max_retries)
        self.connect_kafka_with_retry(max_retries)
        self.flink_processor = FlinkProcessor()
        self.shutdown_flag = False

        # Load Sentiment Analysis Model
        self.model = load_model("src/model/model.h5")

        # Load Tokenizer (must be the same used for training)
        with open("src/model/tokenizer.pickle", "rb") as handle:
            self.tokenizer = pickle.load(handle)

        self.max_length = 100  # Adjust based on training

        # Define sentiment classes
        self.sentiment_classes = ["negative", "neutral", "positive"]

    def shutdown(self):
        """Handle shutdown signal."""
        logging.info("Shutdown signal received.")
        self.shutdown_flag = True
        self.cleanup()

    def cleanup(self):
        """Cleanup resources."""
        if self.client:
            self.client.close()
            logging.info("MongoDB client closed.")
        if self.consumer:
            self.consumer.close()
            logging.info("Kafka consumer closed.")

    def connect_mongo_with_retry(self, max_retries):
        for i in range(max_retries):
            try:
                self.client = MongoClient(
                    Config.MONGO.uri,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                    socketTimeoutMS=5000,
                    waitQueueTimeoutMS=5000
                )
                self.db = self.client[Config.MONGO.database]
                self.comments_collection = self.db['comments']
                
                logging.info("Testing MongoDB connection...")
                self.client.admin.command('ping')
                logging.info("Successfully connected to MongoDB")
                
                if hasattr(Config.KAFKA, 'topics_prefix'):
                    self._setup_kafka_consumer()
                return
                
            except Exception as e:
                if i == max_retries - 1:
                    raise Exception(f"Failed to connect to MongoDB after {max_retries} attempts: {str(e)}")
                logging.warning(f"Failed to connect to MongoDB, retrying in 5 seconds... ({i+1}/{max_retries})")
                time.sleep(5)

    def connect_kafka_with_retry(self, max_retries):
        if not hasattr(Config.KAFKA, 'topics_prefix'):
            logging.info("Kafka topics_prefix not configured, skipping Kafka setup")
            return

        for i in range(max_retries):
            try:
                self._setup_kafka_consumer()
                logging.info("Successfully connected to Kafka")
                return
            except NoBrokersAvailable as e:
                if i == max_retries - 1:
                    raise Exception(f"Failed to connect to Kafka after {max_retries} attempts: {str(e)}")
                logging.warning(f"Failed to connect to Kafka, retrying in 5 seconds... ({i+1}/{max_retries})")
                time.sleep(5)

    def _setup_kafka_consumer(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=Config.KAFKA.bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='mongo_consumer_group'
        )
        topic_pattern = f'^{Config.KAFKA.topics_prefix}.*'
        self.consumer.subscribe(pattern=topic_pattern)

    def predict_sentiment(self, text):
        """Predict sentiment of a given text using the trained model."""
        sequences = self.tokenizer.texts_to_sequences([text])
        padded_sequences = pad_sequences(sequences, maxlen=self.max_length)

        prediction = self.model.predict(padded_sequences)
        sentiment_index = np.argmax(prediction)  # Get class with highest probability
        return self.sentiment_classes[sentiment_index]

    def save_comments(self, comments):
        """Save comments to MongoDB and process with Flink in parallel."""
        if self.shutdown_flag:
            logging.warning("Shutdown initiated, skipping comment processing.")
            return 0

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                mongo_future = executor.submit(self._save_to_mongo, comments)
                stream_future = executor.submit(self.flink_processor.process_stream, comments)

                # Wait for all futures to complete
                concurrent.futures.wait([mongo_future, stream_future], return_when=concurrent.futures.ALL_COMPLETED)

                for future in [mongo_future, stream_future]:
                    if future.exception():
                        raise future.exception()

                return mongo_future.result()

        except Exception as e:
            logging.error(f"Error processing comments: {str(e)}")
            raise e

    def _save_to_mongo(self, comments):
        """Preprocess comments, predict sentiment, and save to MongoDB."""
        comments_to_insert = []
        for comment in comments:
            sentiment = self.predict_sentiment(comment.get('comment', ""))
            comment_data = {
                'id': comment.get('id'),
                'article_title': comment.get('article_title'),
                'article_url': comment.get('article_url'),
                'user_comment': comment.get('comment'),
                'topic': comment.get('topic'),
                'score': comment.get('score'),
                'created_at': comment.get('created_at'),
                'sentiment': sentiment  # Append sentiment prediction
            }
            self.comments_collection.update_one(
                {'id': comment_data['id']},
                {'$set': comment_data},
                upsert=True
            )
        logging.info(f"Upserted {len(comments)} comments into MongoDB with sentiment analysis")
        return len(comments)
