# dashboard.py
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import pandas as pd
import time
import os
from pymongo import MongoClient
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from ..config.settings import MongoConfig  # Import MongoDB settings

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class DashboardApp:
    def __init__(self):
        self.app = Flask(__name__, static_folder='src/dashboard')
        self.setup_app()
        self.df = None
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.csv_path = os.getenv('CSV_PATH', 'src/dashboard/hespress_dataset.csv')

        # MongoDB Connection (using settings.py)
        self.mongo_client = MongoClient(MongoConfig.uri)
        self.db = self.mongo_client[MongoConfig.database]
        self.comments_collection = self.db[MongoConfig.collection]

        self.load_dataset()

    def setup_app(self):
        """Set up Flask app, CORS, and SocketIO."""
        CORS(self.app)
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")
        self.setup_routes()
        self.setup_socket_handlers()

    def load_dataset(self):
        """Load dataset from CSV on startup."""
        if os.path.exists(self.csv_path):
            self.df = pd.read_csv(self.csv_path)
        else:
            self.df = pd.DataFrame(columns=['id', 'article_title', 'article_url', 'user_comment', 'topic', 'score', 'created_at', 'sentiment'])

    def fetch_new_comments(self):
        """Fetch new comments from MongoDB, keep only required columns, and upsert into the CSV."""
        if self.df is None:
            logger.warning("Dataset not loaded")
            return

        # Define the required columns
        required_columns = ["id", "comment", "topic", "article_title", "article_url", "score", "sentiment"]

        # Fetch new comments from MongoDB (no timestamp filtering)
        new_comments = list(self.comments_collection.find({}))

        if new_comments:
            logger.info(f"Fetched {len(new_comments)} new comments from MongoDB")

            # Convert to DataFrame
            new_df = pd.DataFrame(new_comments)

            # Drop MongoDB '_id' field
            new_df = new_df.drop(columns=["_id"], errors="ignore")

            # Keep only the required columns, filling missing ones with empty values
            new_df = new_df.reindex(columns=required_columns, fill_value="")

            # Perform upsert to avoid duplicate IDs
            self.df = pd.concat([self.df, new_df]).drop_duplicates(subset=['id'], keep='last')

            # Save updated DataFrame to CSV
            self.df.to_csv(self.csv_path, index=False)

            logger.info(f"Updated CSV with {len(new_comments)} new unique comments")



    def analyze_data(self):
        """Analyze data for visualization."""
        if self.df is None or self.df.empty:
            return [], {}

        topic_stats = self.df.groupby('topic').agg({'id': 'count', 'score': ['sum', 'mean']}).reset_index()
        topic_stats.columns = ['topic', 'count', 'score_sum', 'score_mean']
        topic_stats = topic_stats.sort_values('count', ascending=False)

        trending = []
        for _, row in topic_stats.iterrows():
            topic_sentiments = self.df[self.df['topic'] == row['topic']]['sentiment'].value_counts()

            trending.append({
                'topic': row['topic'],
                'times_mentioned': int(row['count']),
                'score_sum': float(row['score_sum']),
                'mean_score': float(row['score_mean']),
                'positive_count': int(topic_sentiments.get('positive', 0)),
                'negative_count': int(topic_sentiments.get('negative', 0)),
                'neutral_count': int(topic_sentiments.get('neutral', 0))
            })

        sentiment_counts = self.df['sentiment'].value_counts()
        total_count = len(self.df)
        insights = {
            sentiment: {
                'percentage': round((sentiment_counts.get(sentiment, 0) / total_count) * 100, 2),
                'total': int(sentiment_counts.get(sentiment, 0))
            }
            for sentiment in ['positive', 'negative', 'neutral']
        }

        return trending, insights

    def setup_routes(self):
        """Define API routes."""
        @self.app.route('/')
        def serve_dashboard():
            return send_from_directory(os.path.abspath("src/dashboard"), "index.html")

        @self.app.route('/visualization')
        def serve_d3():
            return send_from_directory(os.path.abspath("src/dashboard"), "visualization.js")

        @self.app.route("/api/sentiment", methods=["GET"])
        def get_sentiment_data():
            """Fetch and return analyzed data."""
            try:
                trending, insights = self.analyze_data()
                return jsonify({"trending": trending, "insights": insights})
            except Exception as e:
                logger.error(f"Error processing sentiment data: {e}")
                return jsonify({"error": str(e)}), 500

    def setup_socket_handlers(self):
        """Handle real-time data streaming."""
        @self.socketio.on('connect')
        def handle_connect():
            logger.info("Client connected")
            self.executor.submit(self.process_real_time_data)

    def process_real_time_data(self):
        """Fetch new data, update CSV, and emit real-time updates."""
        while True:
            try:
                self.fetch_new_comments()
                trending, insights = self.analyze_data()
                self.socketio.emit("real_time_data", {"trending": trending, "insights": insights})
                time.sleep(5)  # Fetch new data every 5 seconds
            except Exception as e:
                logger.error(f"Error in data processing: {e}")
                time.sleep(1)

    def run(self, debug=False):
        """Start the Flask-SocketIO server."""
        self.socketio.run(self.app, debug=debug)

if __name__ == "__main__":
    dashboard = DashboardApp()
    dashboard.run(debug=True)
