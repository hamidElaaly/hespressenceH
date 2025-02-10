from sqlalchemy import create_engine, Column, String, DateTime, Float, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError
from ..config.settings import Config
import logging
from datetime import datetime

class PostgresManager:
    def __init__(self):
        self.engine = create_engine(
            f'postgresql://{Config.POSTGRES.user}:{Config.POSTGRES.password}@'
            f'{Config.POSTGRES.host}:{Config.POSTGRES.port}/{Config.POSTGRES.database}'
        )
        self.metadata = MetaData()
        
        # Define comments table
        self.comments_table = Table(
            'processed_comments', self.metadata,
            Column('id', String, primary_key=True),
            Column('article_title', String),
            Column('article_url', String),
            Column('user_comment', String),
            Column('topic', String),
            Column('score', Float),
            Column('created_at', DateTime),
            Column('stored_at', DateTime)
        )
        
        # Create tables if they don't exist
        try:
            self.metadata.create_all(self.engine)
            logging.info("PostgreSQL tables initialized successfully")
        except SQLAlchemyError as e:
            logging.error(f"Error initializing PostgreSQL tables: {str(e)}")
            raise e

    def store_comments(self, comments):
        """Store raw comments in PostgreSQL"""
        try:
            with self.engine.connect() as connection:
                current_time = datetime.now()
                
                # Prepare comments for insertion
                records = [
                    {
                        'id': comment['id'],
                        'article_title': comment['article_title'],
                        'article_url': comment['article_url'],
                        'user_comment': comment['comment'],
                        'topic': comment['topic'],
                        'score': comment['score'],
                        'created_at': comment['created_at'],
                        'stored_at': current_time
                    }
                    for comment in comments
                ]
                
                # Insert comments
                result = connection.execute(
                    self.comments_table.insert(),
                    records
                )
                
                logging.info(f"Stored {result.rowcount} comments in PostgreSQL")
                return result.rowcount
                
        except SQLAlchemyError as e:
            logging.error(f"Error storing comments in PostgreSQL: {str(e)}")
            raise e 