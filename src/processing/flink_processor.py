from .flink_manager import FlinkManager
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, Table
from pyflink.common.typeinfo import Types
from ..config.settings import Config
import logging
from typing import List, Dict, Any
from ..storage.postgres_manager import PostgresManager

class FlinkProcessor:
    def __init__(self):
        # Initialize environments
        self._setup_environments()
        self.postgres_manager = PostgresManager()
        
    def _setup_environments(self):
        """Initialize Flink streaming and table environments"""
        try:
            # Initialize environments
            self.stream_env = StreamExecutionEnvironment.get_execution_environment()
            self.stream_env.enable_checkpointing(Config.FLINK.checkpoint_interval)
            self.stream_env.get_checkpoint_config().set_min_pause_between_checkpoints(
                Config.FLINK.min_pause_between_checkpoints
            )
            
            self.table_env = StreamTableEnvironment.create(
                stream_execution_environment=self.stream_env,
                environment_settings=EnvironmentSettings.new_instance().in_streaming_mode().build()
            )
            
            # # Detailed logging of catalog information
            # logging.info("Available catalogs:")
            # try:
            #     catalogs = self.table_env.list_catalogs()
            #     logging.info(f"Catalogs: {catalogs}")
            # except Exception as catalog_error:
            #     logging.error(f"Error listing catalogs: {catalog_error}")
            
            # # Explicitly create the table before processing
            # manager = FlinkManager()
            # try:
            #     manager.create_postgres_sink()
            #     logging.info("PostgreSQL sink created successfully")
            # except Exception as sink_error:
            #     logging.error(f"Failed to create PostgreSQL sink: {sink_error}")
            #     raise
            
            # try:
            #     manager.create_kafka_source()
            #     logging.info("Kafka source created successfully")
            # except Exception as source_error:
            #     logging.error(f"Failed to create Kafka source: {source_error}")
            #     raise
            
        except Exception as e:
            logging.error(f"Error setting up environments: {str(e)}")
            raise

    def process_stream(self, comments: List[Dict[str, Any]]) -> None:
        try:
            # Log the incoming comments
            logging.info(f"Processing {len(comments)} comments")
            
            # Ensure the sink table is created
            FlinkManager().create_postgres_sink()
            
            # Detailed logging for each step
            logging.info("Dropping temporary view if exists")
            self.table_env.execute_sql("DROP TEMPORARY VIEW IF EXISTS temp_comments")
            
            logging.info("Converting comments to DataStream")
            comments_stream = self.stream_env.from_collection(
                collection=comments,
                type_info=Types.ROW_NAMED(
                    ['id', 'article_title', 'article_url', 'user_comment', 'topic', 'score', 'created_at'],
                    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), 
                    Types.STRING(), Types.DOUBLE(), Types.SQL_TIMESTAMP()]
                )
            )
            
            logging.info("Creating Table from DataStream")
            comments_table = self.table_env.from_data_stream(comments_stream)
            
            logging.info("Creating temporary view")
            self.table_env.create_temporary_view("temp_comments", comments_table)

            logging.info("Executing INSERT statement")
            insert_result = self.table_env.execute_sql("""
                INSERT INTO processed_comments
                SELECT 
                    id, article_title, article_url, user_comment, 
                    topic, score, created_at,
                    CURRENT_TIMESTAMP as stored_at
                FROM temp_comments
            """)

            # Log insert result details
            logging.info(f"Insert operation result: {insert_result}")
            logging.info("Comments inserted into temporary table successfully.")

        except Exception as e:
            logging.error(f"Comprehensive error in stream processing: {str(e)}")
            # Log the full stack trace
            import traceback
            logging.error(traceback.format_exc())
            raise e


    def process_batch(self, comments: List[Dict[str, Any]]) -> None:
        """
        Batch processing for historical analysis or model retraining
        """
        try:
            # Drop the temporary view if it already exists
            self.table_env.execute_sql("DROP TEMPORARY VIEW IF EXISTS comments_batch")
            
            # Convert comments to Table
            comments_table = self.table_env.from_elements(
                comments,
                ['id', 'article_title', 'article_url', 'user_comment', 'topic', 'score', 'created_at']
            )
            
            # Register for SQL querying
            self.table_env.create_temporary_view("comments_batch", comments_table)
            
            # Example batch analysis query
            analysis_result = self.table_env.sql_query("""
                SELECT 
                    topic,
                    COUNT(*) as comment_count,
                    AVG(score) as avg_score,
                    MIN(created_at) as earliest_comment,
                    MAX(created_at) as latest_comment
                FROM comments_batch
                GROUP BY topic
            """)
            
            # Execute batch processing
            analysis_result.execute().print()
            
        except Exception as e:
            logging.error(f"Error in batch processing: {str(e)}")
            raise e