from ..config.settings import Config
import logging
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.catalog import JdbcCatalog
from ..config.settings import Config
import logging
from pathlib import Path
import os
import requests
import logging
import time
from typing import Optional, List

class FlinkManager:
    JDBC_JARS = {
        'flink-jdbc': {
            'name': 'flink-connector-jdbc-3.2.0-1.19.jar',
            'url': 'https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar'
        },
        'postgres': {
            'name': 'postgresql-42.7.5.jar',
            'url': 'https://jdbc.postgresql.org/download/postgresql-42.7.5.jar'
        },
        'kafka': {
            'name': 'flink-connector-kafka-1.17.0.jar',
            'url': 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/1.17.0/flink-connector-kafka-1.17.0.jar'
        },
        'json': {
            'name': 'flink-json-1.16.0.jar',
            'url': 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.16.0/flink-json-1.16.0.jar'
        },
        'json': {
            'name': 'flink-python-1.17.0.jar',
            'url': 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-python/1.17.0/flink-python-1.17.0.jar'
        } 
    }
    
    def __init__(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.enable_checkpointing(Config.FLINK.checkpoint_interval)
        self.env.get_checkpoint_config().set_min_pause_between_checkpoints(
            Config.FLINK.min_pause_between_checkpoints
        )
        
        self.settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        self.t_env = StreamTableEnvironment.create(self.env, environment_settings=self.settings)
        
        self._setup_jdbc_dependencies()
        # self._setup_postgres_catalog()

    def _find_jars(self) -> List[str]:
        """Find required JARs in common locations"""
        jar_paths = []
        search_paths = [
            Path("/opt/flink/lib"),
            Path("/usr/local/lib/flink"),
            Path.home() / ".flink/lib",
            Path.cwd() / "lib"
        ]
        
        for jar_info in self.JDBC_JARS.values():
            jar_found = False
            for path in search_paths:
                jar_path = path / jar_info['name']
                if jar_path.exists():
                    jar_paths.append(str(jar_path))
                    jar_found = True
                    break
                    
            if not jar_found:
                jar_paths.append(self._download_jar(jar_info))
                
        return jar_paths

    def _download_jar(self, jar_info: dict) -> str:
        """Download JAR from repository"""
        target_dir = Path.home() / ".flink/lib"
        target_dir.mkdir(parents=True, exist_ok=True)
        jar_path = target_dir / jar_info['name']
        
        if not jar_path.exists():
            response = requests.get(jar_info['url'])
            response.raise_for_status()
            with open(jar_path, 'wb') as f:
                f.write(response.content)
            
        return str(jar_path)

    def _setup_jdbc_dependencies(self):
        """Configure JDBC dependencies"""
        jar_paths = self._find_jars()
        jar_urls = [f"file://{path}" for path in jar_paths]
        self.t_env.get_config().set("pipeline.jars", ";".join(jar_urls))
        logging.info(f"Configured JARs: {jar_urls}")

    def _setup_postgres_catalog(self):
        """Set up PostgreSQL catalog information"""
        try:
            catalog = JdbcCatalog(
                "postgres_catalog",
                "hespress_db",
                "postgres",
                "postgres",
                "jdbc:postgresql://postgres:5432"
            )
            self.t_env.register_catalog("postgres_catalog", catalog)
            self.t_env.use_catalog("postgres_catalog")
            
            logging.info("PostgreSQL catalog registered and set as active")
            
        except Exception as e:
            logging.error(f"Could not set up PostgreSQL catalog: {str(e)}")
            raise e

    def create_postgres_sink(self):
        """Create JDBC sink connector for PostgreSQL"""
        try:
            # Ensure we're using the postgres catalog
            self.t_env.use_catalog("postgres_catalog")
            self.t_env.use_database("hespress_db")
            
            # Log current catalog and database
            current_catalog = self.t_env.get_current_catalog()
            current_database = self.t_env.get_current_database()
            logging.info(f"Current catalog: {current_catalog}, Current database: {current_database}")
            
            create_table_stmt = """
            CREATE TABLE processed_comments (
                id STRING,
                article_title STRING,
                article_url STRING,
                user_comment STRING,
                topic STRING,
                score DOUBLE,
                created_at TIMESTAMP(3),
                stored_at TIMESTAMP(3),
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector' = 'jdbc',
                'hostname' = 'postgres',
                'port' = '5432',
                'database-name' = 'hespress_db',
                'username' = 'postgres',
                'password' = 'postgres'
            )
            """
            
            # Log the exact SQL being executed
            logging.info(f"Executing table creation SQL: {create_table_stmt}")
            
            # Execute the table creation
            result = self.t_env.execute_sql(create_table_stmt)
            logging.info(f"Table creation result: {result}")
            
            return True
                
        except Exception as e:
            logging.error(f"Error in setting up PostgreSQL sink: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            raise e
    
    def create_kafka_source(self):
        try:
            # Switch to the default catalog (instead of postgres_catalog)
            self.t_env.execute_sql("USE CATALOG postgres_catalog")
            
            # Try to create a database if needed (this might not be supported)
            try:
                self.t_env.execute_sql("CREATE DATABASE IF NOT EXISTS hespress_db")
            except Exception as db_create_error:
                logging.warning(f"Could not create database: {db_create_error}")
            
            # Use the default or existing database
            self.t_env.execute_sql("USE hespress_db")
            
            # Create Kafka source table using default catalog
            return self.t_env.execute_sql("""
                CREATE TABLE IF NOT EXISTS kafka_comments (
                    id STRING,
                    user_comment STRING,
                    topic STRING,
                    article_title STRING,
                    article_url STRING,
                    score INT,
                    created_at TIMESTAMP(3),
                    proctime AS PROCTIME()
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = 'hespress.comments.*',
                    'properties.bootstrap.servers' = 'kafka:9092',
                    'properties.group.id' = 'flink_consumer_group',
                    'format' = 'json',
                    'scan.startup.mode' = 'earliest-offset'
                )
            """)
        except Exception as e:
            logging.error(f"Error creating Kafka source: {str(e)}")
            raise e