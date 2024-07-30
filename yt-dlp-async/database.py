import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
from typing import List
# from .video_id import Logging

# Load environment variables from .env file
load_dotenv()

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL')
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, DATABASE_URL)

class DatabaseOperations:
    # @staticmethod
    # def test_db_connection():
    #     if DATABASE_URL:
    #         Logging.logger.info("DATABASE_URL is set")
    #         try:
    #             db_ops = DatabaseOperations()
    #             conn = db_ops.get_db_connection()
    #             DatabaseOperations.release_db_connection(conn)
    #             Logging.logger.info("Database connection successful")
    #         except psycopg2.Error as e:
    #             Logging.logger.error(f"Database connection failed: {e}")
    #     else:
    #         Logging.logger.error("DATABASE_URL is not set")

    @staticmethod
    def get_db_connection():
        # Get a connection from the pool
        return connection_pool.getconn()

    @staticmethod
    def release_db_connection(conn):
        # Release the connection back to the pool
        connection_pool.putconn(conn)

    @staticmethod
    async def get_video_count():
        conn = DatabaseOperations.get_db_connection()
        try:
            query = """
            SELECT COUNT(1) FROM videos_to_be_processed
            """
            with conn.cursor() as cur:
                cur.execute(query)
                count_result = cur.fetchone()[0]  # Fetch the result and extract the first column value
                # Logging.logger.info(f"get_video_count(): videos_to_be_processed: {count_result}")
            conn.commit()
        finally:
            DatabaseOperations.release_db_connection(conn)
        
        return count_result

    @staticmethod
    def get_all_video_ids():
        video_ids = []

        conn = DatabaseOperations.get_db_connection()
        try:
            with conn.cursor() as cur:
                query = "SELECT video_id FROM videos_to_be_processed"
                cur.execute(query)
                result = cur.fetchall()  # Fetch all results
                video_ids = [row[0] for row in result]  # Extract video IDs from the results
            conn.commit()
        finally:
            DatabaseOperations.release_db_connection(conn)
        
        return video_ids

    @staticmethod
    async def insert_video_ids(video_ids: List[str]):
        # Logging.logger.info(f"Attempting to insert video_ids: {len(video_ids)}")
        # Logging.logger.info(f"start insert_video_ids: inserting {video_ids}")
        
        conn = DatabaseOperations.get_db_connection()
        try:
            query = """
            INSERT INTO videos_to_be_processed (video_id)
            VALUES (%s)
            ON CONFLICT (video_id) DO NOTHING
            """
            with conn.cursor() as cursor:
                # Batch insert video IDs
                cursor.executemany(query, [(vid,) for vid in video_ids])

            conn.commit()
    
        finally:
            DatabaseOperations.release_db_connection(conn)

    @staticmethod
    async def insert_video_ids_bulk(id_file_path: str):        
        conn = DatabaseOperations.get_db_connection()
        try:
            # Create a cursor object
            cursor = conn.cursor()

            # SQL statements
            create_temp_table_sql = """
            CREATE TEMP TABLE temp_video_ids (
                video_id text
            );
            """

            copy_sql = """
            COPY temp_video_ids (video_id)
            FROM stdin
            WITH (FORMAT text);
            """

            insert_sql = """
            INSERT INTO videos_to_be_processed (video_id)
            SELECT video_id
            FROM temp_video_ids
            ON CONFLICT (video_id) DO NOTHING;
            """

            drop_temp_table_sql = """
            DROP TABLE temp_video_ids;
            """

            # Execute the SQL statements
            cursor.execute(create_temp_table_sql)

            with open(id_file_path, 'r') as f:
                cursor.copy_expert(sql=copy_sql, file=f)

            cursor.execute(insert_sql)
            cursor.execute(drop_temp_table_sql)

            conn.commit()
    
        finally:
            DatabaseOperations.release_db_connection(conn)

    @staticmethod
    async def insert_video_id_metadata(video_ids: List[str]):
        # Logging.logger.info(f"Attempting to insert video_ids: {len(video_ids)}")
        # Logging.logger.info(f"start insert_video_ids: inserting {video_ids}")
        
        conn = DatabaseOperations.get_db_connection()
        try:
            query = """
            INSERT INTO videos_to_be_processed (video_id)
            VALUES (%s)
            ON CONFLICT (video_id) DO NOTHING
            """
            with conn.cursor() as cursor:
                # Batch insert video IDs
                cursor.executemany(query, [(vid,) for vid in video_ids])

            conn.commit()
    
        finally:
            DatabaseOperations.release_db_connection(conn)

