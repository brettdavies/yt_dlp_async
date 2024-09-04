# Standard Libraries
import os
import datetime
import asyncio
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from sshtunnel import SSHTunnelForwarder
from typing import Any, Dict, List, Optional, Tuple

# Logging and Configuration
from loguru import logger
from dotenv import load_dotenv

# Third Party Libraries
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# SSH and database configuration
SSH_HOST = os.environ.get('SSH_HOST')
SSH_PORT = int(os.environ.get('SSH_PORT', 22))
SSH_USER = os.environ.get('SSH_USER')
SSH_KEY_PATH = os.environ.get('SSH_KEY_PATH')
REMOTE_BIND_ADDRESS = ('127.0.0.1', 5432)

DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME')

class DatabaseOperations:
    # Initialize the SSH tunnel
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=REMOTE_BIND_ADDRESS
    )
    tunnel.start()

    # Initialize the connection pool
    connection_pool = pool.SimpleConnectionPool(
        minconn=1,
        maxconn=10,  # Adjust the max connections based on your requirements
        user=DB_USER,
        password=DB_PASSWORD,
        host='127.0.0.1',
        port=tunnel.local_bind_port,
        database=DB_NAME
    )

    @staticmethod
    @contextmanager
    def get_db_connection():
        """
        Context manager for database connection.
        Ensures that the connection is properly closed.
        """
        conn = None
        try:
            conn = DatabaseOperations.connection_pool.getconn()
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Error obtaining database connection: {e}")
            raise
        finally:
            if conn:
                DatabaseOperations.connection_pool.putconn(conn)

    @staticmethod
    def close_connection_pool():
        """
        Close all connections in the connection pool.
        """
        DatabaseOperations.connection_pool.closeall()

    @staticmethod
    async def execute_query(query: str, params: Tuple = ()) -> List[Tuple]:
        """
        Helper function to execute a query with error handling and logging.

        Args:
            query (str): The SQL query to execute.
            params (Tuple): The parameters for the SQL query.

        Returns:
            List[Tuple]: The result of the query.
        """
        with DatabaseOperations.get_db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    result = cursor.fetchall()
                conn.commit()
                return result
            except psycopg2.Error as e:
                logger.error(f"SQL Error: {e}")
                conn.rollback()
                return []
            except Exception as e:
                logger.error(f"An error occurred: {e}")
                conn.rollback()
                return []

    @staticmethod
    async def get_count_videos_to_be_processed() -> Optional[int]:
        """
        Retrieves the count of videos to be processed from the database.

        Returns:
            int: The count of videos to be processed.
            None: If an error occurs during the retrieval process.
        """
        with DatabaseOperations.get_db_connection() as conn:
            try:
                query = """
                SELECT COUNT(1) FROM yt_videos_to_be_processed
                """
                with conn.cursor() as cur:
                    cur.execute(query)
                    count_result = cur.fetchone()[0]  # Fetch the result and extract the first column value
                conn.commit()
            except psycopg2.Error as e:
                logger.error(f"SQL Error when retrieving the count of videos to be processed:\n{e}")
                return None
        
        return count_result

    @staticmethod
    async def get_video_ids_without_metadata() -> List[str]:
        """
        Retrieves a list of video IDs without metadata from the database.

        Args:
            forbidden_queue (asyncio.Queue): A queue containing forbidden video IDs.

        Returns:
            List[str]: A list of video IDs without metadata.

        Raises:
            Exception: If an error occurs during the database operation.
        """
        video_ids: List[str] = []

        with DatabaseOperations.get_db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    query = f"""
                        SELECT video_id 
                        FROM yt_videos_to_be_processed 
                        WHERE has_failed_metadata = FALSE
                        ORDER BY RANDOM() 
                        LIMIT 50
                    """
                    # Execute the query with parameters
                    cursor.execute(query)
                    result = cursor.fetchall()  # Fetch all results
                    video_ids = [row[0] for row in result]  # Extract video IDs from the results
                conn.commit()
            except psycopg2.Error as e:
                logger.error(f"SQL Error when retrieving the video ids without metadata:\n{e}")
                return False
        return video_ids

    @staticmethod
    async def set_video_id_failed_metadata_true(video_ids: set[str]):
        with DatabaseOperations.get_db_connection() as conn:
            try:
                query = """
                UPDATE yt_videos_to_be_processed
                SET has_failed_metadata = TRUE
                WHERE video_id = %s
                """
                with conn.cursor() as cursor:
                    # Batch insert video IDs
                    cursor.executemany(query, [(vid,) for vid in video_ids])
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"SQL Error when updating has_failed_metadata:\n{e}")

    @staticmethod
    async def insert_video_ids(video_ids_batch: List[str]):
        """
        Insert a batch of video IDs into the database.

        Args:
            video_ids_batch (List[str]): The batch of video IDs to insert.
        """
        with DatabaseOperations.get_db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    insert_query = """
                    INSERT INTO yt_video_ids (video_id)
                    VALUES (%s)
                    ON CONFLICT (video_id) DO NOTHING;
                    """
                    cursor.executemany(insert_query, [(video_id,) for video_id in video_ids_batch])
                    conn.commit()
                    logger.info(f"Inserted {len(video_ids_batch)} video IDs into the database.")
            except psycopg2.Error as e:
                logger.error(f"SQL Error when inserting video ids:\n{e}")

    @staticmethod
    async def insert_video_ids_bulk(id_file_path: str):
        """
        Insert video IDs in bulk from a file into the 'yt_videos_to_be_processed' table.

        Parameters:
        - id_file_path (str): The file path of the file containing the video IDs.

        Returns:
        None
        """
        with DatabaseOperations.get_db_connection() as conn:
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
                INSERT INTO yt_videos_to_be_processed (video_id)
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
            except psycopg2.Error as e:
                logger.error(f"SQL Error when writing bulk video ids from: {id_file_path}:\n{e}")
                conn.rollback()

    @staticmethod
    async def insert_update_video_metadata(metadata: Dict[str, Any]):
        """
        Inserts or updates video metadata in the database.

        Args:
            metadata (Dict[str, Any]): A dictionary containing the video metadata.

        Returns:
            None
        """
        with DatabaseOperations.get_db_connection() as conn:
            try:
                # Create a cursor object
                cursor = conn.cursor()

                # Metadata Table
                sql_metadata = """
                INSERT INTO yt_metadata (
                    video_id, kind, etag, title, description, published_at,
                    channel_id, channel_title, category_id, live_broadcast_content,
                    default_language, default_audio_language, duration, dimension,
                    definition, caption, licensed_content, projection,
                    upload_status, privacy_status, license, embeddable,
                    public_stats_viewable, made_for_kids, view_count, like_count,
                    dislike_count, favorite_count, comment_count
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s
                ) ON CONFLICT (video_id) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    etag = EXCLUDED.etag,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    published_at = EXCLUDED.published_at,
                    channel_id = EXCLUDED.channel_id,
                    channel_title = EXCLUDED.channel_title,
                    category_id = EXCLUDED.category_id,
                    live_broadcast_content = EXCLUDED.live_broadcast_content,
                    default_language = EXCLUDED.default_language,
                    default_audio_language = EXCLUDED.default_audio_language,
                    duration = EXCLUDED.duration,
                    dimension = EXCLUDED.dimension,
                    definition = EXCLUDED.definition,
                    caption = EXCLUDED.caption,
                    licensed_content = EXCLUDED.licensed_content,
                    projection = EXCLUDED.projection,
                    upload_status = EXCLUDED.upload_status,
                    privacy_status = EXCLUDED.privacy_status,
                    license = EXCLUDED.license,
                    embeddable = EXCLUDED.embeddable,
                    public_stats_viewable = EXCLUDED.public_stats_viewable,
                    made_for_kids = EXCLUDED.made_for_kids,
                    view_count = EXCLUDED.view_count,
                    like_count = EXCLUDED.like_count,
                    dislike_count = EXCLUDED.dislike_count,
                    favorite_count = EXCLUDED.favorite_count,
                    comment_count = EXCLUDED.comment_count;
                """.format(**metadata)

                # Tags Table
                sql_tags = """
                INSERT INTO yt_tags (
                    video_id, tag
                ) VALUES (
                    %s, %s
                ) ON CONFLICT (video_id, tag) DO UPDATE SET
                    tag = EXCLUDED.tag;
                """

                # Thumbnails Table
                sql_thumbnails = """
                INSERT INTO yt_thumbnails (
                    video_id, thumbnail_size, url, width, height
                ) VALUES
                    (%s, 'default', %s, %s, %s),
                    (%s, 'medium', %s, %s, %s),
                    (%s, 'high', %s, %s, %s),
                    (%s, 'standard', %s, %s, %s),
                    (%s, 'maxres', %s, %s, %s)
                ON CONFLICT (video_id, thumbnail_size) DO UPDATE SET
                    url = EXCLUDED.url,
                    width = EXCLUDED.width,
                    height = EXCLUDED.height;
                """

                # Localized Info Table
                sql_localized = """
                INSERT INTO yt_localized_info (
                    video_id, language, title, description
                ) VALUES (
                    %s, %s, %s, %s
                ) ON CONFLICT (video_id, language) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description;
                """

                # Topic Categories Table
                sql_categories = """
                INSERT INTO yt_topic_categories (
                    video_id, category
                ) VALUES (
                    %s, %s
                ) ON CONFLICT (video_id, category) DO NOTHING;
                """

                cursor.execute(sql_metadata, (
                    metadata['video_id'], metadata['kind'], metadata['etag'], metadata['title'], metadata['description'], metadata['published_at'],
                    metadata['channel_id'], metadata['channel_title'], metadata['category_id'], metadata['live_broadcast_content'],
                    metadata['default_language'], metadata['default_audio_language'], metadata['duration'], metadata['dimension'],
                    metadata['definition'], metadata['caption'], metadata['licensed_content'], metadata['projection'],
                    metadata['upload_status'], metadata['privacy_status'], metadata['license'], metadata['embeddable'],
                    metadata['public_stats_viewable'], metadata['made_for_kids'], metadata['view_count'], metadata['like_count'],
                    metadata['dislike_count'], metadata['favorite_count'], metadata['comment_count']
                ))
                for tag in metadata['tags']:
                    cursor.execute(sql_tags, (metadata['video_id'], tag))
                cursor.execute(sql_thumbnails, (
                    metadata['video_id'], metadata['default_url'], metadata['default_width'], metadata['default_height'],
                    metadata['video_id'], metadata['medium_url'], metadata['medium_width'], metadata['medium_height'],
                    metadata['video_id'], metadata['high_url'], metadata['high_width'], metadata['high_height'],
                    metadata['video_id'], metadata['standard_url'], metadata['standard_width'], metadata['standard_height'],
                    metadata['video_id'], metadata['maxres_url'], metadata['maxres_width'], metadata['maxres_height']
                ))
                cursor.execute(sql_localized, (
                    metadata['video_id'], metadata['language'], metadata['localized_title'], metadata['localized_description']
                ))
                for category in metadata['topic_category']:
                    cursor.execute(sql_categories, (metadata['video_id'], category))

                conn.commit()
            except psycopg2.Error as e:
                logger.error(f"SQL Error when writing metadata for video {metadata['video_id']}:\n{e}")
                conn.rollback()

    @staticmethod
    async def save_events(df: pd.DataFrame):
        """
        Insert events into the 'e_events' table in the database.

        Args:
            df (pd.DataFrame): The DataFrame containing the events data.

        Raises:
            ValueError: If the DataFrame does not contain all the required columns.

        Returns:
            None
        """
        # Ensure the DataFrame has the required columns
        required_columns = ['event_id', 'date', 'type', 'short_name', 'home_team', 'away_team', 'home_team_normalized','away_team_normalized']
        if not all(column in df.columns for column in required_columns):
            raise ValueError(f"DataFrame must contain the following columns: {', '.join(required_columns)}")

        # Extract the data to be inserted
        records = df[required_columns].values.tolist()
        with DatabaseOperations.get_db_connection() as conn:
            try:
                query = """
                INSERT INTO e_events (event_id, date, type, short_name, home_team, away_team, home_team_normalized, away_team_normalized)
                VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (event_id) DO NOTHING
                """
                with conn.cursor() as cursor:
                    # Batch insert records using parameterized queries to prevent SQL injection
                    cursor.executemany(query, records)
                conn.commit()
            except psycopg2.Error as e:
                logger.error(f"SQL Error when writing events:\n{e}")
                conn.rollback()

    @staticmethod
    def check_if_existing_e_events_by_date(date_obj: datetime):
        """
        Check if there are existing e_events with the given date.

        Args:
            date_obj (datetime): The date to check for existing e_events.
        Returns:
            bool: True if there are existing e_events with the given date, False otherwise.
        """
        with DatabaseOperations.get_db_connection() as conn:
            try:
                query = """
                SELECT COUNT(1)
                FROM e_events
                WHERE date::date = %s::date
                """
                with conn.cursor() as cursor:
                    cursor.execute(query, (date_obj,))
                    count_result = cursor.fetchone()[0]  # Fetch the result and extract the first column value
                conn.commit()
            except psycopg2.Error as e:
                logger.error(f"SQL Error when retrieving events with date_obj: {date_obj}:\n{e}")
                return False
                
            return (count_result > 0)
    
    @staticmethod
    async def get_e_events_team_info(date_obj: datetime, opposing_team: str, is_home_unknown: bool) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns the normalized team abbreviation

        Args:
            date_obj (datetime): The date object representing the date of the event.
            opposing_team (str): The name of the opposing team.
            is_home_unknown (bool): A flag indicating whether the home team is unknown.

        Returns:
            Tuple[Optional[str], Optional[str]]: A tuple containing the event ID (optional) and the normalized team abbreviation (optional).
        """
        with DatabaseOperations.get_db_connection() as conn:
            try:
                event_id = None
                team = 'Unknown'
                team_column = 'home_team_normalized' if is_home_unknown else 'away_team_normalized'
                opposing_column = 'away_team' if is_home_unknown else 'home_team'
                query = f"""
                SELECT {team_column}
                FROM e_events
                WHERE date::date = %s::date
                    AND {opposing_column} = %s
                """
                with conn.cursor() as cursor:
                    cursor.execute(query, (date_obj, opposing_team))
                    result = cursor.fetchone()  # Fetch the result
                    if result:
                        team = result  # Extract event_id and team
                    else:
                        team = 'Unknown'  # Handle case where no result is found
                conn.commit()
            except psycopg2.Error as e:
                logger.error(f"SQL Error when retrieving team information.\n{e}")
                return event_id, 'Unknown' # Handle exception case
        return event_id, team

    @staticmethod
    async def get_event_id(date_obj: datetime, home_team: str, away_team: str) -> Optional[str]:
        """
        Retrieve the event ID based on the date and team information.

        Args:
            date_obj (datetime): The date of the event.
            home_team (str): The home team name.
            away_team (str): The away team name.

        Returns:
            Optional[str]: The event ID if found, otherwise None.
        """
        if home_team == 'Unknown' and away_team == 'Unknown':
            return None

        with DatabaseOperations.get_db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    if home_team != 'Unknown' and away_team != 'Unknown':
                        query = """
                        SELECT event_id
                        FROM e_events
                        WHERE (date AT TIME ZONE 'America/New_York')::date = %s::date
                            AND home_team_normalized = %s
                            AND away_team_normalized = %s
                        """
                        cursor.execute(query, (date_obj, home_team, away_team))
                    else:
                        team_column = 'home_team_normalized' if home_team != 'Unknown' else 'away_team_normalized'
                        team_value = home_team if home_team != 'Unknown' else away_team
                        query = f"""
                        SELECT event_id
                        FROM e_events
                        WHERE (date AT TIME ZONE 'America/New_York')::date = %s::date
                            AND {team_column} = %s
                        """
                        cursor.execute(query, (date_obj, team_value))
                    
                    result = cursor.fetchone()
                    event_id = result[0] if result else None
                    conn.commit()
                    return event_id
            except psycopg2.Error as e:
                logger.error(f"SQL Error when retrieving event id information for date {date_obj}, home team {home_team}, away team {away_team}:\n{e}")
                return None

    @staticmethod
    async def update_video_file(video_file: Dict[str, Dict[str, str]]):
        """
        Update or Insert video file metadata into the database.

        Args:
            video_files (Dict[str, Dict[str, str]]): A dictionary containing the metadata of the video file.
                The dictionary should have the following structure:
                {
                    'video_id': {
                        'format_id': 'format_id_value',
                        'file_size': 'file_size_value',
                        'local_path': 'local_path_value'
                    },
                    ...
                }

        Raises:
            Exception: If there is an error while inserting the metadata into the database.
        """
        with DatabaseOperations.get_db_connection() as conn:
            try:
                query = """
                INSERT INTO yt_video_file (video_id, a_format_id, file_size, local_path)
                VALUES (
                %s, %s, %s, %s
                ) ON CONFLICT (video_id, a_format_id) DO UPDATE SET
                a_format_id = EXCLUDED.a_format_id,
                file_size = EXCLUDED.file_size,
                local_path = EXCLUDED.local_path
                """
                with conn.cursor() as cursor:
                    # Batch insert/update records using parameterized queries to prevent SQL injection
                    for video_id, metadata in video_file.items():
                        a_format_id = metadata['a_format_id']
                        file_size = metadata['file_size']
                        local_path = metadata['local_path']
                        cursor.execute(query, (video_id, a_format_id, file_size, local_path))
                conn.commit()
            except Exception as e:        
                print(f"An error occurred: {e}")
                conn.rollback()

    @staticmethod
    async def get_video_ids_without_files() -> List[str]:
        """
        Retrieves a list of video IDs that meet the specified criteria:
        - The video ID is not associated with any video file.
        - The video ID is associated with tags related to major league baseball (MLB).
        - The video title does not contain certain keywords.
        - The video duration is at least 1 hour and 15 minutes.

        Returns:
        - A list of video IDs that meet the specified criteria.
        """
        video_ids = []

        with DatabaseOperations.get_db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    query = f""" 
                    SELECT m.video_id
                    FROM yt_metadata m
                    LEFT JOIN yt_video_file vf ON m.video_id = vf.video_id
                    WHERE TRUE
                        AND m.video_id IN (
                            SELECT DISTINCT video_id
                            FROM yt_tags
                            WHERE TRUE
                                AND (
                                    lower(tag) ILIKE '%major league%'
                                    OR lower(tag) ILIKE '%mlb%'
                                    OR lower(tag) ILIKE '%baseball%'
                                    OR lower(tag) ILIKE '%alcs%'
                                    OR lower(tag) ILIKE '%nlcs%'
                                    OR lower(tag) ILIKE '%world series%'
                                )
                                AND lower(tag) NOT ILIKE '%ncaa%'
                        )
                    AND lower(m.title) NOT ILIKE '%draft%'
                    AND lower(m.title) NOT ILIKE '%ncaa%'
                    AND lower(m.title) NOT ILIKE '%mls%'
                    AND lower(m.title) NOT ILIKE '%nfl%'
                    AND lower(m.title) NOT ILIKE '%nba%'
                    AND lower(m.title) NOT ILIKE '%college%'
                    AND lower(m.title) NOT ILIKE '%cws%'
                    AND lower(m.title) NOT ILIKE '%topgolf%'
                    AND lower(m.title) NOT ILIKE '%futures%'
                    AND lower(m.title) NOT ILIKE '%all star game%'
                    AND lower(m.title) NOT ILIKE '%all-star game%'
                    AND lower(m.title) NOT ILIKE '%wbc%'
                    AND lower(m.title) NOT ILIKE '%world baseball%'
                    AND lower(m.title) NOT ILIKE '%derby%'
                    AND lower(m.title) NOT ILIKE '%softball%'
                    AND lower(m.title) NOT ILIKE '%mlb the show%'
                    AND lower(m.title) NOT ILIKE '%interview%'
                    AND lower(m.title) NOT ILIKE '%makeup%'
                    AND lower(m.title) NOT ILIKE '%ballpark zen%'
                    AND lower(m.title) NOT ILIKE '%check out%'
                    AND lower(m.title) NOT ILIKE '%breaks down%'
                    AND m.duration >= interval '1 hour 15 minutes'
                    AND vf.video_id IS NULL
                    LIMIT 1000;
                    """
                    cursor.execute(query)
                    result = cursor.fetchall()  # Fetch all results
                    video_ids = [row[0] for row in result]  # Extract video IDs from the results
                conn.commit()
            except Exception as e:
                print(f"An error occurred: {e}")
                return False  # Handle exception case
        return video_ids

    @staticmethod
    async def insert_video_file(metadata: Dict[str, Any]):
        """
        Insert video file metadata into the database.

        Args:
            metadata (Dict[str, Any]): A dictionary containing the metadata of the video file.
                The dictionary should have the following keys:
                - 'video_id': The ID of the video.
                - 'format_id': The ID of the video format.
                - 'file_size': The size of the video file.
                - 'local_path': The local path of the video file.

        Raises:
            Exception: If there is an error while inserting the metadata into the database.
        """
        with DatabaseOperations.get_db_connection() as conn:
            try:
                query = """
                INSERT INTO yt_video_file (video_id, format_id, file_size, local_path)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (video_id) DO NOTHING
                """
                with conn.cursor() as cursor:
                    cursor.execute(query, (
                        metadata['video_id'], metadata['format_id'], metadata['file_size'], metadata['local_path']
                    ))
                conn.commit()
            except psycopg2.Error as e:
                logger.error(f"An error occurred while inserting video file metadata: {e}")
                conn.rollback()
