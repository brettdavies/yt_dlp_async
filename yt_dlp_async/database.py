# Standard Libraries
import os
import datetime
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from sshtunnel import SSHTunnelForwarder
from psycopg2.extras import DictCursor
from typing import Any, Dict, List, Optional, Tuple, Union

# Logging and Configuration
from loguru import logger
from dotenv import load_dotenv

# Third Party Libraries
import pandas as pd

# Load environment variables from .env file
load_dotenv()

class DatabaseOperations:
    def __init__(self) -> None:
        """
        Initialize the SSH tunnel and the psycopg2 connection pool.
        """
        # SSH and database configuration
        ssh_host = os.environ.get('SSH_HOST')
        ssh_port = int(os.environ.get('SSH_PORT', 22))
        ssh_user = os.environ.get('SSH_USER')
        ssh_key_path = os.environ.get('SSH_KEY_PATH')
        remote_bind_address = ('127.0.0.1', 5432)

        db_user = os.environ.get('DB_USER')
        db_password = os.environ.get('DB_PASSWORD')
        db_name = os.environ.get('DB_NAME')

        # Validate required SSH and DB configurations
        required_vars = ['SSH_HOST', 'SSH_PORT', 'SSH_USER', 'SSH_KEY_PATH', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
        missing = [var for var in required_vars if not os.environ.get(var)]
        if missing:
            logger.error(f"Missing required environment variables for SSH or DB: {', '.join(missing)}")
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

        try:
            # Initialize the SSH tunnel
            self.tunnel = SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_pkey=ssh_key_path,
                remote_bind_address=remote_bind_address
            )
            self.tunnel.start()
            logger.warning(f"SSH tunnel established on local port {self.tunnel.local_bind_port}.")

            # Initialize the connection pool
            self.connection_pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,  # Adjust the max connections based on your requirements
                user=db_user,
                password=db_password,
                host='127.0.0.1',
                port=self.tunnel.local_bind_port,
                database=db_name
            )
            logger.warning("Database connection pool created successfully.")

        except Exception as e:
            logger.error(f"Unexpected error during initialization: {e}")
            self.close()  # Ensure resources are cleaned up
            raise

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context and clean up resources.
        """
        self.close()

    def close(self):
        """
        Close the connection pool and the SSH tunnel.
        """
        try:
            if hasattr(self, 'connection_pool') and self.connection_pool:
                self.connection_pool.closeall()
                logger.warning("Database connection pool closed.")

            if hasattr(self, 'tunnel') and self.tunnel:
                self.tunnel.stop()
                logger.warning("SSH tunnel closed.")
        except Exception as e:
            logger.error(f"Error during closing resources: {e}")

    @contextmanager
    def get_db_connection(self, worker_name: Optional[str]) -> Any:
        """
        Context manager for database connection.
        Ensures that the connection is properly returned to the pool.
        """
        conn = None
        try:
            conn = self.connection_pool.getconn()
            if conn is None:
                logger.error(f"{"["+worker_name+"] " if worker_name else None}Failed to obtain database connection from the pool.")
                raise psycopg2.OperationalError(f"{"["+worker_name+"] " if worker_name else None}Unable to obtain database connection.")
            yield conn
        except psycopg2.Error as e:
            logger.error(f"{"["+worker_name+"] " if worker_name else None}Error obtaining database connection: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)
                logger.debug(f"{"["+worker_name+"] " if worker_name else None}Database connection returned to the pool.")

    def execute_query(self, worker_name: str, query: str, params: tuple, operation: str = 'fetch') -> Optional[Union[List[Dict], int]]:
        """
        Execute a database query.

        Args:
            worker_name (str): Name of the worker executing the query.
            query (str): SQL query to execute.
            params (tuple): Parameters for the SQL query.
            operation (str): Type of operation ('fetch', 'commit', 'execute'). Defaults to 'fetch'.

        Returns:
            Optional[Union[List[Dict], int]]: Result of the query based on the operation type.
        """
        try:
            with self.get_db_connection(worker_name) as conn:
                with conn.cursor(cursor_factory=DictCursor) as cursor:
                    cursor.execute(query, params)
                    
                    if operation == 'fetch':
                        return cursor.fetchall()
                    elif operation == 'commit':
                        conn.commit()
                        return cursor.rowcount
                    elif operation == 'execute':
                        return cursor.rowcount
                    else:
                        logger.error(f"Unknown operation type: {operation}")
                        return None
        except psycopg2.Error as e:
            logger.error(f"SQL Error when executing query: {e}")
            if operation == 'commit':
                conn.rollback()
                logger.debug(f"[{worker_name}] Transaction rolled back due to error.")

            return None

    def get_count_videos_to_be_processed(self) -> Optional[int]:
        """
        Retrieves the count of videos to be processed from the database.

        Returns:
            The count of videos to be processed, or None if an error occurs.
        """
        query = """
        SELECT COUNT(1) FROM yt_videos_to_be_processed
        """
        count_result = self.execute_query(None, query, ())

        if count_result is not None:
            return count_result[0][0]
        else:
            return None

    def get_video_ids_without_metadata(self) -> List[str]:
        """
        Retrieves a list of video IDs without metadata from the database.

        Returns:
            A list of video IDs that lack associated metadata.
        """
        video_ids: List[str] = []

        query = """
            SELECT video_id 
            FROM yt_videos_to_be_processed 
            WHERE has_failed_metadata = FALSE
            ORDER BY RANDOM() 
            LIMIT 50
        """
        result = self.execute_query(None, query, ())
        if result is not None:
            video_ids = [row[0] for row in result]
        return video_ids

    def set_video_id_failed_metadata_true(self, video_ids: set[str]) -> None:
        """
        Marks the given video IDs as having failed metadata retrieval.

        Args:
            video_ids: A set of video IDs to update.
        """
        query = """
        UPDATE yt_videos_to_be_processed
        SET has_failed_metadata = TRUE
        WHERE video_id = %s
        """
        params = [(vid,) for vid in video_ids]
        self.execute_query(None, query, params, operation='commit')

    def insert_video_ids(self, video_ids_batch: List[str]) -> None:
        """
        Inserts a batch of video IDs into the database.

        Args:
            video_ids_batch: The batch of video IDs to insert.
        """
        insert_query = """
        INSERT INTO yt_video_ids (video_id)
        VALUES (%s)
        ON CONFLICT (video_id) DO NOTHING;
        """
        params = [(video_id,) for video_id in video_ids_batch]
        self.execute_query(None, insert_query, params, operation='commit')

    def insert_video_ids_bulk(self, id_file_path: str) -> None:
        """
        Inserts video IDs in bulk from a file into the database.

        Args:
            id_file_path: The file path containing the video IDs.

        Raises:
            psycopg2.Error: If a database error occurs during bulk insertion.
        """
        with DatabaseOperations.get_db_connection() as conn:
            try:
                cursor = conn.cursor()

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

                cursor.execute(create_temp_table_sql)

                with open(id_file_path, 'r') as f:
                    cursor.copy_expert(sql=copy_sql, file=f)

                cursor.execute(insert_sql)
                cursor.execute(drop_temp_table_sql)

                conn.commit()
            except psycopg2.Error as e:
                logger.error(f"SQL Error when writing bulk video ids from: {id_file_path}:\n{e}")
                conn.rollback()

    def insert_update_video_metadata(self, metadata: Dict[str, Any]) -> None:
        """
        Inserts or updates video metadata in the database.

        Args:
            metadata: A dictionary containing the video metadata.

        Raises:
            psycopg2.Error: If a database error occurs during insertion or update.
        """
        with DatabaseOperations.get_db_connection() as conn:
            try:
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
                """

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

    def get_dates_no_event_metadata(self) -> List[datetime.datetime]:
        """
        Retrieves dates that have no associated event metadata.

        Returns:
            A list of dates lacking event metadata.
        """
        query = """
        SELECT *
        FROM (
            SELECT DISTINCT yt_metadata.event_date_local_time
            FROM yt_metadata
            LEFT JOIN e_events ON yt_metadata.event_date_local_time = e_events.date::DATE
            WHERE e_events.date IS NULL
        ) AS distinct_dates
        ORDER BY RANDOM()
        """
        result = self.execute_query(None, query, ())
        if result is not None:
            dates_no_event_metadata = [row[0] for row in result]
        else:
            dates_no_event_metadata = []
        return dates_no_event_metadata

    def save_events(self, df: pd.DataFrame) -> None:
        """
        Inserts events into the 'e_events' table in the database.

        Args:
            df: The DataFrame containing the events data.

        Raises:
            ValueError: If the DataFrame does not contain all the required columns.
        """
        required_columns = ['event_id', 'date', 'type', 'short_name', 'home_team', 'away_team',
                            'home_team_normalized', 'away_team_normalized']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Missing required column '{col}' in the DataFrame.")
                raise ValueError(f"Missing required column '{col}' in the DataFrame.")

        records = df[required_columns].values.tolist()

        insert_query = """
        INSERT INTO e_events (event_id, date, type, short_name, home_team, away_team, home_team_normalized, away_team_normalized)
        VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (event_id) DO NOTHING
        """

        params = records
        self.execute_query(None, insert_query, params, operation='commit')

    def check_if_existing_e_events_by_date(self, date_obj: datetime.datetime) -> bool:
        """
        Checks if there are existing events in 'e_events' for the given date.

        Args:
            date_obj: The date to check for existing events.

        Returns:
            True if events exist for the given date, False otherwise.
        """
        query = """
        SELECT COUNT(1)
        FROM e_events
        WHERE (date AT TIME ZONE 'America/New_York')::date = %s::date
        """
        params = (date_obj,)
        count_result = self.execute_query(None, query, params)[0][0]

        return count_result > 0

    def get_e_events_team_info(self, date_obj: datetime.datetime, opposing_team: str, is_home_unknown: bool) -> Tuple[Optional[str], Optional[str]]:
        """
        Retrieves normalized team abbreviation from 'e_events'.

        Args:
            date_obj: The date of the event.
            opposing_team: The name of the opposing team.
            is_home_unknown: Flag indicating whether the home team is unknown.

        Returns:
            A tuple containing the event ID (optional) and the normalized team abbreviation (optional).
        """
        event_id = None
        team = 'Unknown'
        team_column = 'home_team_normalized' if is_home_unknown else 'away_team_normalized'
        opposing_column = 'away_team' if is_home_unknown else 'home_team'

        select_query = f"""
        SELECT {team_column}
        FROM e_events
        WHERE (date AT TIME ZONE 'America/New_York')::date = %s::date
            AND {opposing_column} = %s
        """

        params = (date_obj, opposing_team)
        result = self.execute_query(None, select_query, params)
        if result:
            team = result[0][0]
        else:
            team = 'Unknown'
        return event_id, team

    def get_event_id(self, date_obj: datetime.datetime, home_team: str, away_team: str) -> Optional[str]:
        """
        Retrieves the event ID based on date and team information.

        Args:
            date_obj: The date of the event.
            home_team: The home team name.
            away_team: The away team name.

        Returns:
            The event ID if found, otherwise None.
        """
        if home_team == 'Unknown' and away_team == 'Unknown':
            return None

        if home_team != 'Unknown' and away_team != 'Unknown':
            select_query = """
            SELECT event_id
            FROM e_events
            WHERE (date AT TIME ZONE 'America/New_York')::date = %s::date
                AND home_team_normalized = %s
                AND away_team_normalized = %s
            """
            params = (date_obj, home_team, away_team)
        else:
            team_column = 'home_team_normalized' if home_team != 'Unknown' else 'away_team_normalized'
            team_value = home_team if home_team != 'Unknown' else away_team
            select_query = f"""
            SELECT event_id
            FROM e_events
            WHERE (date AT TIME ZONE 'America/New_York')::date = %s::date
                AND {team_column} = %s
            """
            params = (date_obj, team_value)
        
        result = self.execute_query(None, select_query, params)
        event_id = result[0] if result else None
        return event_id

    def update_audio_file(self, video_file: Dict[str, Dict[str, str]]) -> bool:
        """
        Updates or inserts audio file metadata into the database.

        Args:
            video_file: A dictionary containing the metadata of the audio file.

        Returns:
            True if the operation is successful, False otherwise.
        """
        insert_query = """
        INSERT INTO yt_video_file (video_id, a_format_id, file_size, local_path)
        VALUES (
        %s, %s, %s, %s
        ) ON CONFLICT (video_id, a_format_id) DO UPDATE SET
        a_format_id = EXCLUDED.a_format_id,
        file_size = EXCLUDED.file_size,
        local_path = EXCLUDED.local_path
        """

        logger.debug("[Video] [DB] Starting to process video_file dictionary.")
        for video_id, metadata in video_file.items():
            logger.debug(f"[Video {video_id}] [DB] Processing video_id: {video_id} with metadata: {metadata}")

            if not isinstance(metadata, dict):
                logger.error(f"[Video {video_id}] [DB] Metadata for video_id {video_id} is not a dictionary: {metadata}")
                continue

            a_format_id = metadata.get('a_format_id')
            file_size = metadata.get('file_size')
            local_path = metadata.get('local_path')

            if a_format_id is None or file_size is None or local_path is None:
                logger.error(f"[Video {video_id}] [DB] Missing required metadata for video_id {video_id}: {metadata}")
                continue

            logger.debug(f"[Video {video_id}] [DB] Executing query with values: {video_id}, {a_format_id}, {file_size}, {local_path}")

            params = (video_id, a_format_id, file_size, local_path)
            result = self.execute_query(None, insert_query, params, operation='commit')
            if result is None:
                logger.error(f"[Video {video_id}] [DB] Failed to insert/update audio file metadata.")
                return False
            else: 
                logger.debug(f"[Video {video_id}] [DB] Successfully inserted/updated audio file metadata.")
        return True

    def get_video_ids_without_files(self) -> List[str]:
        """
        Retrieves a list of video IDs that lack associated video files.

        Returns:
            A list of video IDs that meet the specified criteria.
        """
        video_ids = []

        query = """
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
        """

        params = ()
        video_ids = self.execute_query(None, query, params)
        return video_ids
