import os
from psycopg2 import pool
from dotenv import load_dotenv
from typing import Any, Dict, List
# from .video_id import Logging

# Load environment variables from .env file
load_dotenv()

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL')
connection_pool = pool.SimpleConnectionPool(1, 10, DATABASE_URL)

class DatabaseOperations:
    @staticmethod
    def get_db_connection():
        # Get a connection from the pool
        return connection_pool.getconn()

    @staticmethod
    def release_db_connection(conn):
        # Release the connection back to the pool
        connection_pool.putconn(conn)

    @staticmethod
    async def get_count_videos_to_be_processed():
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
    async def get_video_ids_without_metadata() -> List[str]:
        video_ids = List[str]

        conn = DatabaseOperations.get_db_connection()
        try:
            with conn.cursor() as cursor:
                query = "SELECT video_id FROM videos_to_be_processed ORDER BY RANDOM() LIMIT 50"
                cursor.execute(query)
                result = cursor.fetchall()  # Fetch all results
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
    async def insert_update_video_metadata(metadata: Dict[str, Any]):
        conn = DatabaseOperations.get_db_connection()
        try:
            # Create a cursor object
            cursor = conn.cursor()

            # Metadata Table
            sql_metadata = """
            INSERT INTO metadata (
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
            # print(f"{sql_metadata}")

            # Tags Table
            sql_tags = """
            INSERT INTO tags (
                video_id, tag
            ) VALUES (
                %s, %s
            ) ON CONFLICT (video_id, tag) DO UPDATE SET
                tag = EXCLUDED.tag;
            """
            # print(f"{sql_tags}")

            # Thumbnails Table
            sql_thumbnails = """
            INSERT INTO thumbnails (
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
            # print(f"{sql_tags}")

            # Localized Info Table
            sql_localized = """
            INSERT INTO localized_info (
                video_id, language, title, description
            ) VALUES (
                %s, %s, %s, %s
            ) ON CONFLICT (video_id, language) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description;
            """
            # print(f"{sql_localized}")

            # Topic Categories Table
            sql_categories = """
            INSERT INTO topic_categories (
                video_id, category
            ) VALUES (
                %s, %s
            ) ON CONFLICT (video_id, category) DO NOTHING;
            """
            # print(f"{sql_categories}")

            # # Content Rating Table
            # sql_rating = """
            # INSERT INTO content_rating (
            #     video_id, rating_type, rating_value
            # ) VALUES (
            #     %s, %s, %s
            # ) ON CONFLICT (video_id, rating_type) DO UPDATE SET
            #     rating_value = EXCLUDED.rating_value;
            # """
            # # print(f"{sql_rating}")


            # # Recording Details Table
            # sql_recording = """
            # INSERT INTO recording_details (
            #     video_id, recording_date, recording_location
            # ) VALUES (
            #     %s, %s, %s
            # ) ON CONFLICT (video_id, recording_date, recording_location) DO UPDATE SET
            #     recording_date = EXCLUDED.recording_date,
            #     recording_location = EXCLUDED.recording_location;
            # """
            # # print(f"{sql_rating}")


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
            # cursor.execute(sql_rating, ( # Not currently being requested from the api. Ready to go in the future as required.
            #     metadata['video_id'], metadata['rating_type'], metadata['rating_value']
            # ))
            # cursor.execute(sql_recording, ( # Not currently being requested from the api. Ready to go in the future as required.
            #     metadata['video_id'], metadata['recording_date'], metadata['recording_location']
            # ))

            conn.commit()

        except Exception as e:
            print(f"Sql Error when writing metadata for video {metadata['video_id']}:\n{e}")

        finally:
            DatabaseOperations.release_db_connection(conn)


            # # Transcripts Video Table
            # sql_transcript = """
            # INSERT INTO transcripts_video (
            #   video_id, transcript_text, language, is_auto_generated
            # ) VALUES (
            #   '{video_id}', '{transcript_text}', '{language}', {is_auto_generated}
            # ) ON CONFLICT (video_id, language) DO UPDATE SET
            #   transcript_text = EXCLUDED.transcript_text,
            #   is_auto_generated = EXCLUDED.is_auto_generated;
            # """.format(**metadata)

            # cursor.execute(sql_transcript)