import asyncio
from asyncpg import create_pool
import os
import sys
import fire
from dataclasses import dataclass
from loguru import logger
import psycopg2
from psycopg2 import pool
from typing import List
from dotenv import load_dotenv
# yt_dlp is not used in this file, however the library needs to be installed in the python environment.

# Configure loguru
logger.remove()
logger.add(sys.stderr, format="{time} - {level} - {message}", level="INFO")

# Load environment variables from .env file
load_dotenv()

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL')
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, DATABASE_URL)

# Queues for different types of IDs
user_id_queue = asyncio.Queue()
playlist_id_queue = asyncio.Queue()
video_id_queue = asyncio.Queue()

# Counters to track open worker tasks
user_id_queue_processing_tasks_counter = 0
playlist_id_queue_processing_tasks_counter = 0
video_id_queue_processing_tasks_counter = 0

# Events to signal worker completion
user_id_queue_done = asyncio.Event()
playlist_id_queue_done = asyncio.Event()
video_id_queue_done = asyncio.Event()


class Logging:
    @staticmethod
    def log_environment_info():
        logger.info("Environment Information:")
        logger.info(f"Python version: {sys.version}")
        if DATABASE_URL:
            logger.info("DATABASE_URL is set")
            try:
                db_ops = DatabaseOperations()
                conn = db_ops.get_db_connection()
                DatabaseOperations.release_db_connection(conn)
                logger.info("Database connection successful")
            except psycopg2.Error as e:
                logger.error(f"Database connection failed: {e}")
        else:
            logger.error("DATABASE_URL is not set")

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
    async def insert_video_ids(video_ids: List[str]):
        logger.info(f"Attempting to insert video_ids: {len(video_ids)}")
        # logger.info(f"start insert_video_ids: inserting {video_ids}")
        
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
    async def get_video_count():
        conn = DatabaseOperations.get_db_connection()
        try:
            query = """
            SELECT COUNT(1) FROM videos_to_be_processed
            """
            with conn.cursor() as cur:
                cur.execute(query)
                count_result = cur.fetchone()[0]  # Fetch the result and extract the first column value
                # logger.info(f"get_video_count(): videos_to_be_processed: {count_result}")
            conn.commit()
        finally:
            DatabaseOperations.release_db_connection(conn)
        
        return count_result

class VideoIdOperations:
    @staticmethod
    async def prep_url(id_val: str, id_type: str):
        
        if id_type == 'user':
            id_val = (f"https://www.youtube.com/@{id_val}/videos")
        elif id_type == 'user_playlist':
            id_val = (f"https://www.youtube.com/@{id_val}/playlists")
        elif id_type == 'playlist':
            id_val = (f"https://www.youtube.com/playlist?list={id_val}")
        
        return id_val

    # This command will print out the IDs of all videos uploaded to the specified channel.
    # yt-dlp --flat-playlist --print id <channel_url>

    # This command will print out the IDs of all videos added to a specified playlist
    # yt-dlp --flat-playlist --print id <playlist_url>
    @staticmethod
    async def fetch_video_ids_from_url(url: str):
        video_ids = []
        try:
            # logger.info(f"start fetch_video_ids_from_url_sub {url}")
            cmd = ["yt-dlp", "--flat-playlist", "--print", "id", url]
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                video_ids.extend(stdout.decode().splitlines())
                # logger.info(f"end fetch_video_ids_from_url_sub {url}")
            else:
                raise Exception(f"Error fetching video_ids from {url}: {stderr.decode()}")
        except Exception as e:
            logger.error(f"Error: {e}")
        # logger.info(f"return video_ids {video_ids}")
        return video_ids

    # This command will print out the IDs of all playlists associated with the specified channel.
    # yt-dlp --flat-playlist --print "%(id)s" <channel_url>
    @staticmethod
    async def fetch_playlist_ids_from_user_id(user_url: str):
        playlist_ids = []
        try:
            # logger.info(f"start fetch_playlist_ids_from_user_id_sub {user_url}")
            cmd = ["yt-dlp", "--flat-playlist", "--print", "%(id)s", user_url]
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                playlist_ids.extend(stdout.decode().splitlines())
                # logger.info(f"end fetch_playlist_ids_from_user_id_sub {user_url}")
            else:
                logger.error(f"Error fetching playlist_ids from {user_url}")
        except Exception as e:
            logger.error(f"Error: {e}")
        return playlist_ids

    def read_ids_from_file(file_path):
        _, file_extension = os.path.splitext(file_path)
        ids = []

        if file_extension == '.txt':
            with open(file_path, 'r') as file:
                ids = [line.strip() for line in file if line.strip()]
        elif file_extension == '.csv':
            import csv
            with open(file_path, 'r') as file:
                reader = csv.reader(file)
                ids = [row[0].strip() for row in reader if row and row[0].strip()]

        return ids

@dataclass(slots=True)
class Fetcher:
    async def fetch(video_ids=None, video_id_files=None, playlist_ids=None, playlist_id_files=None, user_ids=None, user_id_files=None):
        Logging.log_environment_info()

        # Create worker tasks
        num_workers = 5  # Number of workers for each type of queue (adjust as needed)
        user_id_workers = [asyncio.create_task(worker_user_ids()) for _ in range(num_workers)]
        playlist_id_workers = [asyncio.create_task(worker_playlist_ids()) for _ in range(num_workers)]
        video_id_workers = [asyncio.create_task(worker_video_ids()) for _ in range(num_workers)]

        # Add video_ids, playlist_ids, and user_ids to their respective queues
        user_ids = user_ids or []
        if user_ids:
            if isinstance(user_ids, str):
                user_ids = user_ids.replace(',', ' ').split()

            for user_id in user_ids:
                await user_id_queue.put(user_id)

        playlist_ids = playlist_ids or []
        if playlist_ids:
            if isinstance(playlist_ids, str):
                playlist_ids = playlist_ids.replace(',', ' ').split()

            for playlist_id in playlist_ids:
                await playlist_id_queue.put(playlist_id)

        video_ids = video_ids or []
        if video_ids:
            if isinstance(video_ids, str):
                video_ids = video_ids.replace(',', ' ').split()

        # Process any files and add video_ids, playlist_ids, and user_ids to their respective queues
        if user_id_files:
            for file in user_id_files:
                user_ids.extend(VideoIdOperations.read_ids_from_file(file))
                for user_id in user_ids:
                    await user_id_queue.put(user_id)

        if playlist_id_files:
            for file in playlist_id_files:
                playlist_ids.extend(VideoIdOperations.read_ids_from_file(file))
                for playlist_id in playlist_ids:
                    await playlist_id_queue.put(playlist_id)
        
        if video_id_files:
            for file in video_id_files:
                video_ids.extend(VideoIdOperations.read_ids_from_file(file))
                for video_id in video_ids:
                    await video_id_queue.put(video_id)

        # Wait for all workers to finish
        await asyncio.gather(*user_id_workers, *playlist_id_workers, *video_id_workers, return_exceptions=True)

async def worker_user_ids():
    global user_id_queue_processing_tasks_counter
    while True:
        # Exit if user_id processing is done
        if user_id_queue_done.is_set() and user_id_queue.empty() and user_id_queue_processing_tasks_counter == 0:
            break

        try:
            user_id = await asyncio.wait_for(user_id_queue.get(), timeout=1)
            logger.info(f"Start work on user_id: {user_id}")
            logger.info(f"Size of user_id_queue: {user_id_queue.qsize()}")
            user_id_queue_processing_tasks_counter += 1  # Increment the counter when a task is started
        except asyncio.TimeoutError:
            if user_id_queue.empty() and not user_id_queue_done.is_set() and user_id_queue_processing_tasks_counter == 0:
                user_id_queue_done.set()
                logger.info("User ID queue processing is complete.")
            continue
        
        try:
            user_url = await VideoIdOperations.prep_url(user_id, 'user')
            # logger.info(f"worker_user_ids user_url after prep_url {user_url}")
            user_playlist_url = await VideoIdOperations.prep_url(user_id, 'user_playlist')
            # logger.info(f"worker_user_ids user_playlist_url after prep_url {user_playlist_url}")
            
            # Process user videos and playlists
            user_playlist_ids = await VideoIdOperations.fetch_playlist_ids_from_user_id(user_playlist_url)
            # logger.info(f"user_playlist_ids after fetch_playlist_ids_from_user_id {user_playlist_ids}")
            if user_playlist_ids:
                for playlist_id in user_playlist_ids:
                    await playlist_id_queue.put(playlist_id)
                # logger.info(f"worker_user_ids playlist_id_queue after playlist_id_queue.put {playlist_id_queue.qsize()}")

            user_video_ids = await VideoIdOperations.fetch_video_ids_from_url(user_url)
            # logger.info(f"worker_user_ids user_video_ids after fetch_video_ids_from_url {len(user_video_id)} {user_video_id}")
            if user_video_ids:
                for user_video_id in user_video_ids:
                    await video_id_queue.put(user_video_id)
                # logger.info(f"worker_user_ids video_id_queue after video_id_queue.put {video_id_queue.qsize()}")

            user_id_queue.task_done()
            logger.info(f"End work on user_id: {user_id}")

        except Exception as e:
            logger.error(f"Error processing user_id {user_id}: {e}")

        finally:
            user_id_queue_processing_tasks_counter -= 1  # Decrement the counter when a task is finished

    logger.info(f"User ID worker has finished.")

async def worker_playlist_ids():
    global playlist_id_queue_processing_tasks_counter
    while True:
        # Exit if user_id processing is done and playlist_id processing is done
        if user_id_queue_done.is_set() and playlist_id_queue_done.is_set() and playlist_id_queue.empty() and playlist_id_queue_processing_tasks_counter == 0:
            break
        
        try:
            playlist_id = await asyncio.wait_for(playlist_id_queue.get(), timeout=1)
            logger.info(f"Start work on playlist_id: {playlist_id}")
            logger.info(f"Size of playlist_id_queue: {playlist_id_queue.qsize()}")
            playlist_id_queue_processing_tasks_counter += 1  # Increment the counter when a task is started

        except asyncio.TimeoutError:
            if playlist_id_queue.empty() and user_id_queue_done.is_set() and not playlist_id_queue_done.is_set() and playlist_id_queue_processing_tasks_counter == 0:
                playlist_id_queue_done.set()
            continue
        
        try:
            playlist_url = await VideoIdOperations.prep_url(playlist_id, 'playlist')
            # logger.info(f"playlist_url after prep_url {playlist_url}")

            # Process playlist videos
            playlist_video_ids = await VideoIdOperations.fetch_video_ids_from_url(playlist_url)
            # logger.info(f"playlist_video_ids after fetch_video_ids_from_url {len(playlist_video_ids)} {playlist_video_ids}")

            if playlist_video_ids:
                for playlist_video_id in playlist_video_ids:
                    await video_id_queue.put(playlist_video_id)
            
            playlist_id_queue.task_done()
            logger.info(f"End work on playlist_id: {playlist_id}")

        except Exception as e:
            logger.error(f"Error processing playlist_id {playlist_id}: {e}")

        finally:
            playlist_id_queue_processing_tasks_counter -= 1  # Decrement the counter when a task is finished

    logger.info(f"Playlist ID worker has finished.")

async def worker_video_ids():
    global video_id_queue_processing_tasks_counter
    while True:
        # Exit if the video_id queue is empty, user_id processing is done, and playlist_id processing is done
        if video_id_queue.empty() and video_id_queue_done.is_set() and user_id_queue_done.is_set() and playlist_id_queue_done.is_set() and video_id_queue_processing_tasks_counter == 0:
            break

        try:
            video_ids_batch = []

            # Collect video IDs from the queue
            while not video_id_queue.empty():
                video_ids_batch.append(await video_id_queue.get())
                # logger.info(f"worker_video_ids video_id_queue {video_id_queue.qsize()}")
                # logger.info(f"worker_video_ids video_ids_batch {len(video_ids_batch)}")
                video_id_queue_processing_tasks_counter += 1  # Increment the counter when a task is started

            # Insert collected video IDs into the database
            if video_ids_batch:
                await DatabaseOperations.insert_video_ids(video_ids_batch)
                logger.info(f"Currrent video count: {await DatabaseOperations.get_video_count()}")

        except Exception as e:
            logger.error(f"Error processing {len(video_ids_batch)} videos {video_ids_batch}: {e}")

        finally:
            video_id_queue_processing_tasks_counter -= 1  # Decrement the counter when a task is finished

            if video_id_queue.empty() and user_id_queue_done.is_set() and playlist_id_queue_done.is_set() and video_id_queue_processing_tasks_counter == 0:
                video_id_queue_done.set()

        await asyncio.sleep(1)  # Optional: reduce CPU usage while waiting for new video IDs

    logger.info(f"Video ID worker has finished.")

def cmd():
    fire.Fire(Fetcher)
