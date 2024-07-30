import asyncio
import os
import sys
from dataclasses import dataclass
from loguru import logger
from typing import List
import fire
import aiomultiprocess

from .utils import Utils
from .database import DatabaseOperations
import yt_dlp # The yt_dlp library is not directly used in this file. It is called by a subprocess. The library needs to be installed in the python environment. Referenced here for poetry dependency checks purposes.

# Configure loguru
logger.remove()
logger.add(sys.stderr, format="{time} - {level} - {message}", level="INFO")

# Queues for different types of IDs
user_id_queue = asyncio.Queue()
playlist_id_queue = asyncio.Queue()
video_id_queue = asyncio.Queue()

# Counters to track the number of active tasks
active_tasks = {
    'user_id': 0,
    'playlist_id': 0,
    'video_id': 0
}

class Logging:
    @staticmethod
    def log_environment_info():
        logger.info("Environment Information:")
        logger.info(f"Python version: {sys.version}")

    # Function to log the state of queues and tasks
    def log_state():
        logger.info(f"user_id_queue size: {user_id_queue.qsize()}, active tasks: {active_tasks['user_id']}")
        logger.info(f"playlist_id_queue size: {playlist_id_queue.qsize()}, active tasks: {active_tasks['playlist_id']}")
        logger.info(f"video_id_queue size: {video_id_queue.qsize()}, active tasks: {active_tasks['video_id']}")

class VideoIdOperations:
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

                for video_id in video_ids:
                    await video_id_queue.put(video_id)

        # Process any files and add video_ids, playlist_ids, and user_ids to their respective queues
        if user_id_files:
            user_ids = user_ids or []
            if isinstance(user_id_files, str):
                user_id_files = user_id_files.replace(',', ' ').split()
                for file in user_id_files:
                    user_ids.extend(VideoIdOperations.read_ids_from_file(file))
                    for user_id in user_ids:
                        await user_id_queue.put(user_id)

        if playlist_id_files:
            playlist_ids = playlist_ids or []
            if isinstance(playlist_id_files, str):
                playlist_id_files = playlist_id_files.replace(',', ' ').split()
                for file in playlist_id_files:
                    playlist_ids.extend(VideoIdOperations.read_ids_from_file(file))
                    for playlist_id in playlist_ids:
                        await playlist_id_queue.put(playlist_id)
        
        if video_id_files:
            video_ids = video_ids or []
            if isinstance(video_id_files, str):
                video_id_files = video_id_files.replace(',', ' ').split()
                for file in video_id_files:
                    _, file_extension = os.path.splitext(file)
                    if file_extension == '.txt':
                        await DatabaseOperations.insert_video_ids_bulk(file)
                    elif file_extension == '.csv':
                        playlist_ids.extend(VideoIdOperations.read_ids_from_file(file))
                        for playlist_id in playlist_ids:
                            await playlist_id_queue.put(playlist_id)

async def process_user_id(user_id):
    active_tasks['user_id'] += 1
    try:
        user_url = await Utils.prep_url(user_id, 'user')
        # logger.info(f"worker_user_ids user_url after prep_url {user_url}")
        user_playlist_url = await Utils.prep_url(user_id, 'user_playlist')
        # logger.info(f"worker_user_ids user_playlist_url after prep_url {user_playlist_url}")
        
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
        logger.info(f"End work on user_id: {user_id}")

    except Exception as e:
        logger.error(f"Error processing user_id {user_id}: {e}")

    finally:
        active_tasks['user_id'] -= 1
        user_id_queue.task_done()
        logger.info(f"ENDING process_user_id {user_id}")

async def process_playlist_id(playlist_id):
    active_tasks['playlist_id'] += 1
    try:
        logger.info(f"STARTING playlist_url {playlist_id}")
        playlist_url = await Utils.prep_url(playlist_id, 'playlist')
        logger.info(f"playlist_url after prep_url {playlist_url}")

        playlist_video_ids = await VideoIdOperations.fetch_video_ids_from_url(playlist_url)
        logger.info(f"playlist_video_ids after fetch_video_ids_from_url {len(playlist_video_ids)} {playlist_video_ids}")

        if playlist_video_ids:
            for playlist_video_id in playlist_video_ids:
                await video_id_queue.put(playlist_video_id)

        logger.info(f"End work on playlist_id: {playlist_id}")
        
    except Exception as e:
        logger.error(f"Error processing playlist_id {playlist_id}: {e}")

    finally:
        active_tasks['playlist_id'] -= 1
        playlist_id_queue.task_done()
        logger.info(f"ENDING playlist_url {playlist_id}")

async def process_video_id():
    active_tasks['video_id'] += 1
    try:
        video_ids_batch = []

        # Collect video IDs from the queue
        while not video_id_queue.empty():
            video_ids_batch.append(await video_id_queue.get())
            video_id_queue.task_done()

        # Insert collected video IDs into the database
        if video_ids_batch:
            logger.info(f"process_video_id attempting to insert {len(video_ids_batch)} videos")
            await DatabaseOperations.insert_video_ids(video_ids_batch)
            logger.info(f"Currrent video count: {await DatabaseOperations.get_video_count()}")

    except Exception as e:
        logger.error(f"Error processing {len(video_ids_batch)} videos {video_ids_batch}: {e}")

    finally:
        active_tasks['video_id'] -= 1
        logger.info(f"ENDING process_video_id")

async def worker(queue: asyncio.Queue):
    Logging.log_state()
    while True:
        if user_id_queue.empty() and playlist_id_queue.empty() and video_id_queue.empty() and (active_tasks['user_id'] + active_tasks['playlist_id'] + active_tasks['video_id']) == 0:
            # If all queues are empty and there are no active_tasks then break out of the loop
            break

        if queue.empty() :
            await asyncio.sleep(0.250)
            continue

        if queue is user_id_queue and not user_id_queue.empty():
            user_id = await user_id_queue.get()
            logger.info(f"Remaining items in user_id_queue: {user_id_queue.qsize()}")
            logger.info(f"Start work on user_id: {user_id}")
            await process_user_id(user_id)
            
        if queue is playlist_id_queue and not playlist_id_queue.empty():
            playlist_id = await playlist_id_queue.get()
            logger.info(f"Remaining items in playlist_id_queue: {playlist_id_queue.qsize()}")
            logger.info(f"Start work on playlist_id: {playlist_id}")
            await process_playlist_id(playlist_id)
        
        if queue is video_id_queue and not video_id_queue.empty():
            logger.info(f"worker Remaining items in video_id_queue: {video_id_queue.qsize()}")
            await process_video_id()
            logger.info(f"worker End items in video_id_queue: {video_id_queue.qsize()}")
        
        Logging.log_state()

async def run_workers():
    queues = [user_id_queue, playlist_id_queue, video_id_queue]
    workers = [worker(queue) for queue in queues]

    logger.info("Starting worker coroutines")
    
    try:
        logger.info("START of run_workers try block")
        await asyncio.gather(*workers)
        # logger.info("run_workers finished asyncio.gather(*workers)")
        
        # Ensure the script ends when all tasks are done
        await asyncio.gather(
            user_id_queue.join(),
            playlist_id_queue.join(),
            video_id_queue.join()
        )
        # logger.info("run_workers finished asyncio.gather")

    except Exception as e:
        logger.error(f"Error running workers: {e}") 

def cmd():
    fire.Fire(Fetcher)
    asyncio.run(run_workers())
