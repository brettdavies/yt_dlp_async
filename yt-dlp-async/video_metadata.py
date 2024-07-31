import os
import sys
import asyncio
from asyncpg import create_pool
import fire
from dataclasses import dataclass
from loguru import logger
import requests
import json
from dotenv import load_dotenv
from typing import List
from .utils import Utils
from .database import DatabaseOperations
import yt_dlp # The yt_dlp library is not directly used in this file. It is called by a subprocess. The library needs to be installed in the python environment. Referenced here for poetry dependency checks purposes.

# YT_API_KEY
YT_API_KEY=""

# Configure loguru
logger.remove()
logger.add(sys.stderr, format="{time} - {level} - {message}", level="INFO")

# Queues for different types of IDs
metadata_queue = asyncio.Queue()

# Counters to track the number of active tasks
active_tasks = {
    'metadata': 0
}

class Logging:
    @staticmethod
    def log_environment_info():
        logger.info("Environment Information:")
        logger.info(f"Python version: {sys.version}")

    # Function to log the state of queues and tasks
    def log_state():
        logger.info(f"video_id_queue size: {metadata_queue.qsize()}, active tasks: {active_tasks['video_id']}")


class VideoIdOperations:
    # Function to fetch video metadata from the YouTube Data API
    async def fetch_video_metadata(video_ids: List[str]) -> json:
        global YT_API_KEY

        video_ids_str = ",".join(video_ids)
        url = f'https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,recordingDetails,status,liveStreamingDetails,localizations,contentDetails,topicDetails&id={video_ids_str}&key={YT_API_KEY}'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'items' in data and len(data['items']) > 0:
                logger.info(f"Successfully fetched metadata for video IDs: {video_ids_str}")
                return data['items']
        else:
            logger.error(f"Failed to fetch metadata for video IDs: {video_ids_str}. Status code: {response.status_code} {response.status_code}")
            if response.status_code == 403:
                logger.error("Received 403 status code. Processing remaining queue and exiting.")
        return []


@dataclass(slots=True)
class Fetcher:
    async def fetch(num_workers: int = 5):
        global YT_API_KEY
        try:
            load_dotenv('./.env')
            HF_TOKEN_PATH = os.getenv("YT_API_KEY")
        except:
            logger.error(f"YT_API_KEY is not set")
            return

        # Ensure num_workers is an integer
        if not isinstance(num_workers, int) or num_workers <=0:
            logger.error(f"num_workers must be a positive integer. The passed value was: {num_workers}")
            return
        Logging.log_environment_info()

        # Create worker tasks
        metadata_workers = [asyncio.create_task(worker_metadata()) for _ in range(num_workers)]

        # Retrieve video_ids and add metadata to queue
        metadatas = await VideoIdOperations.fetch_video_metadata(await DatabaseOperations.get_video_ids_without_metadata())
        for metadata in metadatas['items']:
            # Create metadata dictionary
            meta_dict = await Utils.prep_metadata_dictionary(metadata)
            await metadata_queue.put(meta_dict)

        # Wait for all workers to finish
        await asyncio.gather(*metadata_workers, return_exceptions=True)

        # Wait for all the queue tasks to finish before the script ends
        await asyncio.gather(
            metadata_queue.join()
        )

async def worker_metadata():
    while True:
        # Logging.log_state()
        # Exit if the user_id queue is empty and there are no running playlist_id tasks
        if metadata_queue.empty() and active_tasks['metadata'] == 0:
            break

        if metadata_queue.empty() :
            await asyncio.sleep(0.250)
            continue

        try:
            active_tasks['metadata'] += 1
            
            # Retrieve video_ids and add to queue
            metadatas = await VideoIdOperations.fetch_video_metadata(await DatabaseOperations.get_video_ids_without_metadata())
            for metadata in metadatas['items']:
                # Create metadata dictionary
                meta_dict = await Utils.prep_metadata_dictionary(metadata)
                await metadata_queue.put(meta_dict)

            # Insert or update video metadata
            meta_dict = await asyncio.wait_for(metadata_queue.get(), timeout=1)
            logger.info(f"Size of metadata_queue: {metadata_queue.qsize()}")
            logger.info(f"Start work on metadata for video_id: {meta_dict[0]['id']}")

            # # Insert or update video metadata
            # await DatabaseOperations.insert_update_video_metadata(meta_dict)

            logger.info(f"End metadata work on video_id: {meta_dict[0]['id']}")

        except Exception as e:
            logger.error(f"Error processing metadata for video_id {meta_dict[0]['id']}: {e}")

        finally:
            metadata_queue.task_done()
            active_tasks['metadata'] -= 1  # Decrement the counter when a task is finished
            logger.info(f"ENDING worker_metadata")

    logger.info(f"Metadata worker has finished.")

def cmd():
    fire.Fire(Fetcher)
