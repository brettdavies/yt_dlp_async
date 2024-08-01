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
# from youtube_transcript_api import YouTubeTranscriptApi # https://pypi.org/project/youtube-transcript-api/
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

# Boolean to track if we've receive a 403 error
is403thrown = False

# Counters to track the number of active tasks
active_tasks = {
    'retrieve': 0,
    'save': 0
}

class Logging:
    @staticmethod
    def log_environment_info():
        logger.info("Environment Information:")
        logger.info(f"Python version: {sys.version}")

    # Function to log the state of queues and tasks
    def log_state():
        logger.info(f"metadata_queue size: {metadata_queue.qsize()}, active tasks: {active_tasks['video_id']}")


class VideoIdOperations:
    # Function to fetch video metadata from the YouTube Data API
    async def fetch_video_metadata(video_ids: List[str]) -> json:
        global YT_API_KEY
        global is403thrown
        
        video_ids_str = ",".join(video_ids)
        # logger.info(f"video_ids_str {video_ids_str}")
        url = f"https://www.googleapis.com/youtube/v3/videos?part=contentDetails,id,liveStreamingDetails,localizations,player,recordingDetails,snippet,statistics,status,topicDetails&id={video_ids_str}&key={YT_API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'items' in data and len(data['items']) > 0:
                logger.info(f"Successfully fetched metadata for {video_ids_str.count(',')+1} video_ids: {video_ids_str}")
                # logger.info(f"data items:\n {data['items']}")
                return data['items']
        else:
            logger.error(f"Failed to fetch metadata for video IDs: {video_ids_str}. Status code: {response.status_code} {response.reason}")
            if response.status_code == 403:
                is403thrown=True
                logger.error("Received 403 status code. Processing remaining queue and exiting.")
        return []


@dataclass(slots=True)
class Fetcher:
    async def fetch(anything=None, num_workers: int = 1):
        global YT_API_KEY
        try:
            load_dotenv()
            YT_API_KEY = os.getenv("YT_API_KEY")
        except:
            logger.error(f"YT_API_KEY is not set")
            return

        # Ensure num_workers is an integer
        if not isinstance(num_workers, int) or num_workers <=0:
            logger.error(f"num_workers must be a positive integer. The passed value was: {num_workers}")
            return
        Logging.log_environment_info()

        # Create worker tasks
        retrieve_metadata_workers = [asyncio.create_task(worker_retrieve_metadata()) for _ in range(num_workers)]
        save_metadata_workers = [asyncio.create_task(worker_save_metadata()) for _ in range(min(num_workers*50, 150))]

        # Wait for all workers to finish
        await asyncio.gather(*retrieve_metadata_workers, *save_metadata_workers, return_exceptions=True)

        # Wait for all the queue tasks to finish before the script ends
        await asyncio.gather(
            metadata_queue.join()
        )

async def worker_retrieve_metadata():
    global is403thrown

    while True:
        try:
            active_tasks['retrieve'] += 1

            if not is403thrown:
                # video_ids = ['wXZBAnSObc8'] # for testing specific video_ids
                video_ids: List[str] = await DatabaseOperations.get_video_ids_without_metadata()
                if video_ids:
                    metadatas = await VideoIdOperations.fetch_video_metadata(video_ids)
                    for metadata in metadatas:
                        # Create metadata dictionary
                        meta_dict = await Utils.prep_metadata_dictionary(metadata)

                        # Add metadata dictionary to the queue
                        await metadata_queue.put(meta_dict)

                        # Log the size of the metadata queue
                        logger.info(f"Size of metadata_queue after put: {metadata_queue.qsize()}")

            # logger.info(f"End metadata work on video_id: {meta_dict["video_id"]}")

        except Exception as e:
            logger.error(f"Error retrieving metadata for video_id {meta_dict["video_id"]}\n{e}")

        finally:
            active_tasks['retrieve'] -= 1  # Decrement the counter when a task is finished
            # logger.info(f"ENDING worker_retrieve_metadata")

            # Exit if is403error is True or if metadata_queue is empty and there are no running retrieval tasks
            if (is403thrown) or (video_ids==[] and active_tasks['retrieve'] == 0):
                break

            await asyncio.sleep(0.250)

    logger.info(f"Metadata worker has finished.")

async def worker_save_metadata():
    while True:
        try:
            active_tasks['save'] += 1

            meta_dict = await asyncio.wait_for(metadata_queue.get(), timeout=1)
            logger.info(f"Size of metadata_queue after get: {metadata_queue.qsize()}")
            # logger.info(f"Start saving metadata for video_id: {meta_dict["video_id"]}")
            # logger.info(f"meta_dict\n{meta_dict}")

            # Insert or update video metadata
            await DatabaseOperations.insert_update_video_metadata(meta_dict)

            metadata_queue.task_done()
            # logger.info(f"End saving metadata for video_id: {meta_dict["video_id"]}")

        except Exception as e:
            logger.error(f"Error saving metadata for video_id {meta_dict["video_id"]}\n{e}")

        finally:
            active_tasks['save'] -= 1  # Decrement the counter when a task is finished
            # logger.info(f"ENDING worker_save_metadata")

            # Exit if the user_id queue is empty and there are no running playlist_id tasks
            if metadata_queue.empty() and active_tasks['metadata'] == 0:
                break

            await asyncio.sleep(0.250)

    logger.info(f"Metadata worker has finished.")

def cmd():
    fire.Fire(Fetcher)
