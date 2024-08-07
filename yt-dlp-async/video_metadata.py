import os
import sys
import asyncio
from asyncpg import create_pool
import aiohttp
import shutil
from datetime import datetime
import fire
from dataclasses import dataclass
from loguru import logger
import requests
import json
from dotenv import load_dotenv
from typing import List
from .utils import Utils
from .database import DatabaseOperations

# YT_API_KEY
YT_API_KEY=""

# Configure loguru
# Log file directory and base name
script_name = "metadata"
log_file_name = f"video_{script_name}.log"
log_file_dir = "../data/log/"
log_file_path = os.path.join(log_file_dir, log_file_name)

# Ensure the log directory exists
os.makedirs(log_file_dir, exist_ok=True)

# Check if the log file exists
if os.path.exists(log_file_path):
    # Create a new name for the old log file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_log_file_path = os.path.join(log_file_dir, f"video_{script_name}_{timestamp}.log")
    # Rename the old log file
    shutil.move(log_file_path, new_log_file_path)

# Remove all existing handlers
logger.remove()

# Add a logger for the screen (stderr)
logger.add(sys.stderr, format="{time} - {level} - {message}", level="INFO")

# Add a logger for the log file
logger.add(log_file_path, format="{time} - {level} - {message}", level="INFO")

# Queues for different types of IDs
metadata_queue = asyncio.Queue()
forbidden_queue = asyncio.Queue()

# Boolean to track if we've receive a 403 error
isQuotaExceeded = False

# Counters to track the number of active tasks
active_tasks = {
    'retrieve': 0,
    'save': 0
}

class Logging:
    @staticmethod
    def log_state():
        # Function to log the state of queues and tasks
        logger.info(f"metadata_queue size: {metadata_queue.qsize()}, active tasks: {active_tasks['video_id']}")

class VideoIdOperations:
    # Function to fetch video metadata from the YouTube Data API
    @staticmethod
    async def fetch_video_metadata(video_ids: List[str], worker_id: str) -> List[dict]:
        global YT_API_KEY
        global isQuotaExceeded
        
        video_ids_str = ",".join(video_ids)
        url = f"https://www.googleapis.com/youtube/v3/videos?part=contentDetails,id,liveStreamingDetails,localizations,player,recordingDetails,snippet,statistics,status,topicDetails&id={video_ids_str}&key={YT_API_KEY}"
        
        if not isQuotaExceeded:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    # logger.info(f"[Worker {worker_id}] response:\n{response}")
                    data = await response.json()
                    # logger.info(f"[Worker {worker_id}] response json:\n{data}")

                    if response.status == 200:
                        items = data.get('items', [])
                        # logger.info(f"[Worker {worker_id}] items json:\n{items}")
                        if items:
                            logger.info(f"[Worker {worker_id}] Successfully fetched metadata for {len(items)} video_ids: {items}")
                            # logger.info(f"[Worker {worker_id}] data items:\n {items}")
                            # Collect problematic IDs
                            problematic_ids = set(video_ids) - {item['id'] for item in items}
                            if problematic_ids:
                                # Handle problematic video IDs after returning the successful data
                                asyncio.create_task(VideoIdOperations.handle_problematic_video_ids(list(problematic_ids), worker_id))
                            return items
                    elif response.status == 403:
                            reason = data.get('error', {}).get('errors', [{}])[0].get('reason', 'unknown')
                            logger.error(f"[Worker {worker_id}] Reason provided: {reason}")

                            if reason == "quotaExceeded":
                                logger.error(f"[Worker {worker_id}] Quota Exceeded. Setting isQuotaExceeded to True.")
                                isQuotaExceeded = True

                            else:
                                problematic_ids = set(video_ids)
                                asyncio.create_task(VideoIdOperations.handle_problematic_video_ids(list(problematic_ids), worker_id))
                    else:
                        code = data.get('error', {}).get('code', 'unknown')
                        message = data.get('error', {}).get('message', 'unknown')
                        logger.error(f"[Worker {worker_id}] Failed to fetch metadata for video IDs: {video_ids_str}. Status code: {response.status} {response.reason}. Error Code: {code}. Error Message: {message}")
                    return []
        else:
            logger.error(f"[Worker {worker_id}] Quota has been exceeded, not making request.")
            return []
        
    @staticmethod
    async def handle_problematic_video_ids(video_ids: List[str], worker_id: str):
        problematic_ids = set()

        async def test_chunk(video_ids):
            try:
                data = await VideoIdOperations.fetch_video_metadata(video_ids, worker_id)
                return data
            except Exception as e:
                logger.error(f"[Worker {worker_id}] Exception during test_chunk: {e}")
                return video_ids

        chunk_size = max(len(video_ids) // 3, 1)  # Chunk into thirds
        chunks = [video_ids[i:i + chunk_size] for i in range(0, len(video_ids), chunk_size)]

        # First pass: test chunks and collect problematic chunks
        for chunk in chunks:
            returned_chunk = await test_chunk(chunk)
            if isinstance(returned_chunk, list):
                if not returned_chunk:
                    problematic_ids.update(chunk)
            else:
                problematic_ids.update(chunk)

        # Further investigation if needed (e.g., test individual ids or smaller chunks)
        if problematic_ids:
            for video_id in problematic_ids:
                try:
                    data = await VideoIdOperations.fetch_video_metadata([video_id], worker_id)
                    if not data:
                        await forbidden_queue.put(video_id)
                        logger.info(f"[Worker {worker_id}] 2 Added {video_id} to forbidden_queue. Queue size: {forbidden_queue.qsize()}")
                except Exception as e:
                    await forbidden_queue.put(video_id)
                    logger.error(f"[Worker {worker_id}] Exception during individual video_id fetch: {e}. Added {video_id} to forbidden_queue. Queue size: {forbidden_queue.qsize()}")

@dataclass(slots=True)
class Fetcher:
    shutdown_event = asyncio.Event()

    @staticmethod
    async def fetch(video_ids=None, video_id_files=None, num_workers: int = 2):
        global YT_API_KEY

        try:
            load_dotenv()
            YT_API_KEY = os.getenv("YT_API_KEY")
        except:
            logger.error(f"YT_API_KEY is not set")
            return

        if not isinstance(num_workers, int) or num_workers <= 0:
            logger.error(f"num_workers must be a positive integer. The passed value was: {num_workers}")
            return

        retrieve_metadata_workers = [asyncio.create_task(worker_retrieve_metadata(worker_id=f"retrieve_{i}")) for i in range(num_workers)]
        save_metadata_workers = [asyncio.create_task(worker_save_metadata(worker_id=f"save_{i}")) for i in range(min(num_workers * 50, 150))]

        try:
            if video_ids or video_id_files:
                Utils.read_ids_from_cli_argument_insert_db(video_ids, video_id_files)

        except Exception as e:
            return

        await asyncio.gather(*retrieve_metadata_workers)
        Fetcher.shutdown_event.set()  # Signal workers to shut down
        await asyncio.gather(*save_metadata_workers)

        await metadata_queue.join()  # Ensure all metadata tasks are processed

async def worker_retrieve_metadata(worker_id: str):
    global isQuotaExceeded

    while not Fetcher.shutdown_event.is_set():
        try:
            active_tasks['retrieve'] += 1

            if not isQuotaExceeded:
                video_ids: List[str] = await DatabaseOperations.get_video_ids_without_metadata(forbidden_queue)
                # logger.info(f"[Worker {worker_id}] video_ids: {video_ids}")
                if video_ids:
                    metadatas = await VideoIdOperations.fetch_video_metadata(video_ids, worker_id)
                    for metadata in metadatas:
                        meta_dict = await Utils.prep_metadata_dictionary(metadata)
                        await metadata_queue.put(meta_dict)
                        logger.info(f"[Worker {worker_id}] Size of metadata_queue after put: {metadata_queue.qsize()}")
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Error retrieving metadata: {e}")
        finally:
            active_tasks['retrieve'] -= 1
            if active_tasks['retrieve'] == 0 and (isQuotaExceeded or not video_ids):
                break
        await asyncio.sleep(.250)
    logger.info(f"[Worker {worker_id}] Retrieve metadata worker has finished.")

async def worker_save_metadata(worker_id: str):
    while True:
        try:
            active_tasks['save'] += 1
            # logger.info(f"[Worker {worker_id}] active_tasks['save'] {active_tasks['save']}")
            # logger.info(f"[Worker {worker_id}] metadata_queue.qsize() {metadata_queue.qsize()}")
            if metadata_queue.qsize() > 0:
                meta_dict = await asyncio.wait_for(metadata_queue.get(), timeout=1)
                logger.info(f"[Worker {worker_id}] Size of metadata_queue after get: {metadata_queue.qsize()}")
                await DatabaseOperations.insert_update_video_metadata(meta_dict)
                metadata_queue.task_done()
        except asyncio.TimeoutError:
            logger.error(f"[Worker {worker_id}] asyncio.TimeoutError")
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Error saving metadata: {e}")
        finally:
            active_tasks['save'] -= 1
            # logger.info(f"[Worker {worker_id}] active_tasks['save'] {active_tasks['save']}")

            # logger.info(f"[Worker {worker_id}] Fetcher.shutdown_event.is_set() {Fetcher.shutdown_event.is_set()}")
            # logger.info(f"[Worker {worker_id}] metadata_queue.empty() {metadata_queue.empty()}")
            if Fetcher.shutdown_event.is_set() and metadata_queue.empty():
                break  # Exit only when the queue is empty and shutdown_event is set

        await asyncio.sleep(.250)

    logger.info(f"[Worker {worker_id}] Save metadata worker has finished.")

def cmd():
    fire.Fire(Fetcher)
