# Standard Libraries
import os
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

# CLI, Logging, and Configuration
import fire
from loguru import logger
from dotenv import load_dotenv

# First Party Libraries
from .utils import Utils
from .database import DatabaseOperations
from .logger_config import LoggerConfig

# Load environment variables
load_dotenv()

# YT_API_KEY from environment
YT_API_KEY = os.getenv("YT_API_KEY", "")

# Configure loguru
LOGGER_NAME = "metadata"
LoggerConfig.setup_logger(LOGGER_NAME)

# Boolean to track if we've receive a 403 error
is_quota_exceeded = False

class QueueManager:
    """
    Manages queues for metadata and forbidden video IDs.
    """
    def __init__(self):
        self.metadata_queue = asyncio.Queue()
        self.active_tasks = {
            'retrieve': 0,
            'save': 0
        }

class Logging:
    @staticmethod
    def log_state(queue_manager: QueueManager) -> None:
        """
        Logs the state of queues and tasks.
        """
        logger.info(f"metadata_queue size: {queue_manager.metadata_queue.qsize()}, active tasks: {queue_manager.active_tasks['video_id']}")

class VideoIdOperations:
    """
    Handles fetching video metadata from the YouTube Data API.
    """
    def __init__(self, queue_manager: QueueManager):
        """
        Initializes the Fetcher with a QueueManager instance.

        Args:
            queue_manager (QueueManager): An instance of QueueManager to manage queues.
        """
        self.queue_manager = queue_manager

    @staticmethod
    async def fetch_video_metadata(video_ids: List[str], worker_id: str) -> List[Dict[str, Any]]:
        """
        Function to fetch video metadata from the YouTube Data API.

        Args:
            video_ids (List[str]): List of video IDs to fetch metadata for.
            worker_id (str): ID of the worker executing the function.

        Returns:
            List[Dict[str, Any]]: List of dictionaries containing the fetched metadata for each video ID.
        """
        global is_quota_exceeded
        
        video_ids_str = ",".join(video_ids)
        url = f"https://www.googleapis.com/youtube/v3/videos?part=contentDetails,id,liveStreamingDetails,localizations,player,recordingDetails,snippet,statistics,status,topicDetails&id={video_ids_str}&key={YT_API_KEY}"
        
        if not isQuotaExceeded:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    logger.debug(f"[Worker {worker_id}] response:\n{response}")
                    data = await response.json()
                    logger.debug(f"[Worker {worker_id}] response json:\n{data}")

                    if response.status == 200:
                        items = data.get('items', [])
                        # logger.info(f"[Worker {worker_id}] items json:\n{items}")
                        if items:
                            logger.info(f"[Worker {worker_id}] Successfully fetched metadata for {len(items)} video_ids")
                            logger.debug(f"[Worker {worker_id}] data items:\n {items}")

                            # Collect problematic IDs
                            problematic_ids = set(video_ids) - {item['id'] for item in items}
                            if problematic_ids:
                                # Handle problematic video IDs after returning the successful data
                                logger.info(f"[Worker {worker_id}] Found problematic video IDs: {problematic_ids}")
                                DatabaseOperations.set_video_id_failed_metadata_true(problematic_ids)
                            return items
                    elif response.status == 403:
                            reason = data.get('error', {}).get('errors', [{}])[0].get('reason', 'unknown')
                            logger.error(f"[Worker {worker_id}] Reason provided: {reason}")

                            if reason == "quotaExceeded":
                                logger.error(f"[Worker {worker_id}] Quota Exceeded. Setting isQuotaExceeded to True.")
                                isQuotaExceeded = True

                    else:
                        code = data.get('error', {}).get('code', 'unknown')
                        message = data.get('error', {}).get('message', 'unknown')
                        logger.error(f"[Worker {worker_id}] Failed to fetch metadata for video IDs: {video_ids_str}. Status code: {response.status} {response.reason}. Error Code: {code}. Error Message: {message}")
                    return []
        else:
            logger.error(f"[Worker {worker_id}] Quota has been exceeded, not making request.")
            return []

@dataclass(slots=True)
class Fetcher:
    """
    Class that handles fetching video metadata from the YouTube Data API.
    """
    queue_manager: QueueManager
    shutdown_event: asyncio.Event

    def __init__(self):
        """
        Initializes the Fetcher with a QueueManager instance.

        Args:
            queue_manager (QueueManager): An instance of QueueManager to manage queues.
        """
        # Instantiate QueueManager
        self.queue_manager = QueueManager()
        self.shutdown_event = asyncio.Event()

    async def fetch(self, video_ids: Optional[List[str]] = None, video_id_files: Optional[List[str]] = None, num_workers: int = 2) -> None:
        """
        Function to fetch video metadata from the YouTube Data API.

        Args:
            video_ids (List[str], optional): List of video IDs to fetch metadata for. Defaults to None.
            video_id_files (List[str], optional): List of file paths containing video IDs to fetch metadata for. Defaults to None.
            num_workers (int, optional): Number of worker tasks to use for fetching metadata. Defaults to 2.
        """
        try:
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

            try:
                if video_ids or video_id_files:
                    Utils.read_ids_from_cli_argument_insert_db(video_ids, video_id_files)

            except Exception as e:
                return

            retrieve_metadata_workers = [asyncio.create_task(worker_retrieve_metadata(self.queue_manager, worker_id=f"retrieve_{i}")) for i in range(num_workers)]
            save_metadata_workers = [asyncio.create_task(worker_save_metadata(self.queue_manager, worker_id=f"save_{i}")) for i in range(min(num_workers * 50, 150))]

            await asyncio.gather(*retrieve_metadata_workers)
            Fetcher.shutdown_event.set()  # Signal workers to shut down
            await asyncio.gather(*save_metadata_workers)

            await self.queue_manager.metadata_queue.join()  # Ensure all metadata tasks are processed

        finally:
            DatabaseOperations.close_connection_pool() # Close the connection pool


async def worker_retrieve_metadata(queue_manager: QueueManager, worker_id: str) -> None:
    """
    Worker function that retrieves video metadata from the YouTube Data API.

    Args:
        worker_id (str): ID of the worker executing the function.
    """
    global is_quota_exceeded

    while not Fetcher.shutdown_event.is_set():
        try:
            queue_manager.active_tasks['retrieve'] += 1

            if not is_quota_exceeded:
                video_ids: List[str] = await DatabaseOperations.get_video_ids_without_metadata()
                logger.info(f"[Worker {worker_id}] {len(video_ids)} video_ids")
                logger.debug(f"[Worker {worker_id}] {video_ids}")
                if video_ids:
                    metadatas = await VideoIdOperations.fetch_video_metadata(video_ids, worker_id)
                    for metadata in metadatas:
                        meta_dict = await Utils.prep_metadata_dictionary(metadata)
                        await queue_manager.metadata_queue.put(meta_dict)
                        logger.info(f"[Worker {worker_id}] Size of metadata_queue after put: {queue_manager.metadata_queue.qsize()}")
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Error retrieving metadata: {e}")
        finally:
            queue_manager.active_tasks['retrieve'] -= 1
            if queue_manager.active_tasks['retrieve'] == 0 and (is_quota_exceeded or not video_ids):
                break
        await asyncio.sleep(.250)
    logger.info(f"[Worker {worker_id}] Retrieve metadata worker has finished.")

async def worker_save_metadata(queue_manager: QueueManager, worker_id: str) -> None:
    """
    Worker function that saves video metadata to the database.

    Args:
        worker_id (str): ID of the worker executing the function.
    """
    while True:
        try:
            queue_manager.active_tasks['save'] += 1
            if queue_manager.metadata_queue.qsize() > 0:
                meta_dict = await asyncio.wait_for(queue_manager.metadata_queue.get(), timeout=1)
                logger.info(f"[Worker {worker_id}] Size of metadata_queue after get: {queue_manager.metadata_queue.qsize()}")
                await DatabaseOperations.insert_update_video_metadata(meta_dict)
                queue_manager.metadata_queue.task_done()
        except asyncio.TimeoutError:
            logger.error(f"[Worker {worker_id}] asyncio.TimeoutError")
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Error saving metadata: {e}")
        finally:
            queue_manager.active_tasks['save'] -= 1
            if Fetcher.shutdown_event.is_set() and queue_manager.metadata_queue.empty():
                break  # Exit only when the queue is empty and shutdown_event is set
        await asyncio.sleep(.250)
    logger.info(f"[Worker {worker_id}] Save metadata worker has finished.")

def cmd() -> None:
    """
    Command line interface for running the Fetcher.
    """
    fire.Fire(Fetcher())
