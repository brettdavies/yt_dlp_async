# Standard Libraries
import os
import asyncio
import subprocess
from typing import List, Any
from dataclasses import dataclass

# CLI, Logging, Configuration
import fire
from loguru import logger

# First Party Libraries
from .database import DatabaseOperations
from .logger_config import LoggerConfig
from .utils import Utils

# Configure loguru
LOG_NAME = "file"
LoggerConfig.setup_logger(log_name=LOG_NAME)

class QueueManager:
    """
    Manages the queue for video files to be processed.
    """
    def __init__(self):
        self.video_file_queue = asyncio.Queue()

class VideoFileOperations:
    """
    Handles operations related to video files, such as downloading and identifying existing files.
    """
    def __init__(self, queue_manager: QueueManager):
        """
        Initializes VideoFileOperations with a QueueManager instance.

        Args:
        queue_manager (QueueManager): An instance of QueueManager to manage queues.
        """
        self.queue_manager = queue_manager

    async def run_video_download(self, worker_id: str) -> None:
        """
        Asynchronously runs the video download process for a single video ID.

        Args:
            worker_id (str): The ID of the worker.

        Raises:
            Exception: If there is an error fetching the video.
        """
        try:
            video_id: str = await self.queue_manager.video_file_queue.get()
            logger.info(f"Size of video_file_queue after get: {self.queue_manager.video_file_queue.qsize()}")
            logger.info(f"[Worker {worker_id}] Starting work on video_id: {video_id}")

            try:
                logger.info(f"[Worker {worker_id}] Processing video {video_id}")
                cmd: List[str] = ["poetry", "run", "download-audio-file", "fetch", "--video_id", f"\"{video_id}\""]
                logger.debug(f"[Worker {worker_id}] Running command: {' '.join(cmd)}")
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()
                logger.debug(f"[Worker {worker_id}] Subprocess stdout\n{stdout.decode()}")
                logger.debug(f"[Worker {worker_id}] Subprocess stderr\n{stderr.decode()}")
                if process.returncode != 0:
                    logger.error(f"[Worker {worker_id}] Error fetching video {video_id}: {stderr.decode()}")
                    raise Exception(f"[Worker {worker_id}] Error fetching video {video_id}: {stderr.decode()}")
                logger.info(f"[Worker {worker_id}] Finished processing video {video_id}")
            finally:
                self.queue_manager.video_file_queue.task_done()

        except subprocess.CalledProcessError as e:
            logger.error(f"[Worker {worker_id}] Subprocess error: {e}")
        except asyncio.QueueEmpty:
            logger.error(f"[Worker {worker_id}] Queue is empty")
        except Exception as e:
            logger.error(f"[Worker {worker_id}] Unexpected error: {e}")

    @staticmethod
    async def identify_video_files(existing_videos_dir) -> None:
        """
        Identifies existing video files in the given directory and updates the database.

        Args:
            existing_videos_dir (str): The directory containing existing video files.

        Raises:
            Exception: If an error occurs during the identification process.
        """
        try:
            video_file_info = {}
            for root, _, files in os.walk(existing_videos_dir):
                for file in files:
                    if file.endswith('.m4a'):
                        video_file_info = {}
                        full_path = os.path.join(root, file)
                        relative_path = full_path.replace(existing_videos_dir, '')
                        video_id, a_format_id = Utils.extract_video_info_filepath(file)
                        if video_id:
                            video_file_info[video_id] = {
                                'local_path': relative_path,
                                'file_size': os.path.getsize(full_path),
                                'a_format_id': a_format_id
                            }
                            logger.info(f"Found video_id {video_id} with file size {video_file_info[video_id]['file_size']} bytes and audio_format_id {video_file_info[video_id]['a_format_id']} at \"{video_file_info[video_id]['local_path']}\".")

            if video_file_info:
                DatabaseOperations.update_audio_file(video_file_info)

        except Exception as e:
            logger.error(f"{e}")

@dataclass(slots=True)
class Fetcher:
    """
    Coordinates the fetching of video files using multiple workers.
    """

    queue_manager: QueueManager
    logger: Any

    def __init__(self):
        """
        Initializes the Fetcher with a new QueueManager instance.
        """

        self.logger: Any = logger
        # Instantiate QueueManager
        self.queue_manager = QueueManager()

    async def fetch(self, existing_videos_dir: str = None, num_workers: int = 10) -> None:
        """
        Fetches video files using multiple workers.

        Args:
            existing_videos_dir (str, optional): Directory containing existing video files to identify.
            num_workers (int): The number of worker tasks to use for fetching.

        Raises:
            ValueError: If num_workers is not a positive integer.

        Returns:
            None
        """
        try:
            if not isinstance(num_workers, int) or num_workers <= 0:
                logger.error(f"num_workers must be a positive integer. The passed value was: {num_workers}")
                raise ValueError(f"num_workers must be a positive integer. The passed value was: {num_workers}")

            if existing_videos_dir:
                if not os.path.isdir(existing_videos_dir):
                    logger.error(f"existing_videos_dir must be a directory. The passed value was: {existing_videos_dir}")
                    return
                logger.info(f"Processing existing videos in directory: {existing_videos_dir}")
                await VideoFileOperations.identify_video_files(existing_videos_dir)
            
            video_ids: List[str] = DatabaseOperations.get_video_ids_without_files()
            for video_id in video_ids:
                await self.queue_manager.video_file_queue.put(video_id)
            logger.info(f"Size of video_file_queue after put: {self.queue_manager.video_file_queue.qsize()}")

            video_file_workers: List[asyncio.Task] = [asyncio.create_task(VideoFileOperations(self.queue_manager).run_video_download(worker_id=f"download_file_{i}")) for i in range(num_workers)]

            await self.queue_manager.video_file_queue.join()  # Wait until the queue is fully processed
            await asyncio.gather(*video_file_workers)  # Wait for all workers to finish

        finally:
            DatabaseOperations.close() # Close the connection pool

def cmd() -> None:
    """
    Runs the command-line interface for the Fetcher.
    """
    fire.Fire(Fetcher())
