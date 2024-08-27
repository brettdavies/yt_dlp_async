# Standard Libraries
import asyncio
import subprocess
from typing import List, Any
from dataclasses import dataclass

# CLI, Logging, Configuration
import fire
from loguru import logger

# First Party Libraries
from .utils import Utils
from .database import DatabaseOperations
from .logger_config import LoggerConfig

# Configure loguru
LoggerConfig.setup_logger("file")

# Queues
video_file_queue = asyncio.Queue()

class VideoFileOperations:
    """
    Class that handles video file operations.

    Methods:
    - run_video_download(worker_id: str) -> None: Asynchronously runs the video download process.
    """
    @staticmethod
    async def run_video_download(worker_id: str) -> None:
        """
        Asynchronously runs the video download process.

        Args:
        - worker_id (str): The ID of the worker.

        Raises:
        - Exception: If there is an error fetching the video.

        Returns:
        - None
        """
        try:
            while video_file_queue.qsize() >= 1:
                video_id: str = await video_file_queue.get()
                logger.info(f"Size of video_file_queue after get: {video_file_queue.qsize()}")
                logger.info(f"[Worker {worker_id}] Starting work on video_id: {video_id}")

                try:
                    logger.info(f"[Worker {worker_id}] Processing video {video_id}")
                    cmd: List[str] = ["python3", "-m", "yt-dlp-async.video_download", "--video_id", f"'{video_id}'"]
                    process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    stdout: bytes
                    stderr: bytes
                    stdout, stderr = await process.communicate()
                    if process.returncode != 0:
                        logger.error(f"[Worker {worker_id}] Error fetching video {video_id}: {stderr.decode()}")
                        raise Exception(f"[Worker {worker_id}] Error fetching video {video_id}: {stderr.decode()}")
                    logger.info(f"[Worker {worker_id}] Finished processing video {video_id}")
                    # logger.debug(f"[Worker {worker_id}] Subprocess output {stdout}")
                finally:
                    video_file_queue.task_done()
        except Exception as e:
            logger.error(f"Error in worker {worker_id}: {e}")

@dataclass(slots=True)
class Fetcher:
    """
    Class that handles fetching video files.

    Methods:
    - fetch(anything: Any = None, num_workers: int = 10) -> None: Fetches video files using multiple workers.
    """

    @staticmethod
    async def fetch(anything: Any = None, num_workers: int = 10) -> None:
        """
        Fetches video files using multiple workers.

        Args:
        - anything (Any): Placeholder argument for compatibility.
        - num_workers (int): The number of worker tasks to use for fetching.

        Returns:
        - None
        """
        if not isinstance(num_workers, int) or num_workers <= 0:
            logger.error(f"num_workers must be a positive integer. The passed value was: {num_workers}")
            return

        video_ids: List[str] = DatabaseOperations.get_video_ids_without_files()
        for video_id in video_ids:
            await video_file_queue.put(video_id)
        logger.info(f"Size of video_file_queue after put: {video_file_queue.qsize()}")

        video_file_workers: List[asyncio.Task] = [asyncio.create_task(VideoFileOperations.run_video_download(worker_id=f"download_file_{i}")) for i in range(num_workers)]

        await video_file_queue.join()  # Wait until the queue is fully processed
        await asyncio.gather(*video_file_workers)  # Wait for all workers to finish

def cmd() -> None:
    """
    Command line interface for running the Fetcher.
    """
    fire.Fire(Fetcher)
