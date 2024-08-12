from dataclasses import dataclass
from datetime import datetime
import os
import sys
import shutil
import asyncio
import fire
from loguru import logger
from typing import List
from .database import DatabaseOperations

# Configure loguru
# Log file directory and base name
script_name = "file"
log_file_dir = "./data/log/"
log_file_ext = ".log"
log_file_name = f"video_{script_name}"
log_debug_file_name = f"video_{script_name}_debug"
os.makedirs(log_file_dir, exist_ok=True) # Ensure the log directory exists
log_file_path = os.path.join(log_file_dir, log_file_name + log_file_ext)
log_debug_file_path = os.path.join(log_file_dir, log_debug_file_name + log_file_ext)

# Check if the log file exists
if os.path.exists(log_file_path):
    # Create a new name for the old log file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_log_file_path = os.path.join(log_file_dir, f"{log_file_name}_{timestamp}.log")
    # Rename the old log file
    shutil.move(log_file_path, new_log_file_path)

# Check if the debug log file exists
if os.path.exists(log_debug_file_path):
    # Create a new name for the old log file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_debug_log_file_path = os.path.join(log_file_dir, f"{log_debug_file_name}_{timestamp}.log")
    # Rename the old log file
    shutil.move(log_debug_file_path, new_debug_log_file_path)

# Remove all existing handlers
logger.remove()

# Add a logger for the screen (stderr)
logger.add(sys.stderr, format="{time} - {level} - {message}", level="INFO")

# Add a logger for the log files
logger.add(log_file_path, format="{time} - {level} - {message}", level="INFO")
# logger.add(log_debug_file_path, format="{time} - {level} - {message}", level="DEBUG")

# Queues
video_file_queue = asyncio.Queue()

class VideoFileOperations:
    @staticmethod
    async def run_video_download(worker_id: str):
        try:
            while video_file_queue.qsize() >=1 :
                video_id = await video_file_queue.get()
                logger.info(f"Size of video_file_queue after get: {video_file_queue.qsize()}")
                logger.info(f"[Worker {worker_id}] Starting work on video_id: {video_id}")

                try:
                    logger.info(f"[Worker {worker_id}] Processing video {video_id}")
                    cmd = ["python3", "-m", "yt-dlp-async.video_download", "--video_id", f"'{video_id}'"]
                    process = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
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
    @staticmethod
    async def fetch(anything=None, num_workers: int = 10):
        if not isinstance(num_workers, int) or num_workers <= 0:
            logger.error(f"num_workers must be a positive integer. The passed value was: {num_workers}")
            return

        video_ids: List[str] = DatabaseOperations.get_video_ids_without_files()
        for video_id in video_ids:
            await video_file_queue.put(video_id)
        logger.info(f"Size of video_file_queue after put: {video_file_queue.qsize()}")
        
        video_file_workers = [asyncio.create_task(VideoFileOperations.run_video_download(worker_id=f"download_file_{i}")) for i in range(num_workers)]

        await video_file_queue.join()  # Wait until the queue is fully processed
        await asyncio.gather(*video_file_workers)  # Wait for all workers to finish

def cmd():
    fire.Fire(Fetcher)
