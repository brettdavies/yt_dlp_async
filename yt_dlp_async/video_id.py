# Standard Libraries
import asyncio
import subprocess
from typing import List, Optional
from dataclasses import dataclass

# CLI and Logging
import fire
from loguru import logger

# Third Party Libraries
import yt_dlp # The yt_dlp library is not directly used in this file. It is called by a subprocess. The library needs to be installed in the python environment. Referenced here for poetry dependency checks purposes.

# First Party Libraries
from .utils import Utils
from .database import DatabaseOperations
from .logger_config import LoggerConfig

# Configure loguru
LoggerConfig.setup_logger("id")

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
    def log_state():
        """
        Function to log the state of queues and tasks.
        """
        logger.info(f"user_id_queue size: {user_id_queue.qsize()}, active tasks: {active_tasks['user_id']}")
        logger.info(f"playlist_id_queue size: {playlist_id_queue.qsize()}, active tasks: {active_tasks['playlist_id']}")
        logger.info(f"video_id_queue size: {video_id_queue.qsize()}, active tasks: {active_tasks['video_id']}")

class VideoIdOperations:
    """
    Class for performing operations related to video IDs.
    Methods:
    - _run_yt_dlp_command(cmd: List[str]) -> List[str]: Runs a yt-dlp command and returns the output as a list of strings.
    - fetch_video_ids_from_url(url: str) -> List[str]: Fetches the video IDs from the given URL.
    - fetch_playlist_ids_from_user_id(user_url: str) -> List[str]: Fetches the playlist IDs from the given user URL.
    """
    @staticmethod
    async def _run_yt_dlp_command(cmd: List[str]) -> List[str]:
        """
        Runs a yt-dlp command and returns the output as a list of strings.

        Args:
            cmd (List[str]): The yt-dlp command to run.

        Returns:
            List[str]: The output of the command.
        """
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                return stdout.decode().splitlines()
            else:
                raise subprocess.CalledProcessError(process.returncode, cmd, output=stdout, stderr=stderr)
        except subprocess.CalledProcessError as e:
            logger.error(f"CalledProcessError: {e.stderr.decode()}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        return []

    @staticmethod
    async def fetch_video_ids_from_url(url: str) -> List[str]:
        """
        Fetches the video IDs from the given URL.

        Args:
            url (str): The URL of the channel or playlist.

        Returns:
            List[str]: A list of video IDs.
        """
        cmd = ["yt-dlp", "--flat-playlist", "--print", "id", url]
        return await VideoIdOperations._run_yt_dlp_command(cmd)

    @staticmethod
    async def fetch_playlist_ids_from_user_id(user_url: str) -> List[str]:
        """
        Fetches the playlist IDs from the given user URL.

        Args:
            user_url (str): The URL of the user's channel.

        Returns:
            List[str]: A list of playlist IDs.
        """
        cmd = ["yt-dlp", "--flat-playlist", "--print", "%(id)s", user_url]
        return await VideoIdOperations._run_yt_dlp_command(cmd)

@dataclass(slots=True)
class Fetcher:
    @staticmethod
    async def fetch(
        video_ids: Optional[List[str]] = None,
        video_id_files: Optional[List[str]] = None,
        playlist_ids: Optional[List[str]] = None,
        playlist_id_files: Optional[List[str]] = None,
        user_ids: Optional[List[str]] = None,
        user_id_files: Optional[List[str]] = None,
        num_workers: int = 5
    ) -> None:
        """
        Fetches video IDs, playlist IDs, and user IDs from various sources.

        Args:
            video_ids (Optional[List[str]]): A list of video IDs.
            video_id_files (Optional[List[str]]): A list of file paths containing video IDs.
            playlist_ids (Optional[List[str]]): A list of playlist IDs.
            playlist_id_files (Optional[List[str]]): A list of file paths containing playlist IDs.
            user_ids (Optional[List[str]]): A list of user IDs.
            user_id_files (Optional[List[str]]): A list of file paths containing user IDs.
            num_workers (int): The number of worker tasks to create.

        Returns:
            None
        """
        if not isinstance(num_workers, int) or num_workers <= 0:
            logger.error(f"num_workers must be a positive integer. The passed value was: {num_workers}")
            return

        # Create worker tasks
        user_id_workers = [asyncio.create_task(worker_user_ids()) for _ in range(num_workers)]
        playlist_id_workers = [asyncio.create_task(worker_playlist_ids()) for _ in range(num_workers)]
        video_id_workers = [asyncio.create_task(worker_video_ids()) for _ in range(num_workers)]

        # Add IDs to their respective queues
        await Fetcher._add_ids_to_queue(user_ids, user_id_files, user_id_queue, Utils.read_ids_from_file)
        await Fetcher._add_ids_to_queue(playlist_ids, playlist_id_files, playlist_id_queue, Utils.read_ids_from_file)

        try:
            if video_ids or video_id_files:
                Utils.read_ids_from_cli_argument_insert_db(video_ids, video_id_files)
        except Exception as e:
            logger.error(f"Error processing video IDs: {e}")
            return

        # Wait for all workers to finish
        await asyncio.gather(*user_id_workers, *playlist_id_workers, *video_id_workers, return_exceptions=True)

        # Wait for all the queue tasks to finish before the script ends
        await asyncio.gather(
            user_id_queue.join(),
            playlist_id_queue.join(),
            video_id_queue.join()
        )

    @staticmethod
    async def _add_ids_to_queue(ids: Optional[List[str]], id_files: Optional[List[str]], queue: asyncio.Queue, read_func) -> None:
        """
        Adds IDs from lists and files to the specified queue.

        Args:
            ids (Optional[List[str]]): A list of IDs.
            id_files (Optional[List[str]]): A list of file paths containing IDs.
            queue (asyncio.Queue): The queue to add IDs to.
            read_func (Callable): Function to read IDs from files.

        Returns:
            None
        """
        if ids:
            if isinstance(ids, str):
                ids = ids.replace(',', ' ').split()
            for id in ids:
                await queue.put(id)

        if id_files:
            if isinstance(id_files, str):
                id_files = id_files.replace(',', ' ').split()
            for file in id_files:
                ids_from_file = read_func(file)
                for id in ids_from_file:
                    await queue.put(id)

async def worker_user_ids():
    """
    Worker task for processing user IDs.
    """
    while True:
        if user_id_queue.empty() and active_tasks['user_id'] == 0:
            break

        if user_id_queue.empty() :
            await asyncio.sleep(0.250)
            continue

        try:
            active_tasks['user_id'] += 1
            user_id = await asyncio.wait_for(user_id_queue.get(), timeout=1)
            logger.info(f"Start work on user_id: {user_id}")
            logger.info(f"Size of user_id_queue: {user_id_queue.qsize()}")

            user_url = await Utils.prep_url(user_id, 'user')
            user_playlist_url = await Utils.prep_url(user_id, 'user_playlist')
            
            # Process user videos and playlists
            user_playlist_ids = await VideoIdOperations.fetch_playlist_ids_from_user_id(user_playlist_url)
            if user_playlist_ids:
                for playlist_id in user_playlist_ids:
                    await playlist_id_queue.put(playlist_id)

            user_video_ids = await VideoIdOperations.fetch_video_ids_from_url(user_url)
            if user_video_ids:
                for user_video_id in user_video_ids:
                    await video_id_queue.put(user_video_id)

            logger.info(f"End work on user_id: {user_id}")

        except Exception as e:
            logger.error(f"Error processing user_id {user_id}: {e}")

        finally:
            user_id_queue.task_done()
            active_tasks['user_id'] -= 1
            logger.info(f"ENDING worker_user_ids {user_id}")

    logger.info(f"User ID worker has finished.")

async def worker_playlist_ids():
    """
    Worker task for processing playlist IDs.
    """
    while True:
        if user_id_queue.empty() and playlist_id_queue.empty() and (active_tasks['user_id'] + active_tasks['playlist_id']) == 0:
            break

        if playlist_id_queue.empty() :
            await asyncio.sleep(0.250)
            continue

        try:
            active_tasks['playlist_id'] += 1
            playlist_id = await asyncio.wait_for(playlist_id_queue.get(), timeout=1)
            logger.info(f"Start work on playlist_id: {playlist_id}")
            logger.info(f"Size of playlist_id_queue: {playlist_id_queue.qsize()}")
        
            playlist_url = await Utils.prep_url(playlist_id, 'playlist')

            # Process playlist videos
            playlist_video_ids = await VideoIdOperations.fetch_video_ids_from_url(playlist_url)

            if playlist_video_ids:
                for playlist_video_id in playlist_video_ids:
                    await video_id_queue.put(playlist_video_id)
            
            logger.info(f"End work on playlist_id: {playlist_id}")

        except Exception as e:
            logger.error(f"Error processing playlist_id {playlist_id}: {e}")

        finally:
            playlist_id_queue.task_done()
            active_tasks['playlist_id'] -= 1
            logger.info(f"ENDING worker_playlist_ids {playlist_id}")

    logger.info(f"Playlist ID worker has finished.")

async def worker_video_ids():
    """
    Worker task for processing video IDs.
    """
    while True:
        if user_id_queue.empty() and playlist_id_queue.empty() and video_id_queue.empty() and (active_tasks['user_id'] + active_tasks['playlist_id'] + active_tasks['video_id']) == 0:
            break

        if video_id_queue.empty() :
            await asyncio.sleep(0.250)
            continue

        try:
            video_ids_batch = []

            # Collect video IDs from the queue
            active_tasks['video_id'] += 1
            while not video_id_queue.empty():
                video_ids_batch.append(await video_id_queue.get())
                video_id_queue.task_done()

            # Insert collected video IDs into the database
            if video_ids_batch:
                logger.info(f"process_video_id attempting to insert {len(video_ids_batch)} videos")
                await DatabaseOperations.insert_video_ids(video_ids_batch)
                logger.info(f"Currrent count of videos to be processed: {await DatabaseOperations.get_count_videos_to_be_processed()}")

        except Exception as e:
            logger.error(f"Error processing {len(video_ids_batch)} videos {video_ids_batch}: {e}")

        finally:
            active_tasks['video_id'] -= 1
            logger.info(f"ENDING process_video_id")

    logger.info(f"Video ID worker has finished.")

def cmd() -> None:
    """
    Command line interface for running the Fetcher.
    """
    fire.Fire(Fetcher)
