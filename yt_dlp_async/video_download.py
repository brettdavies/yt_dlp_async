# Standard Libraries
import os
import re
import shutil
from dataclasses import dataclass

# CLI and Logging
import fire
from loguru import logger

# Third Party Libraries
import yt_dlp

# First Party Libraries
from .utils import Utils
from .database import DatabaseOperations
from .logger_config import LoggerConfig

# Constants
BASE_URL = "https://www.youtube.com/watch?v="
OUTPUT_DIR = os.getenv('OUTPUT_DIR', '/media/bigdaddy/data/yt_dlp_data/')
SUBTITLES_LANGS = ['en', 'en-orig']
SUBTITLES_FORMAT = 'ttml'

@dataclass
class Fetcher:
    """
    Downloads videos from YouTube and processes their audio files.
    """
    video_id: str
    
    def __init__(self):
        """
        Initializes the Fetcher with default settings.
        """
        self.base_url: str = BASE_URL
    
    def fetch(self, video_id: str) -> None:
        """
        Fetches and processes the video by downloading its audio.

        Args:
            video_id (str): The YouTube video ID to fetch.
        """
        self.video_id = video_id
        self.video_name = f"[Video {self.video_id}] "
        self.video_url = f"{self.base_url}{self.video_id}"

        # Configure loguru
        LOGGER_NAME = f"download_{self.video_id}"
        LoggerConfig.setup_logger(log_name=LOGGER_NAME, log_level='DEBUG')
        logger.info(f"{self.video_name}Fetching video {self.video_id}")
        self.download_audio()

    def download_audio(self) -> None:
        """
        Downloads the audio from the video URL.

        Uses the video URL stored in `self.video_url`.

        Raises:
            Exception: If an error occurs during the download process.
        """

        ydl_opts = {
            'outtmpl': os.path.join(OUTPUT_DIR, '1aTemp', '%(title)s.%(ext)s'),  # Specify output directory and file format
            'format':
                'bestaudio[ext=m4a][acodec^=mp4a][format_note!*=DRC] \
                /bestaudio[ext=m4a][acodec^=mp4a] \
                /bestaudio[acodec^=mp4a][format_note!*=DRC] \
                /bestaudio[format_note!*=DRC]/bestaudio',
            'format_sort': ['abr'],
            'progress_hooks': [self.progress_hook],
            'postprocessor_hooks': [self.postprocess_hook],
            'writeinfojson': True,  # Save metadata to a JSON file
            'writeautomaticsub': True,
            'subtitleslangs': SUBTITLES_LANGS,
            'subtitlesformat': SUBTITLES_FORMAT,
            'embed_chapters': False,
            'add_metadata': False,
            'quiet': False,
            'skip_download': False,  # True = Don't download the video / audio
        }

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([self.video_url])
        except yt_dlp.DownloadError as e:
            logger.error(f"{self.video_name}Download error: {e}")
        except Exception as e:
            logger.error(f"{self.video_name}Unexpected error: {e}")

    def progress_hook(self, d) -> None:
        """
        Callback function called during the download process to report progress.

        Args:
            d (dict): A dictionary containing information about the download progress.
        """
        if d['status'] == 'downloading':
            logger.debug(f"{self.video_name}Downloading: {d['_percent_str']} at {d['_speed_str']} ETA {d['_eta_str']}")
        if d['status'] == 'finished':
            logger.info(f"{self.video_name}Download complete. \"{os.path.basename(d.get('filename'))}\" {d.get('_default_template')}")
    
    def postprocess_hook(self, d) -> bool:
        """
        Post-processes the downloaded video file after download completion.

        Args:
            d (dict): Dictionary containing download information.

        Returns:
            bool: `True` if post-processing is successful, `False` otherwise.
        """
        try:
            if d['status'] == 'finished':
                logger.info(f"{self.video_name}Post-processing started")

                # Access info_dict
                info_dict: dict = d.get('info_dict', {})
                # logger.debug(f"{self.video_name}Info dict:\n{info_dict}")
                
                existing_filepath = info_dict.get('filepath', None)
                if not existing_filepath:
                    return
                
                # Define the new output path and filename
                new_path, new_filename = self.determine_path_and_name(info_dict)

                # Extract the base name (excluding extension) of the audio file
                existing_base_name = os.path.splitext(os.path.basename(existing_filepath))[0]
                
                # Ensure the new directory exists
                os.makedirs(new_path, exist_ok=True)
                
                # Get the current directory
                current_directory = os.path.dirname(existing_filepath)
                
                # Iterate over all files in the current directory
                for file_name in os.listdir(current_directory):
                    _, file_extension = os.path.splitext(file_name)
                    if file_name.startswith(existing_base_name):
                        # Construct full file paths
                        old_file_path = os.path.join(current_directory, file_name)
                        new_file_path = os.path.join(new_path, new_filename + file_extension)
                        
                        # Move the file to the new directory
                        shutil.move(old_file_path, new_file_path)

                        logger.info(f"{self.video_name}Moved: {old_file_path} -> {new_file_path}")
                        if file_extension == '.m4a':
                            # Initialize video_file_info as a dictionary
                            video_file_info = {}
                            def populate_video_file_info(info_dict, new_file_path):
                                # Ensure info_dict contains the necessary keys
                                video_id = info_dict.get('id', '')
                                a_format_id = info_dict.get('format_id')
                                file_size = info_dict.get('filesize', 0)  # Provide a default value of 0 if filesize is not present
                                local_path = new_file_path.replace(OUTPUT_DIR, '')

                                logger.debug(f"{self.video_name}Video ID: {video_id}")
                                logger.debug(f"{self.video_name}Format ID: {a_format_id}")
                                logger.debug(f"{self.video_name}File size: {file_size}")
                                logger.debug(f"{self.video_name}Local path: {local_path}")

                                # Check if video_id is not empty
                                if video_id:
                                    video_file_info[video_id] = {
                                        'a_format_id': a_format_id,
                                        'file_size': file_size,
                                        'local_path': local_path
                                    }
                                else:
                                    logger.error("Video ID is missing in info_dict")
                            populate_video_file_info(info_dict, new_file_path)
                            logger.debug(f"{self.video_name}Video file info: {video_file_info}")
                            
                            logger.info(f"{self.video_name}Inserting video file info into the database")
                            db_success: bool = DatabaseOperations.update_audio_file(video_file_info)
                            if not db_success:
                                logger.error(f"{self.video_name}Failed to insert video file info into the database")
                                return False
                            logger.info(f"{self.video_name}Inserted video file info into the database")

                logger.info(f"{self.video_name}Post-processing finished")
            return True

        except Exception as e:
            logger.error(f"{self.video_name}Post-processing error\n{e}")
            return False

    def determine_path_and_name(self, info_dict: dict) -> tuple:
        """
        Determines the file path and name for the video based on provided metadata.

        Args:
            info_dict (dict): Dictionary containing information about the video.

        Returns:
            tuple: A tuple `(path, file_name)` for storing the video.
        """
        title:str = info_dict.get('title', )
        description:str = info_dict.get('description', )
        video_id:str = info_dict.get('id', )
        asr:str = info_dict.get('asr', )
        language:str = info_dict.get('language', )
        acodec:str = info_dict.get('acodec', )
        format_id:str = info_dict.get('format_id', )
        quality:str = str(info_dict.get('quality', ))
        format_note:str = info_dict.get('format_note', )
        dynamic_range:str = str(info_dict.get('dynamic_range', 'None'))
        if dynamic_range == 'None':
            dynamic_range = 'No DRC'
        duration:str = info_dict.get('duration', '0')

        # Try to extract date from title first
        date_obj = Utils.extract_date(title)

        # If date is not found in the title, try the description
        if not date_obj:
            date_obj = Utils.extract_date(description)

        # If date is still not found, use 'date_unknown'
        if not date_obj:
            path_date = 'unknown_date'
            filename_date = 'unknown_date'
        else:
            # Convert date to the required format
            path_date = date_obj.strftime('%Y/%m/%d')
            filename_date = date_obj.strftime('%Y.%m.%d')

        # Try to extract teams from title first
        home_team, away_team = Utils.extract_teams(title)

        if home_team == 'Unknown' and away_team == 'Unknown':
            # Try to extract teams from description
            home_team, away_team = Utils.extract_teams(description)
            
        # Convert the duration to a string
        duration_string = self.format_duration(str(duration))

        # Construct the path and file name
        path = f"{OUTPUT_DIR}{path_date}/"
        if home_team == 'Unknown' or away_team == 'Unknown':
            path = f"{OUTPUT_DIR}unknown_teams/{path_date}/"        
        file_name = f"{away_team} at {home_team} - {filename_date} - [{language}][{duration_string}][{asr}][{dynamic_range}][{acodec}][{quality}][{format_note}]{{fid-{format_id}}}{{yt-{video_id}}}"

        return path, file_name

    def format_duration(self, duration: str) -> str:
        """
        Formats the duration string into a human-readable format.

        Args:
            duration (str): The duration string in ISO 8601 format.

        Returns:
            str: The formatted duration as 'XH YM ZS'.
        """
        match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration)
        if not match:
            return str(duration)
        hours, minutes, seconds = match.groups()
        
        return f"{hours or '0H'} {minutes or '0M'} {seconds or '0S'}".strip()

def cmd() -> None:
    """
    Provides a command-line interface for running the Fetcher.
    """
    fire.Fire(Fetcher())
