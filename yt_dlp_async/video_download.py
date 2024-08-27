# Standard Libraries
import os
import re
import asyncio
import shutil
import subprocess
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

# CLI and Logging
import fire
from loguru import logger

# Third Party Libraries
import yt_dlp

# First Party Libraries
from .utils import Utils
from .database import DatabaseOperations
from .logger_config import LoggerConfig
from .e_events import EventFetcher

# Global video_name
video_name: str = None

@dataclass(slots=True)
class Fetcher:
    """
    This module contains the `Fetcher` class which is responsible for fetching and downloading videos from YouTube.
    Attributes:
        video_id (str): The ID of the video to be fetched.
    Methods:
        fetch(): Fetches and downloads the video.
        download_audio(url): Downloads the audio of the video.
        progress_hook(d): Progress hook function for the download process.
        postprocess_hook(d): Postprocess hook function for the download process.
        determine_path_and_name(info_dict): Determines the path and filename for the downloaded audio file.
        extract_date(text): Extracts the date from the given text.
        format_duration(duration): Formats the duration of the video.
    """
    def __init__(self, video_id: str):
        self.video_id = video_id
        self.video_name = None

    def fetch(self):
        """
        Fetches the video by downloading its audio from the given video URL.

        Returns:
            None
        """
        # Configure loguru
        log_file_name = f"video_download_{self.video_id}"
        LoggerConfig.setup_logger(log_file_name)

        self.video_name = "[Video " + self.video_id + "] "
        # URL
        base_url: str = "https://www.youtube.com/watch?v="
        video_url = f"{base_url}{self.video_id}"

        self.download_audio(video_url)

    def download_audio(self, url):
        """
        Downloads the audio from the given URL.

        Args:
            url (str): The URL of the video to download the audio from.

        Raises:
            Exception: If an error occurs during the download process.

        Returns:
            None
        """
        try:
            ydl_opts = {
                'outtmpl': '/media/bigdaddy/data/yt-dlp-data/1aTemp/%(title)s.%(ext)s',  # Specify output directory and file format
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
                'subtitleslangs': ['en','en-orig'],
                'subtitlesformat': 'ttml',
                'embed_chapters': False,
                'add_metadata': False,
                'quiet': False,
                'skip_download': False,  # Don't download the video
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
        except Exception as e:
            logger.error(f"{self.video_name}Error: {e}")

    def progress_hook(self, d):
        """
        A progress hook function that is called during the download process.

        Parameters:
        - d (dict): A dictionary containing information about the download progress.

        Returns:
        - None

        Description:
        - This function is called for each progress update during the download process.
        - It logs a message when the download is finished, including the video name, the filename, and the default template.
        """
        if d['status'] == 'finished':
            logger.info(f"{self.video_name}Download complete. {os.path.basename(d.get('filename'))} {d.get('_default_template')}")

    def postprocess_hook(self, d):
        """
        Postprocesses the downloaded video file.
        Args:
            d (dict): The dictionary containing the download information.
        Returns:
            bool: True if the postprocessing is successful, False otherwise.
        """
        try:
            if d['status'] == 'finished':
                # Access info_dict
                info_dict: dict = d.get('info_dict', {})
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
                            video_file_info: Dict[str, Any] = {
                                'video_id': info_dict.get('id', ''),
                                'format_id': info_dict.get('format_id'),
                                'file_size': info_dict.get('filesize', ),
                                'local_path': new_file_path
                            }
                            DatabaseOperations.insert_video_file(video_file_info)
            return True

        except Exception as e:
            logger.error(f"{self.video_name}{e}")
            return False  # Indicate failure

    def determine_path_and_name(self, info_dict: dict):
        """
        Determines the path and file name for a video based on the given information.
        Args:
            info_dict (dict): A dictionary containing information about the video.
        Returns:
            tuple: A tuple containing the path and file name for the video.
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
        duration:int = info_dict.get('duration', 0)

        # Try to extract date from title first
        date_obj, title = self.extract_date(title)

        # If date is not found in the title, try the description
        if not date_obj:
            date_obj, description = self.extract_date(description)

        # If date is still not found, use 'date_unknown'
        if not date_obj:
            path_date = 'unknown_date'
            filename_date = 'unknown_date'
        else:
            # Convert date to the required format
            path_date = date_obj.strftime('%Y/%m/%d')
            filename_date = date_obj.strftime('%Y.%m.%d')

            # Run the EventFetcher
            self.run_event_fetcher(path_date)

        # Try to extract teams from title first
        home_team, away_team = Utils.extract_teams(title)

        if home_team == 'Unknown' and away_team == 'Unknown':
            # Try to extract teams from description
            home_team, away_team = Utils.extract_teams(description)

        # If home_team is unknown, try looking it up in e_events using the date and away team
        if home_team == 'Unknown':
            home_team = DatabaseOperations.get_e_events_team_info(date_obj, away_team, is_home_unknown=True)
        # If away_team is unknown, try looking it up in e_events using the date and home team
        elif away_team == 'Unknown':
            away_team = DatabaseOperations.get_e_events_team_info(date_obj, home_team, is_home_unknown=False)

        # Retrieve the e_event_id using the date, home team, and away team
        e_event_id = DatabaseOperations.get_e_events_event_id(date_obj, home_team, away_team)
        
        # Convert the duration to a string
        duration_string = self.format_duration(duration)
        
        # Construct the path and file name
        e_id = ''
        if e_event_id:
            e_id = f"{{e-{e_event_id}}}"
        path = f"/media/bigdaddy/data/yt-dlp-data/{path_date}/"
        if home_team == 'Unknown' or away_team == 'Unknown':
            path = f"/media/bigdaddy/data/yt-dlp-data/unknown_teams/{path_date}/"        
        file_name = f"{away_team} at {home_team} - {filename_date} - [{language}][{duration_string}][{asr}][{dynamic_range}][{acodec}][{quality}][{format_note}]{{fid-{format_id}}}{e_id}{{yt-{video_id}}}"
        return path, file_name

    def extract_date(self, text: str):
        """
        Extracts a date from the given text in various formats.

        Args:
            text (str): The text from which to extract the date.

        Returns:
            tuple: A tuple containing the extracted date as a datetime object and the remaining text.
        """
        # Extract date in DD.MM.YYYY format
        date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%d.%m.%Y'), text

        # Extract date in MM.DD.YYYY format
        date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%m.%d.%Y'), text

        # Extract date in MM.DD.YY format
        date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{2})', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%m.%d.%y'), text

        # Extract date in MM/DD/YY format
        date_match = re.search(r'(\d{1,2}\/\d{1,2}\/\d{2})', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%m/%d/%y'), text

        # Extract date in MM-DD-YY format
        date_match = re.search(r'(\d{1,2}\-\d{1,2}\-\d{2})', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%m-%d-%y'), text

        # Extract date in "Month DD, YYYY" format
        date_match = re.search(r'(\b\w+\s\d{1,2},\s\d{4}\b)', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%B %d, %Y'), text

        return None, text

    def format_duration(self, duration:int) -> str:
        """
        Formats the given duration in seconds into a string representation in the format HnnMnnSnn.
        Parameters:
        - duration (int): The duration in seconds to be formatted.
        Returns:
        - str: The formatted duration string in the format HnnMnnSnn.
        Raises:
        - Exception: If an error occurs during the formatting process.
        """
        try:
            total_seconds = int(duration)
            h, remainder = divmod(total_seconds, 3600)
            m, s = divmod(remainder, 60)
        
            # Format as HnnMnnSnn
            return f"H{h:02}M{m:02}S{s:02}" # formatted_time
        except Exception as e:
            logger.error(f"{e}")

def cmd() -> None:
    """
    Command line interface for running the Fetcher.
    """
    fire.Fire(Fetcher)

# Run the main function
if __name__ == "__main__":
    fire.Fire(Fetcher.fetch)
