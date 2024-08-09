from dataclasses import dataclass
from datetime import datetime
import os
import sys
import re
import shutil
import fire
from loguru import logger
from typing import Dict, Any
from .utils import Utils
from .database import DatabaseOperations
from .e_events import EventFetcher
import yt_dlp

# Global video_name
video_name: str = None

@dataclass(slots=True)
class Fetcher:
    def fetch(video_id: str):
        # Configure loguru
        # Log file directory and base name
        script_name = f"download_{video_id}"
        log_file_name = f"video_{script_name}.log"
        log_file_dir = "./data/log/"
        os.makedirs(log_file_dir, exist_ok=True) # Ensure the log directory exists
        log_file_path = os.path.join(log_file_dir, log_file_name)

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

        global video_name
        video_name = "[Video " + video_id + "] "
        # URL
        base_url: str = "https://www.youtube.com/watch?v="
        video_url = f"{base_url}{video_id}"

        download_audio(video_url)

# def get_formats(url: str):
#     # Function to get available formats
#     try:
#         ydl_opts = {
#             'listformats': True,
#         }

#         # Define a custom hook to capture the output
#         class MyLogger:
#             def __init__(self):
#                 self.formats = []

#             def debug(self, msg):
#                 if msg.startswith('[info] Available formats for'):
#                     self.formats.append(msg)

#             def warning(self, msg):
#                 pass

#             def error(self, msg):
#                 print(msg)

#         logger = MyLogger()
#         ydl_opts['logger'] = logger

#         with yt_dlp.YoutubeDL(ydl_opts) as ydl:
#             await asyncio.to_thread(ydl.download, [url])

#         # Print the captured formats
#         for format in logger.formats:
#             print(format)

#     except Exception as e:
#         print(f"Error: {e}")

def extract_date(text: str):
    # Extract date in DD.MM.YYYY format
    date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
    if date_match:
        date_str = date_match.group(1)
        text = text.replace(date_match.group(0), '').strip()
        return datetime.strptime(date_str, '%d.%m.%Y'), text

    # Extract date in "Month DD, YYYY" format
    date_match = re.search(r'(\b\w+\s\d{1,2},\s\d{4}\b)', text)
    if date_match:
        date_str = date_match.group(1)
        text = text.replace(date_match.group(0), '').strip()
        return datetime.strptime(date_str, '%B %d, %Y'), text

    return None, text

def format_duration(duration:int) -> str:
    try:
        total_seconds = int(duration)
        h, remainder = divmod(total_seconds, 3600)
        m, s = divmod(remainder, 60)
    
        # Format as HnnMnnSnn
        return f"H{h:02}M{m:02}S{s:02}" # formatted_time
    except Exception as e:
        logger.error(f"{e}")

def run_event_fetcher(date_stub: str):
    event_processor = EventFetcher(date_stub)
    event_processor.run()

def determine_path_and_name(info_dict: dict):
    # logger.info(f"{{video_id}info_dict.keys()}")
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
    date_obj, title = extract_date(title)

    # If date is not found in the title, try the description
    if not date_obj:
        date_obj, description = extract_date(description)

    # If date is still not found, use 'date_unknown'
    if not date_obj:
        path_date = 'date_unknown'
        filename_date = 'date_unknown'
    else:
        # Convert date to the required format
        path_date = date_obj.strftime('%Y/%m/%d')
        filename_date = date_obj.strftime('%Y.%m.%d')

        # Run the EventFetcher
        run_event_fetcher(path_date)

    # Try to extract teams from title first
    home_team, away_team = Utils.extract_teams(title)
    # logger.info(f"{video_id}home_team, away_team after extract_teams: {away_team} at {home_team}")

    # If home_team is unknown, try looking it up in e_events using the date and away team
    if home_team == 'Unknown':
        home_team = DatabaseOperations.get_e_events_team_info(date_obj, away_team, is_home_unknown=True)
    # If away_team is unknown, try looking it up in e_events using the date and home team
    elif away_team == 'Unknown':
        away_team = DatabaseOperations.get_e_events_team_info(date_obj, home_team, is_home_unknown=False)

    # Retrieve the e_event_id using the date, home team, and away team
    e_event_id = DatabaseOperations.get_e_events_event_id(date_obj, home_team, away_team)
    
    # Convert the duration to a string
    duration_string = format_duration(duration)
    
    # Construct the path and file name
    e_id = ''
    if e_event_id:
        e_id = f"{{e-{e_event_id}}}"
    path = f"./data/{path_date}/"
    file_name = f"{away_team} at {home_team} - {filename_date} - [{language}][{duration_string}][{asr}][{dynamic_range}][{acodec}][{quality}][{format_note}]{{fid-{format_id}}}{e_id}{{yt-{video_id}}}"
    return path, file_name

def progress_hook(d):
    if d['status'] == 'downloading':
        total_bytes = d.get('total_bytes', 0)
        downloaded_bytes = d.get('downloaded_bytes', 0)
        percentage = downloaded_bytes / total_bytes * 100 if total_bytes else 0
        logger.info(f"{video_name}Downloaded {downloaded_bytes} of {total_bytes} bytes ({percentage:.2f}%)")

    elif d['status'] == 'finished':
        logger.info("{video_id}Download complete.")

def postprocess_hook(d):
    try:
        if d['status'] == 'finished':
            # Access info_dict
            info_dict: dict = d.get('info_dict', {})
            existing_filepath = info_dict.get('filepath', None)
            if not existing_filepath:
                return
            
            # Define the new output path and filename
            new_path, new_filename = determine_path_and_name(info_dict)

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
                    logger.info(f"{video_name}Moved: {old_file_path} -> {new_file_path}")
                    if file_extension == '.m4a':
                        # logger.info(f"file_extension == '.m4a'")
                        video_file_info: Dict[str, Any] = {
                            'video_id': info_dict.get('id', ''),
                            'format_id': info_dict.get('format_id'),
                            'file_size': info_dict.get('filesize', ),
                            'local_path': new_file_path
                        }
                        # logger.info(f"{video_file_info}")
                        DatabaseOperations.insert_video_file(video_file_info)
        return True

    except Exception as e:
        logger.error(f"{video_name}{e}")
        return False  # Indicate failure

def download_audio(url):
    try:
        ydl_opts = {
            'outtmpl': './data/1aTemp/%(title)s.%(ext)s',  # Specify output directory and file format
            'format':
                'bestaudio[ext=m4a][acodec^=mp4a][format_note!*=DRC] \
                /bestaudio[ext=m4a][acodec^=mp4a] \
                /bestaudio[acodec^=mp4a][format_note!*=DRC] \
                /bestaudio[format_note!*=DRC]/bestaudio',
            'format_sort': ['abr'],
            # 'postprocessors': [{
            #     'key': 'FFmpegExtractAudio',
            #     'preferredcodec': 'wav',
            #     'preferredquality': '192',
            # }],
            # 'progress_hooks': [progress_hook],
            'postprocessor_hooks': [postprocess_hook],
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
        logger.error(f"{video_name}Error: {e}")

def cmd():
    fire.Fire(Fetcher)

# Run the main function
if __name__ == "__main__":
    import fire
    fire.Fire(Fetcher.fetch)