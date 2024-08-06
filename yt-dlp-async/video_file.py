from dataclasses import dataclass
from datetime import datetime
import os
import sys
import re
import shutil
import asyncio
import fire
import json
from loguru import logger
from .utils import Utils
from .e_events import EventFetcher
import yt_dlp

# Configure loguru
# Log file directory and base name
script_name = "file"
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

class Logging:
    @staticmethod
    def log_environment_info():
        logger.info("Environment Information:")
        logger.info(f"Python version: {sys.version}")

@dataclass(slots=True)
class Fetcher:
    async def fetch(anything = None, num_workers: int = 1):
        # Ensure num_workers is an integer
        if not isinstance(num_workers, int) or num_workers <=0:
            logger.error(f"num_workers must be a positive integer. The passed value was: {num_workers}")
            return
    
        # Logging.log_environment_info()

        # Example usage
        video_id = '1dy-upHOvls'
        url = f"https://www.youtube.com/watch?v={video_id}"
        # asyncio.run(save_transcripts_to_file(url, f"./data/{video_id} transcripts.json"))
        # asyncio.run(save_auto_generated_transcripts_to_file(url, f"./data/{video_id} auto gen transcripts.json"))
        # asyncio.run(get_formats(url))
        logger.info(f"url: {url}")
        await asyncio.create_task(download_audio(url))


        # # Create worker tasks
        # video_id_workers = [asyncio.create_task(worker_video_files()) for _ in range(num_workers)]

        # # Wait for all workers to finish
        # await asyncio.gather(*video_id_workers, return_exceptions=True)

        # # Wait for all the queue tasks to finish before the script ends
        # await asyncio.gather(
        #     video_file_queue.join()
        # )

# async def save_auto_generated_transcripts_to_file(video_url, filename):
#     ydl_opts = {
#         'quiet': True,  # Suppress other output
#         'skip_download': True,  # Don't download the video
#         'writeinfojson': False  # Prevent writing info.json file
#     }

#     with yt_dlp.YoutubeDL(ydl_opts) as ydl:
#         info = ydl.extract_info(video_url, download=False)
#         # subtitles = info.get('subtitles', {})
#         automatic_captions = info.get('automatic_captions', {})

#         # Prepare data to be written to file
#         data = {
#             'video_url': video_url,
#             'auto_generated_subtitles': {}
#         }

#         # for lang, subs in subtitles.items():
#         #     auto_generated = [sub for sub in subs if 'automatic' in sub['name'].lower()]
#         #     if auto_generated:
#         #         data['auto_generated_subtitles'][lang] = [sub['url'] for sub in auto_generated]

#         for lang, subs in automatic_captions.items():
#                 data['auto_generated_subtitles'][lang]

#         # Write data to file
#         with open(filename, 'w') as file:
#             json.dump(data, file, indent=4)


# async def save_transcripts_to_file(video_url, filename):
#     ydl_opts = {
#         'quiet': True,  # Suppress other output
#         'skip_download': True,  # Don't download the video
#     }

#     with yt_dlp.YoutubeDL(ydl_opts) as ydl:
#         info = ydl.extract_info(video_url, download=False)
#         subtitles = info.get('subtitles', {})

#         # Prepare data to be written to file
#         data = {
#             'video_url': video_url,
#             'subtitles': {}
#         }
        
#         for lang, subs in subtitles.items():
#             data['subtitles'][lang] = []
#             for sub in subs:
#                 data['subtitles'][lang].append(sub['url'])

#         # Write data to file
#         with open(filename, 'w') as file:
#             json.dump(data, file, indent=4)

# Function to get available formats

async def get_formats(url: str):
    try:
        ydl_opts = {
            'listformats': True,
        }

        # Define a custom hook to capture the output
        class MyLogger:
            def __init__(self):
                self.formats = []

            def debug(self, msg):
                if msg.startswith('[info] Available formats for'):
                    self.formats.append(msg)

            def warning(self, msg):
                pass

            def error(self, msg):
                print(msg)

        logger = MyLogger()
        ydl_opts['logger'] = logger

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            await asyncio.to_thread(ydl.download, [url])

        # Print the captured formats
        for format in logger.formats:
            print(format)

    except Exception as e:
        print(f"Error: {e}")

async def extract_date(text: str):
    # Extract the date from text in DD.MM.YYYY format.
    date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
    if date_match:
        date_str = date_match.group(1)
        text = text.replace(date_match.group(0), '').strip()  # Remove date from the text
        return datetime.strptime(date_str, '%d.%m.%Y'), text
    else:
        return None, text

async def format_duration(duration:int) -> str:
    try:
        total_seconds = int(duration)
        h, remainder = divmod(total_seconds, 3600)
        m, s = divmod(remainder, 60)
    
        # Format as HnnMnnSnn
        return f"H{h:02}M{m:02}S{s:02}" # formatted_time
    except Exception as e:
        logger.error(f"{e}")

async def run_event_fetcher(date_stub: str):
    event_processor = EventFetcher(date_stub)
    await event_processor.run()

async def determine_path_and_name(info_dict: dict):
    # logger.info(f"{info_dict.keys()}")
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
    date_obj, title = await extract_date(title)

    # If date is not found in the title, try the description
    if not date_obj:
        date_obj, description = await extract_date(description)

    # If date is still not found, use 'date_unknown'
    if not date_obj:
        path_date = 'date_unknown'
        filename_date = 'date_unknown'
    else:
        # Convert date to the required format
        path_date = date_obj.strftime('%Y/%m/%d')
        filename_date = date_obj.strftime('%Y.%m.%d')

        # Run the EventFetcher asynchronously
        await run_event_fetcher(path_date)

    # Try to extract teams from title first
    teams_str, title = Utils.extract_teams(title)

    # If neither team is not found in the title, try the description
    if teams_str == 'Unknown at Unknown':
        teams_str, description = Utils.extract_teams(description)

    # Convert the duration to a string
    duration_string = await format_duration(duration)
    
    # Construct the path and file name
    path = f"../data/{path_date}/"
    file_name = f"{teams_str} - {filename_date} - [{language}][{duration_string}][{asr}][{dynamic_range}][{acodec}][{quality}][{format_note}]{{fid-{format_id}}}{{yt-{video_id}}}"
    return path, file_name

def progress_hook(d):
    if d['status'] == 'downloading':
        total_bytes = d.get('total_bytes', 0)
        downloaded_bytes = d.get('downloaded_bytes', 0)
        percentage = downloaded_bytes / total_bytes * 100 if total_bytes else 0
        print(f"Downloaded {downloaded_bytes} of {total_bytes} bytes ({percentage:.2f}%)")

    elif d['status'] == 'finished':
        print("Download complete.")

async def async_postprocess_hook(d):
    try:
        if d['status'] == 'finished':
            # Access info_dict
            info_dict: dict = d.get('info_dict', {})
            existing_filepath = info_dict.get('filepath', None)
            if not existing_filepath:
                return
            

            # Define the new output path and filename
            new_path, new_filename = await determine_path_and_name(info_dict)

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
                    print(f"Moved: {old_file_path} -> {new_file_path}")

    except Exception as e:
        logger.error(f"{e}")

def postprocess_hook_wrapper(d):
    asyncio.create_task(async_postprocess_hook(d))

async def download_audio(url):
    try:
        ydl_opts = {
            'outtmpl': '../data/1aTemp/%(title)s.%(ext)s',  # Specify output directory and file format
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
            'progress_hooks': [progress_hook],
            'postprocessor_hooks': [postprocess_hook_wrapper],
            'writeinfojson': True,  # Save metadata to a JSON file
            'writeautomaticsub': True,
            'subtitleslangs': ['en','en-orig'],
            'subtitlesformat': 'ttml',
            'embed_chapters': False,
            'add_metadata': False,
            'skip_download': False,  # Don't download the video
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

    except Exception as e:
        print(f"Error: {e}")

def cmd():
    fire.Fire(Fetcher)
