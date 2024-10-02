# Documentation for `video_download.py`

## Module Overview

The `video_download.py` module is designed to download videos from YouTube and process their audio files. It uses the `yt-dlp` library to handle the downloading and supports post-processing steps such as moving files and updating the database with video file information.

### Key Components

- **YouTube Downloading**: Utilizes `yt-dlp` to download video audio streams from YouTube.
- **Audio Processing**: Handles post-processing of downloaded audio files, including renaming and moving to appropriate directories.
- **Metadata Extraction**: Extracts metadata from video titles and descriptions to determine file paths and names.
- **Database Integration**: Updates the database with information about the downloaded audio files.
- **Modularity**: The modules are designed to be modular and extensible, allowing for easy integration and future enhancements.
- **Logging**: Uses `loguru` for logging, providing detailed information during the download and post-processing phases.

---

## Classes and Methods

### `Fetcher`

Downloads videos from YouTube and processes their audio files.

#### `__init__(self)`

Initializes the `Fetcher` with default settings.

---

#### `fetch(self, video_id: str) -> None`

Fetches and processes the video by downloading its audio.

- **Args**:
  - `video_id` (`str`): The YouTube video ID to fetch.

---

#### `download_audio(self) -> None`

Downloads the audio from the video URL.

- **Uses**:
  - The video URL stored in `self.video_url`.

- **Raises**:
  - `Exception`: If an error occurs during the download process.

---

#### `progress_hook(self, d: dict) -> None`

Callback function called during the download process to report progress.

- **Args**:
  - `d` (`dict`): A dictionary containing information about the download progress.

---

#### `postprocess_hook(self, d: dict) -> bool`

Post-processes the downloaded video file after download completion.

- **Args**:
  - `d` (`dict`): Dictionary containing download information.

- **Returns**:
  - `bool`: `True` if post-processing is successful, `False` otherwise.

---

#### `determine_path_and_name(self, info_dict: dict) -> tuple`

Determines the file path and name for the video based on provided metadata.

- **Args**:
  - `info_dict` (`dict`): Dictionary containing information about the video.

- **Returns**:
  - `tuple`: A tuple `(path, file_name)` for storing the video.

---

#### `format_duration(self, duration: str) -> str`

Formats the duration string into a human-readable format.

- **Args**:
  - `duration` (`str`): The duration string in ISO 8601 format.

- **Returns**:
  - `str`: The formatted duration as `'XH YM ZS'`.

---

### Function: `cmd() -> None`

Provides a command-line interface for running the `Fetcher`.

---

## Example Usage

### Prerequisites

- **Python Environment**: Ensure Python 3.7 or higher is installed.
- **Dependencies**: Install required packages:
  ```bash
  pip install yt-dlp fire loguru
  ```
- **yt-dlp**: Make sure `yt-dlp` is installed and accessible.

### Running the Module

Use the command-line interface to download and process a YouTube video.

```bash
python video_download.py fetch --video_id "dQw4w9WgXcQ"
```

- **Parameters**:
  - `--video_id`: The YouTube video ID to download and process.

---

## Understanding the Workflow

1. **Initialization**:
   - The `Fetcher` is initialized with default settings.

2. **Fetching Video**:
   - The `fetch` method is called with a specific `video_id`.
   - Constructs the video URL and sets up logging.

3. **Downloading Audio**:
   - `download_audio` sets up `yt-dlp` options and initiates the download.
   - Includes progress and post-processing hooks.

4. **Progress Reporting**:
   - `progress_hook` provides real-time updates during the download.

5. **Post-Processing**:
   - `postprocess_hook` moves files to appropriate directories.
   - Updates the database with file information.

6. **File Naming and Path Determination**:
   - `determine_path_and_name` uses video metadata to decide where to store the file and what to name it.

7. **Duration Formatting**:
   - `format_duration` converts ISO 8601 duration strings into human-readable formats.

---

## Additional Notes

- **Error Handling**: Exceptions are logged, and the process continues gracefully.
- **Metadata Extraction**: The module attempts to extract as much metadata as possible to organize files effectively.
- **Database Operations**: Assumes `DatabaseOperations` is configured for database interactions.
- **Logging**: Detailed logs are generated for debugging and monitoring purposes.

---

## Configuration Constants

- **`BASE_URL`**: Base URL for YouTube videos (`"https://www.youtube.com/watch?v="`).
- **`OUTPUT_DIR`**: Directory where downloaded files will be stored (default from environment variable or `'/yt_dlp_data/'`).
- **`SUBTITLES_LANGS`**: Languages for subtitles to download (e.g., `['en', 'en-orig']`).
- **`SUBTITLES_FORMAT`**: Format for subtitles (e.g., `'ttml'`).

---

## Conclusion

The `video_download.py` module provides a robust solution for downloading and processing YouTube videos. By leveraging `yt-dlp` and asynchronous processing, it efficiently handles downloads, metadata extraction, and file organization. The module is suitable for integration into larger applications or for use as a standalone tool.
