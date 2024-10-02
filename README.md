# yt_dlp_async

`yt_dlp_async` is an asynchronous Python application designed to fetch and process YouTube video and playlist IDs using `yt-dlp` and store them in a PostgreSQL database. The application supports parallel processing of user IDs, playlist IDs, and video IDs through a set of asynchronous worker tasks.

## Features

- **Asynchronous Processing**: Utilizes the power of `asyncio` to handle multiple tasks concurrently, improving efficiency and performance.
- **Parallel Processing with Workers**: Introduces the concept of workers to enable parallel processing of user IDs, playlist IDs, and video IDs. This allows for even faster data retrieval and processing.
- **Environment Configuration**: Loads configuration settings from a `.env` file, making it easy to customize the application for different environments.
- **Database Operations**: Manages database connections and performs batch inserts using `psycopg2`, ensuring efficient and reliable data storage.
- **Command-Line Interface (CLI)**: Provides a user-friendly CLI interface with commands to execute various functionalities.
- **Detailed Module Documentation**: Comprehensive documentation is available for each module, facilitating easier understanding and maintenance.

With the addition of workers, `yt_dlp_async` can now handle multiple tasks simultaneously, significantly improving the overall performance and reducing the time required for fetching and processing YouTube IDs.

Each worker operates independently, allowing for efficient utilization of system resources and maximizing throughput. This feature is particularly useful when dealing with large datasets or when time-sensitive operations are required.

To configure the number of workers, simply adjust the corresponding setting in the `.env` file or use command-line arguments. By fine-tuning the number of workers based on your system's capabilities, you can achieve optimal performance for your specific use case.

## Prerequisites

Before using `yt_dlp_async`, ensure that you have the following prerequisites installed:

- **Python 3.12 or higher**
- **Poetry**
- **yt-dlp**
- **Access to a PostgreSQL database**

## Installation

To install `yt_dlp_async`, follow these steps:

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/yourusername/yt_dlp_async.git
   cd yt_dlp_async
   ```

2. **Install Dependencies**:

   Use Poetry to handle dependencies:

   ```bash
   poetry install
   ```

3. **Create and Configure the `.env` File**:

   Create a `.env` file in the root directory with the following content:

   ```ini
   DATABASE_URL=postgresql://admin:admin123@localhost:5432/video_data
   ```

   Adjust the `DATABASE_URL` to match your PostgreSQL setup.

4. **Prepare the Database**

   Execute `db_schema.sql` to create the necessary tables and functions.

## Documentation

Detailed documentation for each module is available in the `docs` directory:

- [video_id.py](docs/video_id.md): Fetching video IDs from YouTube.

- [video_metadata.py](docs/video_metadata.md): Retrieves and processes metadata for YouTube videos.
    - **Requires a Google API key with access to the YouTube Data API v3.** You can obtain an API key and ensure appropriate access by following [Google's official instructions](https://developers.google.com/youtube/v3/getting-started#before-you-start).

- [video_file.py](docs/video_file.md): Handles downloading of video files from YouTube.

- [video_download.py](docs/video_download.md): Manages the processing and organization of downloaded videos.

- [utils.py](docs/utils.md): Provides utility functions for URL preparation, file reading, and data extraction.

- [e_events.py](docs/e_events.md): Fetches and processes event data from the ESPN API.

- [metadata.py](docs/metadata.md): Contains mappings for standardizing team names and abbreviations.

- [db_schema.sql](docs/db_schema.md): Defines the database schema for storing YouTube and ESPN data.

## Usage

### CLI Commands

`yt_dlp_async` provides several CLI commands for fetching and processing YouTube IDs. The commands are accessible through the `fire` library.

- **Fetch Video IDs**:

  ```bash
  poetry run get-video-id fetch [OPTIONS]
  ```

- **Fetch Video Metadata**:

  ```bash
  poetry run get-video-metadata fetch [OPTIONS]
  ```

- **Download Video Files**:

  ```bash
  poetry run get-video-file fetch [OPTIONS]
  ```

### Configuration

The application uses the `.env` file for configuration. Make sure to specify your `DATABASE_URL` to connect to your PostgreSQL database.

### Example Usage

#### Fetching Video IDs

To start fetching video IDs, you can use the following command:

```bash
poetry run get-video-id fetch \
  --video_ids "dQw4w9WgXcQ,9bZkp7q19f0" \
  --video_id_files "video_ids.txt" \
  --playlist_ids "PLMC9KNkIncKtPzgY-5rmhvj7fax8fdxoj" \
  --playlist_id_files "playlist_ids.csv" \
  --user_ids "UC_x5XG1OV2P6uZZ5FSM9Ttw" \
  --user_id_files "user_ids.txt" \
  --num_workers 4
```

- **Options**:
  - `--video_ids`: Comma-separated list of video IDs to fetch.
  - `--video_id_files`: One or more text files containing video IDs (one per line).
  - `--playlist_ids`: Comma-separated list of playlist IDs to fetch.
  - `--playlist_id_files`: One or more CSV files containing playlist IDs.
  - `--user_ids`: Comma-separated list of user or channel IDs to fetch.
  - `--user_id_files`: One or more text files containing user IDs (one per line).
  - `--num_workers`: Number of workers to use for parallel processing (default is 1).

You can mix and match these options to fetch video IDs from different sources simultaneously. For example, you can provide a combination of video IDs, playlist IDs, and user IDs in a single command.

For detailed usage and examples, refer to the [video_id.py documentation](docs/video_id.md).

#### Fetching Video Metadata

To retrieve metadata for YouTube videos, you can use the following command:

```bash
poetry run get-video-metadata fetch \
  --num_workers 4
```

This command will process video IDs from the database that do not yet have metadata.

For more information, see the [video_metadata.py documentation](docs/video_metadata.md).

#### Downloading Video Files

To download YouTube videos, you can use the following command:

```bash
poetry run get-video-file fetch \
  --existing_videos_dir "/path/to/existing_videos" \
  --num_workers 4
```

- **Options**:
  - `--existing_videos_dir`: Path to the directory containing existing video files.
  - `--num_workers`: Number of workers to use for parallel processing (default is 10).

This command will download video files for video IDs that are in the database but do not have associated files yet.

For additional details, refer to the [video_file.py documentation](docs/video_file.md).

## License

This project is licensed under the BSD 3-Clause License. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [yt-dlp](https://github.com/yt-dlp/yt-dlp) for the video extraction tool.
- [Poetry](https://python-poetry.org/) for dependency management and packaging.
- [Loguru](https://github.com/Delgan/loguru) for advanced logging.

---

By utilizing the comprehensive documentation and modular design, `yt_dlp_async` provides a robust and efficient solution for fetching, processing, and storing YouTube data. For more detailed information on each module and its functionalities, please refer to the documentation linked above.