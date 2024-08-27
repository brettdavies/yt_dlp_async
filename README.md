# yt_dlp_async
`yt_dlp_async` is an asynchronous Python application designed to fetch and process YouTube video and playlist IDs using `yt-dlp` and store them in a PostgreSQL database. The application supports parallel processing of user IDs, playlist IDs, and video IDs through a set of asynchronous worker tasks.

## Features

- **Asynchronous Processing**: Utilizes the power of `asyncio` to handle multiple tasks concurrently, improving efficiency and performance.
- **Parallel Processing with Workers**: Introduces the concept of workers to enable parallel processing of user IDs, playlist IDs, and video IDs. This allows for even faster data retrieval and processing.
- **Environment Configuration**: Loads configuration settings from a `.env` file, making it easy to customize the application for different environments.
- **Database Operations**: Manages database connections and performs batch inserts using `psycopg2`, ensuring efficient and reliable data storage.
- **Command Line Interface (CLI)**: Provides a user-friendly CLI interface with commands to execute various functionalities.

With the addition of workers, `yt_dlp_async` can now handle multiple tasks simultaneously, significantly improving the overall performance and reducing the time required for fetching and processing YouTube IDs.

Each worker operates independently, allowing for efficient utilization of system resources and maximizing throughput. This feature is particularly useful when dealing with large datasets or when time-sensitive operations are required.

To configure the number of workers, simply adjust the corresponding setting in the `.env` file. By fine-tuning the number of workers based on your system's capabilities, you can achieve optimal performance for your specific use case.

With the enhanced parallel processing capabilities provided by workers, `yt_dlp_async` offers a powerful solution for efficiently fetching and processing YouTube video and playlist IDs, making it an ideal choice for applications that require high-performance data retrieval and storage.

## Prerequisites

Before using `yt_dlp_async`, ensure that you have the following prerequisites installed:

- Python 3.12 or higher
- Poetry
- `yt-dlp`
- Access to a PostgreSQL database

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

4. **Prepare Database**

   Execute `db_schema.sql` to create the necessary tables and functions.

## Usage

### CLI Commands

`yt_dlp_async` provides several CLI commands for fetching and processing YouTube IDs. The commands are accessible through the `fire` library.

```bash
poetry run get-video-id
poetry run get-video-metadata
poetry run get-video-file
```

Each command should be defined in the respective module and can be executed with appropriate arguments.

### Configuration

The application uses the `.env` file for configuration. Make sure to specify your `DATABASE_URL` to connect to your PostgreSQL database.

### Example Usage

#### run get-video-id

To start fetching video IDs, you can use the following command:

```bash
poetry run get-video-id --video_ids="id1,id2" --video_id_files="file1.txt" --playlist_ids="playlist1,playlist2" --playlist_id_files="file2.csv" --user_ids="user1,user2" --user_id_files="file3.txt" --workers=4
```

This command allows you to fetch video IDs from various sources. Here's a breakdown of the available options:

- `--video_ids`: Specify a comma-separated list of video IDs to fetch.
- `--video_id_files`: Specify one or more text files containing video IDs, with each ID on a separate line.
- `--playlist_ids`: Specify a comma-separated list of playlist IDs to fetch.
- `--playlist_id_files`: Specify one or more CSV files containing playlist IDs, with each ID in a separate row under the "playlist_id" column.
- `--user_ids`: Specify a comma-separated list of user IDs to fetch.
- `--user_id_files`: Specify one or more text files containing user IDs, with each ID on a separate line.
- `--workers`: Specify the number of workers to use for parallel processing. Default is 1.

You can mix and match these options to fetch video IDs from different sources simultaneously. For example, you can provide a combination of video IDs, playlist IDs, and user IDs in a single command.

Make sure to replace `id1`, `id2`, `file1.txt`, `playlist1`, `playlist2`, `file2.csv`, `user1`, `user2`, and `file3.txt` with the actual IDs and file names you want to use.

By using these options, you can easily customize the fetching process and retrieve the desired video IDs for further processing or analysis.

#### run get-video-metadata

To retrieve metadata for YouTube videos, you can use the following command:

```bash
poetry run get-video-metadata --video_ids="id1,id2" --video_id_files="file1.txt" --workers=4
```

This command allows you to fetch metadata for YouTube videos from various sources. Here's a breakdown of the available options:

- `--video_ids`: Specify a comma-separated list of video IDs for which you want to retrieve metadata.
- `--video_id_files`: Specify one or more text files containing video IDs, with each ID on a separate line.
- `--workers`: Specify the number of workers to use for parallel processing. Default is 1.

You can mix and match these options to retrieve metadata for videos from different sources simultaneously. For example, you can provide a combination of video IDs from a list and a text file in a single command.

Make sure to replace `id1`, `id2`, and `file1.txt` with the actual video IDs and file names you want to use.

By using these options, you can easily customize the metadata retrieval process and obtain the desired information about YouTube videos.

#### run get-video-file

To download YouTube videos, you can use the following command:

```bash
poetry run get-video-file --video_ids="id1,id2" --video_id_files="file1.txt" --output_dir="output" --workers=4
```

This command allows you to download YouTube videos from various sources. Here's a breakdown of the available options:

- `--video_ids`: Specify a comma-separated list of video IDs for which you want to download the videos.
- `--video_id_files`: Specify one or more text files containing video IDs, with each ID on a separate line.
- `--output_dir`: Specify the directory where the downloaded videos will be saved. Default is the current directory.
- `--workers`: Specify the number of workers to use for parallel processing. Default is 1.

You can mix and match these options to download videos from different sources simultaneously. For example, you can provide a combination of video IDs from a list and a text file in a single command.

Make sure to replace `id1`, `id2`, `file1.txt`, and `output` with the actual video IDs, file names, and output directory you want to use.

By using these options, you can easily customize the video downloading process and save the YouTube videos to the desired location on your system.

## Development

To contribute to `yt_dlp_async`, follow these steps:

1. **Install Development Dependencies**:

   ```bash
   poetry install --dev
   ```

2. **Run Tests**:

   If tests are defined, you can run them using:

   ```bash
   poetry run pytest
   ```

3. **Build the Project**:

   To build the project, use:

   ```bash
   poetry build
   ```

4. **Format Code**:

   Ensure code consistency by formatting:

   ```bash
   poetry run black yt_dlp_async
   ```

5. **Make Changes and Commit**:

   ```bash
   git add .
   git commit -m "Add your feature"
   ```

6. **Push and Create a Pull Request**:

   ```bash
   git push origin feature/your-feature
   ```

   Then, create a pull request on GitHub.

## License

This project is licensed under the BSD 3-Clause License. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [yt-dlp](https://github.com/yt-dlp/yt-dlp) for the video extraction tool.
- [Poetry](https://python-poetry.org/) for dependency management and packaging.
- [Loguru](https://github.com/Delgan/loguru) for advanced logging.

---
