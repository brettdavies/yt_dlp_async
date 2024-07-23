# yt-dlp-async

`yt-dlp-async` is an asynchronous Python application designed to fetch and process YouTube video and playlist IDs using `yt-dlp` and store them in a PostgreSQL database. The application supports parallel processing of user IDs, playlist IDs, and video IDs through a set of asynchronous worker tasks.

## Features

- **Asynchronous Processing**: Uses `asyncio` to handle multiple tasks concurrently, improving efficiency and performance.
- **Environment Configuration**: Loads configuration settings from a `.env` file.
- **Database Operations**: Manages database connections and performs batch inserts using `psycopg2`.
- **Command Line Interface (CLI)**: Provides CLI commands to execute various functionalities.

## Prerequisites

- Python 3.12 or higher
- PostgreSQL database
- `yt-dlp` installed

## Installation

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/yourusername/yt-dlp-async.git
   cd yt-dlp-async
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

## Usage

### CLI Commands

`yt-dlp-async` provides several CLI commands for fetching and processing YouTube IDs. The commands are accessible through the `fire` library.

```bash
poetry run get-video-id
poetry run get-video-metadata
poetry run get-video-file
```

Each command should be defined in the respective module and can be executed with appropriate arguments.

### Example Usage

To start fetching video IDs, you can use the following command:

```bash
poetry run get-video-id --video_ids="id1,id2" --video_id_files="file1.txt" --playlist_ids="playlist1,playlist2" --playlist_id_files="file2.csv" --user_ids="user1,user2" --user_id_files="file3.txt"
```

### Configuration

The application uses the `.env` file for configuration. Make sure to specify your `DATABASE_URL` to connect to your PostgreSQL database.

### Project Structure

The project structure is as follows:

```
/yt-dlp-async/
├── pyproject.toml
├── .env
└── yt-dlp-async/
    ├── videoId.py
    ├── videoMetadata.py
    └── videoFile.py
```

- `pyproject.toml`: Configuration file for Poetry.
- `.env`: Environment variables configuration.
- `yt-dlp-async/`: Source code directory.

## Development

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
   poetry run black yt-dlp-async
   ```

## Contributing

Contributions are welcome! Please follow these steps:

1. **Fork the Repository**: Create a fork of the repository on GitHub.
2. **Clone Your Fork**:

   ```bash
   git clone https://github.com/yourusername/yt-dlp-async.git
   ```

3. **Create a Branch**:

   ```bash
   git checkout -b feature/your-feature
   ```

4. **Make Changes and Commit**:

   ```bash
   git add .
   git commit -m "Add your feature"
   ```

5. **Push and Create a Pull Request**:

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

Feel free to adjust the sections as necessary to fit the exact functionality and structure of your project.