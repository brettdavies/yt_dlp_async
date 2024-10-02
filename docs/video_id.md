# `video_id.py`

## Design Considerations and Enhancements

- **Asynchronous Processing**: Utilizes `asyncio` to handle asynchronous I/O operations, enabling efficient fetching of IDs from YouTube.

- **Concurrent Worker Tasks**: Implements concurrent worker tasks and queues for parallel processing of user IDs, playlist IDs, and video IDs, improving performance and scalability.

- **Modularity and Extensibility**: Organized into classes and functions, making it easy to extend and maintain.

- **External Libraries**: Employs `yt-dlp` for interacting with YouTube and `fire` for command-line interface.

- **Database Integration**: Interacts with `DatabaseOperations` to insert video IDs into a database for downstream processing.

- **Logging and Error Handling**: Uses `loguru` for comprehensive logging and includes robust error handling to ensure reliable operation during network and subprocess execution.

## Module Overview

The `video_id.py` module is designed to fetch video IDs, playlist IDs, and user IDs from YouTube using the `yt-dlp` library. It processes inputs from command-line arguments or files, retrieves associated video IDs, and stores them in a database. The module leverages asynchronous programming and queues to manage and process IDs concurrently, making it suitable for handling large datasets efficiently.

### Key Libraries and Dependencies

- **`asyncio`**: Asynchronous I/O framework used for managing asynchronous tasks and queues.
  - [Asyncio Documentation](https://docs.python.org/3/library/asyncio.html)

- **`yt-dlp`**: A command-line program to download videos from YouTube and other sites, used here to fetch IDs.
  - [yt-dlp Documentation](https://github.com/yt-dlp/yt-dlp)

- **`fire`**: A library for automatically generating command-line interfaces.
  - [Fire Documentation](https://github.com/google/python-fire)

- **`loguru`**: A modern logging library that simplifies logging in Python.
  - [Loguru Documentation](https://loguru.readthedocs.io/)

- **`dataclasses`**: A module that provides a decorator and functions for automatically adding special methods to classes.
  - [Dataclasses Documentation](https://docs.python.org/3/library/dataclasses.html)

- **`subprocess`**: Module for spawning new processes and connecting to their input/output/error pipes.
  - [Subprocess Documentation](https://docs.python.org/3/library/subprocess.html)

- **First Party Libraries**:
  - **`Utils`**: Contains utility functions for URL preparation and reading IDs from files.
  - **`DatabaseOperations`**: Manages database operations for inserting and retrieving video IDs.
  - **`LoggerConfig`**: Configures logging settings using `loguru`.

---

## Classes and Methods

### `QueueManager`

The `QueueManager` class manages queues for user IDs, playlist IDs, and video IDs, along with tracking the number of active tasks for each ID type.

---

### `VideoIdOperations`

The `VideoIdOperations` class provides static methods for running `yt-dlp` commands to fetch video and playlist IDs from YouTube.

#### Key Methods

##### Method: `_run_yt_dlp_command(cmd: List[str]) -> List[str]`

Runs a `yt-dlp` command asynchronously and returns the output as a list of strings.

```python
@staticmethod
async def _run_yt_dlp_command(cmd: List[str]) -> List[str]:
    """
    Runs a yt-dlp command and returns the output as a list of strings.

    Args:
        cmd (List[str]): The yt-dlp command to run.

    Returns:
        List[str]: The output of the command.
    """
```

---

##### Method: `fetch_video_ids_from_url(url: str) -> List[str]`

Fetches video IDs from the given URL using `yt-dlp`.

```python
@staticmethod
async def fetch_video_ids_from_url(url: str) -> List[str]:
    """
    Fetches the video IDs from the given URL.

    Args:
        url (str): The URL of the channel or playlist.

    Returns:
        List[str]: A list of video IDs.
    """
```

---

##### Method: `fetch_playlist_ids_from_user_id(user_url: str) -> List[str]`

Fetches playlist IDs associated with a user's channel URL.

```python
@staticmethod
async def fetch_playlist_ids_from_user_id(user_url: str) -> List[str]:
    """
    Fetches the playlist IDs from the given user URL.

    Args:
        user_url (str): The URL of the user's channel.

    Returns:
        List[str]: A list of playlist IDs.
    """
```

---

### `Fetcher`

The `Fetcher` class coordinates the fetching and processing of video IDs, playlist IDs, and user IDs. It initializes the queues and starts the worker tasks.

---

### Key Methods

##### Method: `fetch(self, ...) -> None`

Fetches IDs from various sources and starts the worker tasks.

```python
async def fetch(
    self,
    video_ids: Optional[List[str]] = None,
    video_id_files: Optional[List[str]] = None,
    playlist_ids: Optional[List[str]] = None,
    playlist_id_files: Optional[List[str]] = None,
    user_ids: Optional[List[str]] = None,
    user_id_files: Optional[List[str]] = None,
    num_workers: int = 5,
) -> None:
    """
    Fetches video IDs, playlist IDs, and user IDs from various sources and starts worker tasks.

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
```

---

##### Method: `add_ids_to_queue(ids, id_files, queue_manager, read_func) -> None`

Adds IDs from lists and files to the specified queue.

```python
async def add_ids_to_queue(ids: Optional[List[str]], id_files: Optional[List[str]], queue_manager: QueueManager, read_func) -> None:
    """
    Adds IDs from lists and files to the specified queue.

    Args:
        ids (Optional[List[str]]): A list of IDs.
        id_files (Optional[List[str]]): A list of file paths containing IDs.
        queue_manager (QueueManager): The queue to add IDs to.
        read_func (Callable): Function to read IDs from files.

    Returns:
        None
    """
```

---

### Worker Functions

These asynchronous functions are used as worker tasks to process IDs from the queues concurrently.

#### Function: `worker_user_ids(queue_manager: QueueManager) -> None`

Processes user IDs by fetching associated playlist and video IDs and adding them to their respective queues.

```python
async def worker_user_ids(queue_manager: QueueManager):
    """
    Worker task for processing user IDs.

    Args:
        queue_manager (QueueManager): Manages the queues and active tasks.

    Returns:
        None
    """
```

---

#### Function: `worker_playlist_ids(queue_manager: QueueManager) -> None`

Processes playlist IDs by fetching associated video IDs and adding them to the video ID queue.

```python
async def worker_playlist_ids(queue_manager: QueueManager):
    """
    Worker task for processing playlist IDs.

    Args:
        queue_manager (QueueManager): Manages the queues and active tasks.

    Returns:
        None
    """
```

---

#### Function: `worker_video_ids(queue_manager: QueueManager) -> None`

Processes video IDs by inserting them into the database.

```python
async def worker_video_ids(queue_manager: QueueManager):
    """
    Worker task for processing video IDs.

    Args:
        queue_manager (QueueManager): Manages the queues and active tasks.

    Returns:
        None
    """
```

---

### Function: `cmd() -> None`

Provides a command-line interface for running the `Fetcher` class.

```python
def cmd() -> None:
    """
    Command-line interface for running the Fetcher.

    Returns:
        None
    """
```

---

## Example Usage

### Prerequisites

- **Python Environment**: Ensure you have Python 3.7 or higher installed.
- **Dependencies**: Install the required packages by running:

  ```bash
  pip install yt-dlp fire loguru
  ```

- **yt-dlp**: The module uses `yt-dlp` to interact with YouTube, so it must be installed and accessible in your environment.

### Running the Module

The `video_id.py` module provides a command-line interface through the `cmd()` function. You can use this interface to fetch video IDs from various sources and insert them into the database for further processing.

#### Command-Line Interface

To use the module from the command line, navigate to the directory containing `video_id.py` and run it using Python. Below are examples of how to use it.

---

### Fetching Video IDs from a List of Video URLs

Suppose you have a list of video URLs or IDs that you want to process.

```bash
python video_id.py fetch --video_ids "dQw4w9WgXcQ,9bZkp7q19f0"
```

- **Parameters**:
  - `--video_ids`: A comma-separated list of YouTube video IDs.

**Explanation**:

- The `fetch` command invokes the `fetch` method of the `Fetcher` class.
- The `--video_ids` argument accepts a list of video IDs.

---

### Fetching Video IDs from a File

If you have a text file containing video IDs (one per line), you can specify the file path.

```bash
python video_id.py fetch --video_id_files "/path/to/video_ids.txt"
```

- **Parameters**:
  - `--video_id_files`: A comma-separated list of file paths containing video IDs.

**Explanation**:

- The `fetch` method reads video IDs from the specified file(s) and processes them.

---

### Fetching Video IDs from a Playlist

To fetch all video IDs from a specific YouTube playlist:

```bash
python video_id.py fetch --playlist_ids "PLMC9KNkIncKtPzgY-5rmhvj7fax8fdxoj"
```

- **Parameters**:
  - `--playlist_ids`: A comma-separated list of YouTube playlist IDs.

**Explanation**:

- The module will fetch all video IDs from the specified playlist(s) and insert them into the database.

---

### Fetching Video IDs from a User's Channel

To fetch all video IDs from a user's channel:

```bash
python video_id.py fetch --user_ids "UC_x5XG1OV2P6uZZ5FSM9Ttw"
```

- **Parameters**:
  - `--user_ids`: A comma-separated list of YouTube user IDs or channel IDs.

**Explanation**:

- The module will fetch all playlist IDs associated with the user and then fetch all video IDs from those playlists.
- It will also fetch any videos uploaded directly to the user's channel.

---

### Fetching IDs from Multiple Sources

You can combine different parameters to fetch IDs from multiple sources simultaneously.

```bash
python video_id.py fetch \
  --video_ids "dQw4w9WgXcQ" \
  --video_id_files "/path/to/more_video_ids.txt" \
  --playlist_ids "PLMC9KNkIncKtPzgY-5rmhvj7fax8fdxoj" \
  --user_ids "UC_x5XG1OV2P6uZZ5FSM9Ttw" \
  --num_workers 10
```

- **Parameters**:
  - `--video_ids`: List of video IDs.
  - `--video_id_files`: List of files containing video IDs.
  - `--playlist_ids`: List of playlist IDs.
  - `--user_ids`: List of user or channel IDs.
  - `--num_workers`: Number of concurrent worker tasks to use (default is 5).

**Explanation**:

- This command fetches video IDs from various sources using 10 worker tasks for improved performance.

---

### Sample Code for Invoking the Fetcher Programmatically

If you prefer to use the module within another Python script, you can invoke the `Fetcher` class directly.

```python
import asyncio
from video_id import Fetcher

async def main():
    fetcher = Fetcher()
    await fetcher.fetch(
        video_ids=["dQw4w9WgXcQ"],
        video_id_files=["/path/to/video_ids.txt"],
        playlist_ids=["PLMC9KNkIncKtPzgY-5rmhvj7fax8fdxoj"],
        user_ids=["UC_x5XG1OV2P6uZZ5FSM9Ttw"],
        num_workers=5,
    )

if __name__ == "__main__":
    asyncio.run(main())
```

**Explanation**:

- This script creates an instance of `Fetcher` and calls the `fetch` method with desired parameters.
- It uses `asyncio.run` to execute the asynchronous `fetch` method.

---

## Additional Notes

- **Database Operations**: The module assumes that `DatabaseOperations` is properly configured to handle database interactions, including methods like `insert_video_ids` and `get_count_videos_to_be_processed`.
  
- **Logging**: The module uses `loguru` for logging. Logs will be output to the console and can be configured via the `LoggerConfig` class.
  
- **Error Handling**: The module includes robust error handling to manage network issues or subprocess errors when invoking `yt-dlp`.

- **Concurrency**: Adjust the `--num_workers` parameter according to your system's capabilities to optimize performance.

---

## Understanding the Workflow

1. **Initialization**:
   - The `Fetcher` class initializes a `QueueManager` instance to manage the queues for user IDs, playlist IDs, and video IDs.

2. **Adding IDs to Queues**:
   - IDs provided via command-line arguments or files are added to their respective queues.

3. **Worker Tasks**:
   - **User ID Workers**:
     - Fetch playlist IDs and video IDs associated with the user ID.
     - Add fetched playlist IDs to the playlist ID queue.
     - Add fetched video IDs directly to the video ID queue.
   - **Playlist ID Workers**:
     - Fetch video IDs from the playlist ID.
     - Add fetched video IDs to the video ID queue.
   - **Video ID Workers**:
     - Retrieve video IDs from the video ID queue.
     - Insert video IDs into the database using `DatabaseOperations`.

4. **Asynchronous Processing**:
   - All workers run asynchronously, enabling concurrent processing and efficient utilization of resources.

5. **Database Insertion**:
   - Video IDs collected from various sources are inserted into the database for downstream tasks, such as downloading or analysis.

---

## Conclusion

The `video_id.py` module provides a flexible and efficient way to collect video IDs from YouTube channels, playlists, and individual videos. By utilizing asynchronous programming and concurrent workers, it can handle large volumes of data effectively. The module is designed to be both a standalone command-line tool and a module that can be integrated into other Python applications.

Adjust the parameters and extend the functionality as needed to suit your specific use case.