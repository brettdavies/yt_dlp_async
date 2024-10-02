# `video_metadata.py`

## Design Considerations and Enhancements

- **Asynchronous Processing**: Utilizes `asyncio` and `aiohttp` to handle asynchronous HTTP requests to the YouTube Data API, enabling efficient retrieval of video metadata.

- **Concurrent Worker Tasks**: Implements concurrent worker tasks and queues for parallel processing of video metadata, improving performance and scalability.

- **Modularity and Extensibility**: Organized into classes and functions, making it easy to extend and maintain.

- **External Libraries**: Employs `aiohttp` for asynchronous HTTP requests and `fire` for command-line interface.

- **Database Integration**: Interacts with `DatabaseOperations` to insert and update video metadata in the database.

- **Logging and Error Handling**: Uses `loguru` for comprehensive logging and includes robust error handling to ensure reliable operation during API calls and data processing.

## Module Overview

The `video_metadata.py` module is designed to fetch video metadata from the YouTube Data API using video IDs. It processes inputs from command-line arguments or files, retrieves metadata for the associated video IDs, and stores them in a database. The module leverages asynchronous programming and queues to manage and process metadata concurrently, making it suitable for handling large datasets efficiently.

### API Key Requirement
- **Google API Key**: This module requires a Google API key with access to the **YouTube Data API v3**. You can obtain an API key and ensure appropriate access by following [Google's official instructions](https://developers.google.com/youtube/v3/getting-started#before-you-start).

### Key Libraries and Dependencies

- **`asyncio`**: Asynchronous I/O framework used for managing asynchronous tasks and queues.
  - [Asyncio Documentation](https://docs.python.org/3/library/asyncio.html)

- **`aiohttp`**: Asynchronous HTTP client/server framework for Python, used to make HTTP requests to the YouTube Data API.
  - [aiohttp Documentation](https://docs.aiohttp.org/)

- **`fire`**: A library for automatically generating command-line interfaces.
  - [Fire Documentation](https://github.com/google/python-fire)

- **`loguru`**: A modern logging library that simplifies logging in Python.
  - [Loguru Documentation](https://loguru.readthedocs.io/)

- **`dotenv`**: Loads environment variables from a `.env` file.
  - [python-dotenv Documentation](https://saurabh-kumar.com/python-dotenv/)

- **`dataclasses`**: A module that provides a decorator and functions for automatically adding special methods to classes.
  - [Dataclasses Documentation](https://docs.python.org/3/library/dataclasses.html)

- **First Party Libraries**:
  - **`Utils`**: Contains utility functions for data preparation and reading IDs from files.
  - **`DatabaseOperations`**: Manages database operations for inserting and updating video metadata.
  - **`LoggerConfig`**: Configures logging settings using `loguru`.
  - **`EventFetcher`**: Fetches event metadata related to video metadata processing.

---

## Classes and Methods

### `QueueManager`

The `QueueManager` class manages queues for video metadata and event metadata, along with tracking the number of active tasks for each operation.

---

### `VideoIdOperations`

The `VideoIdOperations` class handles fetching video metadata from the YouTube Data API.

#### Key Methods

##### Method: `fetch_video_metadata(video_ids: List[str], worker_id: str) -> List[Dict[str, Any]]`

Fetches video metadata for the given list of video IDs using the YouTube Data API.

```python
@staticmethod
async def fetch_video_metadata(video_ids: List[str], worker_id: str) -> List[Dict[str, Any]]:
    """
    Fetches video metadata from the YouTube Data API.

    Args:
        video_ids (List[str]): List of video IDs to fetch metadata for.
        worker_id (str): ID of the worker executing the function.

    Returns:
        List[Dict[str, Any]]: List of dictionaries containing the fetched metadata.
    """
```

---

##### Method: `populate_event_metadata_queue(queue_manager: QueueManager) -> None`

Populates the event metadata queue with dates that require event metadata processing.

```python
async def populate_event_metadata_queue(queue_manager: QueueManager) -> None:
    """
    Populates the event metadata queue with dates that require event metadata processing.

    Args:
        queue_manager (QueueManager): The queue manager instance.

    Returns:
        None
    """
```

---

### `Fetcher`

The `Fetcher` class coordinates the fetching and processing of video metadata. It initializes the queues and starts the worker tasks.

#### Constructor: `__init__(self)`

Initializes the `Fetcher` with a `QueueManager` instance and a shutdown event.

```python
@dataclass(slots=True)
class Fetcher:
    queue_manager: QueueManager
    shutdown_event: asyncio.Event

    def __init__(self):
        """
        Initializes the Fetcher with a QueueManager instance.

        Args:
            None

        Returns:
            None
        """
```

---

### Key Methods

##### Method: `fetch(self, video_ids: Optional[List[str]] = None, video_id_files: Optional[List[str]] = None, num_workers: int = 2) -> None`

Fetches video metadata from the YouTube Data API and starts worker tasks.

```python
async def fetch(self, video_ids: Optional[List[str]] = None, video_id_files: Optional[List[str]] = None, num_workers: int = 2) -> None:
    """
    Fetches video metadata from the YouTube Data API and starts worker tasks.

    Args:
        video_ids (Optional[List[str]]): A list of video IDs.
        video_id_files (Optional[List[str]]): A list of file paths containing video IDs.
        num_workers (int): The number of worker tasks to create.

    Returns:
        None
    """
```

---

### Worker Functions

These asynchronous functions are used as worker tasks to process video metadata from the queues concurrently.

#### Function: `worker_retrieve_metadata(queue_manager: QueueManager, shutdown_event: asyncio.Event, worker_id: str) -> None`

Retrieves video metadata from the YouTube Data API and puts it into the metadata queue.

```python
async def worker_retrieve_metadata(queue_manager: QueueManager, shutdown_event: asyncio.Event, worker_id: str) -> None:
    """
    Worker function that retrieves video metadata from the YouTube Data API.

    Args:
        queue_manager (QueueManager): Manages the queues and active tasks.
        shutdown_event (asyncio.Event): Event to signal when to shut down the worker.
        worker_id (str): ID of the worker executing the function.

    Returns:
        None
    """
```

---

#### Function: `worker_save_metadata(queue_manager: QueueManager, shutdown_event: asyncio.Event, worker_id: str) -> None`

Saves video metadata from the metadata queue into the database.

```python
async def worker_save_metadata(queue_manager: QueueManager, shutdown_event: asyncio.Event, worker_id: str) -> None:
    """
    Worker function that saves video metadata to the database.

    Args:
        queue_manager (QueueManager): Manages the queues and active tasks.
        shutdown_event (asyncio.Event): Event to signal when to shut down the worker.
        worker_id (str): ID of the worker executing the function.

    Returns:
        None
    """
```

---

#### Function: `worker_event_metadata(queue_manager: QueueManager, worker_id: str) -> None`

Processes event metadata by calling an instance of the `EventFetcher` class.

```python
async def worker_event_metadata(queue_manager: QueueManager, worker_id: str) -> None:
    """
    Worker function that processes event metadata.

    Args:
        queue_manager (QueueManager): Manages the queues and active tasks.
        worker_id (str): ID of the worker executing the function.

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
  pip install aiohttp fire loguru python-dotenv
  ```

- **YouTube Data API Key**: Obtain an API key from Google Cloud Console and set it in your environment variables or a `.env` file as `YT_API_KEY`.

### Running the Module

The `video_metadata.py` module provides a command-line interface through the `cmd()` function. You can use this interface to fetch video metadata from various sources and insert it into the database for further processing.

#### Command-Line Interface

To use the module from the command line, navigate to the directory containing `video_metadata.py` and run it using Python. Below are examples of how to use it.

---

### Fetching Metadata for a List of Video IDs

Suppose you have a list of video IDs that you want to process.

```bash
python video_metadata.py fetch --video_ids "dQw4w9WgXcQ,9bZkp7q19f0"
```

- **Parameters**:
  - `--video_ids`: A comma-separated list of YouTube video IDs.

**Explanation**:

- The `fetch` command invokes the `fetch` method of the `Fetcher` class.
- The `--video_ids` argument accepts a list of video IDs.

---

### Fetching Metadata from a File

If you have a text file containing video IDs (one per line), you can specify the file path.

```bash
python video_metadata.py fetch --video_id_files "/path/to/video_ids.txt"
```

- **Parameters**:
  - `--video_id_files`: A comma-separated list of file paths containing video IDs.

**Explanation**:

- The `fetch` method reads video IDs from the specified file(s) and processes them.

---

### Fetching Metadata for Video IDs Already in the Database

If you have video IDs stored in the database without metadata, you can simply run the fetch command without any parameters.

```bash
python video_metadata.py fetch
```

**Explanation**:

- The module will fetch video IDs that are in the database but lack metadata and attempt to retrieve and store their metadata.

---

### Sample Code for Invoking the Fetcher Programmatically

If you prefer to use the module within another Python script, you can invoke the `Fetcher` class directly.

```python
import asyncio
from video_metadata import Fetcher

async def main():
    fetcher = Fetcher()
    await fetcher.fetch(
        video_ids=["dQw4w9WgXcQ"],
        video_id_files=["/path/to/video_ids.txt"],
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

- **Database Operations**: The module assumes that `DatabaseOperations` is properly configured to handle database interactions, including methods like `insert_update_video_metadata`, `set_video_id_failed_metadata_true`, and `get_video_ids_without_metadata`.

- **YouTube Data API Quota**: The module handles the `quotaExceeded` error from the YouTube Data API and stops making requests when the quota is exceeded.

- **Logging**: The module uses `loguru` for logging. Logs will be output to the console and can be configured via the `LoggerConfig` class.

- **Error Handling**: The module includes robust error handling to manage network issues or API errors when invoking the YouTube Data API.

- **Concurrency**: Adjust the `--num_workers` parameter according to your system's capabilities and API quota limits to optimize performance.

---

## Understanding the Workflow

1. **Initialization**:
   - The `Fetcher` class initializes a `QueueManager` instance to manage the queues for metadata and event metadata.
   - A shutdown event is also initialized to signal when workers should stop.

2. **Adding Video IDs**:
   - Video IDs provided via command-line arguments or files are added to the database if not already present.
   - The module then retrieves video IDs from the database that lack metadata.

3. **Worker Tasks**:
   - **Retrieve Metadata Workers**:
     - Fetch video IDs without metadata from the database.
     - Use the YouTube Data API to fetch metadata for these video IDs.
     - Put the retrieved metadata into the metadata queue.
   - **Save Metadata Workers**:
     - Retrieve metadata from the metadata queue.
     - Save the metadata to the database using `DatabaseOperations`.
   - **Event Metadata Workers**:
     - Fetch dates that require event metadata processing.
     - Use `EventFetcher` to process event metadata for these dates.

4. **Asynchronous Processing**:
   - All workers run asynchronously, enabling concurrent processing and efficient utilization of resources.
   - The shutdown event ensures that workers stop gracefully when there is no more work to do.

5. **Database Insertion**:
   - Video metadata collected from the YouTube Data API is inserted or updated in the database for downstream tasks.

---

## Conclusion

The `video_metadata.py` module provides a flexible and efficient way to collect video metadata from the YouTube Data API. By utilizing asynchronous programming and concurrent workers, it can handle large volumes of data effectively. The module is designed to be both a standalone command-line tool and a module that can be integrated into other Python applications.

Adjust the parameters and extend the functionality as needed to suit your specific use case.
