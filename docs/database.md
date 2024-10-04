# `database.py` Module Documentation

This document provides a detailed overview of the `database.py` module, which is responsible for managing database operations, including establishing SSH tunnels, handling database connections, and executing queries. The module is designed to interact with a PostgreSQL database securely over an SSH tunnel.

---

## Overview

The `database.py` module provides a class `DatabaseOperations` that encapsulates all database-related functionalities required by the application. It establishes an SSH tunnel to securely connect to a remote PostgreSQL database and manages a connection pool for efficient database interactions.

---

## Environment Variables

The module relies on several environment variables for configuration. These variables must be set in a [`.env`](../.env) file or exported in the environment.

- **SSH Configuration:**
  - `SSH_HOST`: SSH server hostname.
  - `SSH_PORT`: SSH server port (default: `22`).
  - `SSH_USER`: SSH username.
  - `SSH_KEY_PATH`: Path to the SSH private key file.
- **Database Configuration:**
  - `DB_USER`: Database username.
  - `DB_PASSWORD`: Database password.
  - `DB_NAME`: Name of the database.

---

## Class: `DatabaseOperations`

### Initialization

```python
def __init__(self) -> None:
```

Initializes the SSH tunnel and the database connection pool.

- **Raises:**
  - `EnvironmentError`: If any required environment variables are missing.
  - `Exception`: If an error occurs during SSH tunnel or connection pool initialization.

---

### Context Management Method

#### `close`

```python
def close(self):
```

Closes the database connection pool and the SSH tunnel.

---

### Database Connection Method

#### `get_db_connection`

```python
@contextmanager
def get_db_connection(self, worker_name: Optional[str]) -> Any:
```

Context manager for obtaining a database connection from the pool.

- **Args:**
  - `worker_name` (Optional[str]): Name of the worker requesting the connection.

- **Yields:**
  - `connection`: A database connection object.

- **Raises:**
  - `psycopg2.OperationalError`: If unable to obtain a database connection.

---

### Query Execution Methods

#### `execute_query`

```python
def execute_query(
    self,
    worker_name: str,
    query: str,
    params: Union[tuple, list],
    operation: str = 'fetch'
) -> Optional[Union[List[Dict], int]]:
```

Executes a database query.

- **Args:**
  - `worker_name` (str): Name of the worker executing the query.
  - `query` (str): SQL query to execute.
  - `params` (tuple or list): Parameters for the SQL query.
  - `operation` (str): Type of operation (`'fetch'`, `'commit'`, `'execute'`). Defaults to `'fetch'`.

- **Returns:**
  - `Optional[Union[List[Dict], int]]`: Result of the query based on the operation type.

- **Raises:**
  - `psycopg2.Error`: If a database error occurs during query execution.

---

### Video Processing Methods

#### `get_count_videos_to_be_processed`

```python
def get_count_videos_to_be_processed(self) -> Optional[int]:
```

Retrieves the count of videos that need to be processed.

- **Returns:**
  - `Optional[int]`: The count of videos to be processed, or `None` if an error occurs.

---

#### `get_video_ids_without_metadata`

```python
def get_video_ids_without_metadata(self) -> List[str]:
```

Retrieves a list of video IDs that lack associated metadata.

- **Returns:**
  - `List[str]`: A list of video IDs without metadata.

---

#### `set_video_id_failed_metadata_true`

```python
def set_video_id_failed_metadata_true(self, video_ids: set[str]) -> None:
```

Marks the given video IDs as having failed metadata retrieval.

- **Args:**
  - `video_ids` (set[str]): A set of video IDs to update.

---

#### `insert_video_ids`

```python
def insert_video_ids(self, video_ids_batch: List[str]) -> None:
```

Inserts a batch of video IDs into the database.

- **Args:**
  - `video_ids_batch` (List[str]): The batch of video IDs to insert.

---

#### `insert_video_ids_bulk`

```python
def insert_video_ids_bulk(self, id_file_path: str) -> None:
```

Inserts video IDs in bulk from a file into the database.

- **Args:**
  - `id_file_path` (str): The file path containing the video IDs.

- **Raises:**
  - `psycopg2.Error`: If a database error occurs during bulk insertion.
  - `FileNotFoundError`: If the specified file does not exist.

---

#### `insert_update_video_metadata`

```python
def insert_update_video_metadata(self, metadata: Dict[str, Any]) -> None:
```

Inserts or updates video metadata in the database.

- **Args:**
  - `metadata` (Dict[str, Any]): A dictionary containing the video metadata.

- **Raises:**
  - `psycopg2.Error`: If a database error occurs during insertion or update.

---

### Event Metadata Methods

#### `get_dates_no_event_metadata`

```python
def get_dates_no_event_metadata(self) -> List[datetime.datetime]:
```

Retrieves dates that have no associated event metadata.

- **Returns:**
  - `List[datetime.datetime]`: A list of dates lacking event metadata.

---

#### `save_events`

```python
def save_events(self, df: pd.DataFrame) -> None:
```

Inserts events into the `e_events` table in the database.

- **Args:**
  - `df` (pd.DataFrame): The DataFrame containing the events data.

- **Raises:**
  - `ValueError`: If the DataFrame does not contain all the required columns.
  - `psycopg2.Error`: If a database error occurs during insertion.

---

#### `check_if_existing_e_events_by_date`

```python
def check_if_existing_e_events_by_date(self, date_obj: datetime.datetime) -> bool:
```

Checks if there are existing events in `e_events` for the given date.

- **Args:**
  - `date_obj` (datetime.datetime): The date to check for existing events.

- **Returns:**
  - `bool`: `True` if events exist for the given date, `False` otherwise.

---

#### `get_e_events_team_info`

```python
def get_e_events_team_info(
    self,
    date_obj: datetime.datetime,
    opposing_team: str,
    is_home_unknown: bool
) -> Tuple[Optional[str], Optional[str]]:
```

Retrieves normalized team abbreviation from `e_events`.

- **Args:**
  - `date_obj` (datetime.datetime): The date of the event.
  - `opposing_team` (str): The name of the opposing team.
  - `is_home_unknown` (bool): Flag indicating whether the home team is unknown.

- **Returns:**
  - `Tuple[Optional[str], Optional[str]]`: A tuple containing the event ID (optional) and the normalized team abbreviation (optional).

---

#### `get_event_id`

```python
def get_event_id(
    self,
    date_obj: datetime.datetime,
    home_team: str,
    away_team: str
) -> Optional[str]:
```

Retrieves the event ID based on date and team information.

- **Args:**
  - `date_obj` (datetime.datetime): The date of the event.
  - `home_team` (str): The home team name.
  - `away_team` (str): The away team name.

- **Returns:**
  - `Optional[str]`: The event ID if found, otherwise `None`.

---

### Audio File Methods

#### `update_audio_file`

```python
def update_audio_file(self, video_file: Dict[str, Dict[str, str]]) -> bool:
```

Updates or inserts audio file metadata into the database.

- **Args:**
  - `video_file` (Dict[str, Dict[str, str]]): A dictionary containing the metadata of the audio file.

- **Returns:**
  - `bool`: `True` if the operation is successful, `False` otherwise.

---

#### `get_video_ids_without_files`

```python
def get_video_ids_without_files(self) -> List[str]:
```

Retrieves a list of video IDs that lack associated video files.

- **Returns:**
  - `List[str]`: A list of video IDs that meet the specified criteria.

---

## Logging and Configuration

The module uses `loguru` for logging, providing detailed logs for debugging and operational purposes. It also utilizes `dotenv` to load environment variables from a `.env` file.

---

## Dependencies

- **Standard Libraries:**
  - `os`
  - `datetime`
  - `contextlib`
  - `typing`
- **Third-Party Libraries:**
  - `psycopg2`: PostgreSQL adapter for Python.
    - [Psycopg2 Documentation](https://www.psycopg.org/docs/)
  - `sshtunnel`: For creating SSH tunnels.
    - [SSHTunnel Documentation](https://sshtunnel.readthedocs.io/en/latest/)
  - `loguru`: For logging.
    - [Loguru Documentation](https://loguru.readthedocs.io/)
  - `dotenv`: For loading environment variables.
    - [python-dotenv Documentation](https://saurabh-kumar.com/python-dotenv/)
  - `pandas`: For data manipulation, especially when dealing with DataFrames.
    - [Pandas Documentation](https://pandas.pydata.org/)

---

## Usage Example

```python
from database import DatabaseOperations

def main():
    with DatabaseOperations() as db_ops:
        video_ids = db_ops.get_video_ids_without_metadata()
        print(f"Video IDs without metadata: {video_ids}")

if __name__ == "__main__":
    main()
```

---

## Notes

- **Connection Management**: The module uses a connection pool to manage database connections efficiently. Always ensure connections are properly closed or returned to the pool.
- **Error Handling**: Exceptions are logged with detailed messages to facilitate debugging.
- **Environment Variables**: Ensure all required environment variables are set before using the module.
- **Security**: Sensitive information like database credentials and SSH keys are managed via environment variables to enhance security.
- **Performance**: The use of prepared statements and bulk operations improves the performance of database interactions.
