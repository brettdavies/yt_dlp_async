# Documentation for `utils.py`

## Module Overview

The `utils.py` module provides a collection of utility functions and classes to support various operations such as URL preparation, file reading, metadata processing, and data extraction. It is designed to assist other modules in the application by offering reusable static methods.

### Key Components

- **URL Preparation**: Functions to construct URLs based on IDs and types.
- **File Reading**: Methods to read IDs from files or command-line arguments.
- **Metadata Processing**: Functions to prepare and manipulate metadata dictionaries.
- **Data Extraction**: Methods to extract dates, teams, and video information from text or file paths.
- **Modularity**: The utility functions are designed to be modular and reusable across different parts of the application.
- **Asynchronous Programming**: Some methods are asynchronous to support non-blocking operations in applications that use `asyncio`.

---

## Classes and Methods

### `Utils`

Utility class providing static methods for URL preparation, file reading, metadata processing, and data extraction.

---

#### `prep_url(id_val: str, id_type: str) -> str`

Prepares a URL based on the given ID value and ID type.

- **Args**:
  - `id_val`: The ID value (e.g., user ID, playlist ID).
  - `id_type`: The type of ID (`'user'`, `'user_playlist'`, `'playlist'`).

- **Returns**:
  - The prepared URL as a string.

---

#### `read_ids_from_file(file_path: str) -> List[str]`

Reads IDs from a file.

- **Args**:
  - `file_path`: The path to the file.

- **Returns**:
  - A list of IDs read from the file.

- **Raises**:
  - `FileNotFoundError`: If the file does not exist.

---

#### `read_ids_from_cli_argument_insert_db(video_ids: List[str], video_id_files: List[str]) -> None`

Reads video IDs from command-line arguments and inserts them into the database.

- **Args**:
  - `video_ids`: A list of video IDs provided as command-line arguments.
  - `video_id_files`: A list of file paths containing video IDs.

---

#### `prep_metadata_dictionary(item: json) -> Dict[str, Any]`

Prepares a metadata dictionary from the given JSON item.

- **Args**:
  - `item`: The JSON item containing the metadata.

- **Returns**:
  - A dictionary containing the prepared metadata.

---

#### `extract_date(text: str) -> Optional[datetime]`

Extracts a date from the given text string.

Attempts to find a date in various formats within the text.

- **Args**:
  - `text`: The text from which to extract the date.

- **Returns**:
  - A `datetime` object if a date is found, otherwise `None`.

---

#### `normalize_date_stub(date_stub: str) -> str`

Normalizes a date stub to the format `%Y%m%d`.

- **Args**:
  - `date_stub`: The date stub in various formats.

- **Returns**:
  - The normalized date stub as a string.

- **Raises**:
  - `ValueError`: If the `date_stub` is not in a valid format.

---

#### `extract_teams(text: str) -> Tuple[str, str]`

Extracts the home team and away team from the given text.

Attempts to identify team names based on predefined abbreviations.

- **Args**:
  - `text`: The text from which to extract team names.

- **Returns**:
  - A tuple `(home_team, away_team)`, where each is a string.
  - Returns `'unknown'` for one or both teams if extraction fails.

---

#### `extract_video_info_filepath(filepath: str) -> Tuple[Optional[str], Optional[str]]`

Extracts video ID and audio format ID from a file path.

Parses the file name to find the video ID and audio format ID enclosed in braces.

- **Args**:
  - `filepath`: The file path from which to extract information.

- **Returns**:
  - A tuple `(video_id, a_format_id)`. Returns `None` for any value not found.

---

## Example Usage

### Prerequisites

- **Python Environment**: Ensure Python 3.7 or higher is installed.
- **Dependencies**: Install required packages (if any).

### Using Utility Methods

```python
from utils import Utils

# Prepare a URL
url = await Utils.prep_url('UC_x5XG1OV2P6uZZ5FSM9Ttw', 'user')
print(url)  # Outputs: https://www.youtube.com/@UC_x5XG1OV2P6uZZ5FSM9Ttw/videos

# Read IDs from a file
ids = await Utils.read_ids_from_file('video_ids.txt')
print(ids)  # Outputs: ['dQw4w9WgXcQ', '9bZkp7q19f0']

# Extract date from text
date = Utils.extract_date('Event on 12.25.2021')
print(date)  # Outputs: datetime.datetime(2021, 12, 25, 0, 0)

# Extract teams from text
home_team, away_team = Utils.extract_teams('Lakers vs. Warriors')
print(home_team, away_team)  # Outputs: 'Lakers' 'Warriors'
```

---

## Understanding the Workflow

1. **URL Preparation**:
   - `prep_url` constructs URLs based on IDs and types for further processing.

2. **File Reading**:
   - `read_ids_from_file` reads IDs from specified files, supporting different file formats like `.txt` and `.csv`.

3. **Metadata Processing**:
   - `prep_metadata_dictionary` transforms raw JSON items into structured metadata dictionaries suitable for database insertion.

4. **Date and Team Extraction**:
   - `extract_date` and `extract_teams` parse text to extract relevant information like dates and team names.

---

## Additional Notes

- **Error Handling**: Methods include error handling and logging to assist in debugging and ensure smooth execution.
- **Asynchronous Methods**: Some methods are asynchronous (`async`) and should be awaited when called.
- **Type Hints**: The module uses type hints for better code readability and to assist with static analysis tools.
- **Logging**: Uses `loguru` for logging debug and error information.

---

## Conclusion

The `utils.py` module provides essential utility functions that streamline various tasks such as URL preparation, file reading, metadata manipulation, and data extraction. By following the Google Python Style Guide for docstrings, the module ensures clarity and maintainability, making it easier for developers to understand and utilize its functionalities.
