# Documentation for `e_events.py`

## Module Overview

The `e_events.py` module provides the `EventFetcher` class, which is responsible for fetching and processing Major League Baseball (MLB) event data from the ESPN API for a specified date. It processes the retrieved data and saves it to the database for further use.

### Key Components

- **Event Data Retrieval**: Fetches event data from the ESPN API based on a provided date.
- **Data Processing**: Extracts and normalizes relevant information from the raw event data.
- **Database Integration**: Saves the processed event data into a database using `DatabaseOperations`.
- **Asynchronous Execution**: Utilizes asynchronous programming (`async`/`await`) to improve performance.
- **Extensibility**: The `EventFetcher` class is designed to be easily extendable for fetching different types of events or integrating with other APIs.

---

## Class and Methods

### `EventFetcher`

Fetches and processes event data from the ESPN API.

The `EventFetcher` retrieves MLB event data for a specified date, processes the data, and saves it to the database.

#### Attributes

- **`team_abbreviations`** (`Dict`): A dictionary mapping team abbreviations to team names.

---

#### `__init__(self)`

Initializes the `EventFetcher` with team abbreviations.

**Example**:

```python
event_fetcher = EventFetcher()
```

---

#### `setup(self, date_stub: str) -> None`

Sets up the `EventFetcher` with the specified date.

Normalizes the date, constructs the API URL, and checks if data for the date is already loaded.

**Args**:

- `date_stub` (`str`): A string representing the date in various formats (e.g., `'20210101'`, `'2021-01-01'`).

**Raises**:

- `ValueError`: If the `date_stub` is not in a valid format.

**Example**:

```python
await event_fetcher.setup('2021-07-04')
```

---

#### `fetch_data(self) -> Optional[Dict]`

Fetches event data from the ESPN API.

**Returns**:

- `Optional[Dict]`: A dictionary containing the fetched data if successful, or `None` if the request fails.

**Example**:

```python
data = await event_fetcher.fetch_data()
```

---

#### `process_event(self, event: Dict) -> Optional[List]`

Processes a single event and extracts relevant information.

**Args**:

- `event` (`Dict`): A dictionary containing event data.

**Returns**:

- `Optional[List]`: A list containing extracted information:

  ```python
  [
      event_id,
      ny_time,
      season_type,
      short_name,
      home_team,
      away_team,
      home_team_normalized,
      away_team_normalized
  ]
  ```

  Returns `None` if any required fields are missing.

**Example**:

```python
processed_event = await event_fetcher.process_event(event)
```

---

#### `extract_events(self, data: Dict) -> List[List]`

Extracts and processes events from the provided data.

**Args**:

- `data` (`Dict`): A dictionary containing events data.

**Returns**:

- `List[List]`: A list of processed events, each represented as a list of extracted information.

**Example**:

```python
events_data = await event_fetcher.extract_events(data)
```

---

#### `create_dataframe(self, events_data: List[List]) -> pd.DataFrame`

Creates a pandas DataFrame from the given events data.

**Args**:

- `events_data` (`List[List]`): A list of event data, where each event is represented as a list.

**Returns**:

- `pd.DataFrame`: A pandas DataFrame containing the events data with specified columns.

**Example**:

```python
df = await event_fetcher.create_dataframe(events_data)
```

---

#### `save_to_database(self, dataframe: pd.DataFrame) -> None`

Saves the events data DataFrame to the database.

**Args**:

- `dataframe` (`pd.DataFrame`): A pandas DataFrame containing events data to be saved.

**Example**:

```python
await event_fetcher.save_to_database(df)
```

---

#### `run(self, date_stub: str) -> None`

Runs the `EventFetcher` to fetch, process, and save events data for a given date.

**Args**:

- `date_stub` (`str`): A string representing the date in various formats.

**Raises**:

- `ValueError`: If the `date_stub` is not in a valid format.

**Example**:

```python
await event_fetcher.run('2021-07-04')
```

---

## Example Usage

### Prerequisites

- **Python Environment**: Ensure you have Python 3.7 or higher installed.
- **Dependencies**: Install the required packages:

  ```bash
  pip install pandas requests pytz loguru
  ```

- **First Party Libraries**: Ensure that `Metadata`, `DatabaseOperations`, and `Utils` are correctly implemented and accessible.

### Running the EventFetcher

Here's how you can use the `EventFetcher` to fetch and process event data:

```python
import asyncio
from e_events import EventFetcher

async def main():
    event_fetcher = EventFetcher()
    await event_fetcher.run('2021-07-04')

if __name__ == '__main__':
    asyncio.run(main())
```

This script initializes the `EventFetcher`, sets up the date, fetches event data for July 4, 2021, processes it, and saves it to the database.

---

## Understanding the Workflow

1. **Initialization**:

   - An instance of `EventFetcher` is created.
   - Team abbreviations are loaded from `Metadata`.

2. **Setup**:

   - The `setup` method normalizes the provided date and constructs the API URL.
   - Checks if data for the date is already loaded in the database.

3. **Data Fetching**:

   - The `fetch_data` method makes an HTTP GET request to the ESPN API.
   - Retrieves event data in JSON format.

4. **Data Processing**:

   - The `extract_events` method iterates over the events and processes each one using `process_event`.
   - `process_event` extracts relevant information from each event.

5. **DataFrame Creation**:

   - The `create_dataframe` method converts the processed events into a pandas DataFrame.

6. **Saving to Database**:

   - The `save_to_database` method saves the DataFrame into the database using `DatabaseOperations`.

7. **Completion**:

   - Logs are recorded at each step to provide insights into the process.
   - Handles cases where data is already loaded or if fetching fails.

---

## Additional Notes

- **Asynchronous Programming**: The methods are asynchronous to allow for non-blocking execution, which is beneficial when integrating into larger asynchronous applications.

- **Error Handling**: Exceptions are caught and logged to prevent the application from crashing and to provide useful debug information.

- **Time Zones**: The event times are converted from UTC to America/New_York timezone for consistency.

- **Normalization**: Team names are normalized using the `Utils.extract_teams` method to ensure consistent data storage.

---

## Configuration Constants

- **`BASE_URL`**: Base URL for the ESPN API (`"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"`).

- **`LIMIT`**: Maximum number of events to fetch (default is `1000`).

---

## Conclusion

The `e_events.py` module provides a robust solution for fetching and processing MLB event data from the ESPN API. By utilizing asynchronous programming and efficient data processing techniques, it ensures that event data is accurately retrieved and stored for further use in the application.
