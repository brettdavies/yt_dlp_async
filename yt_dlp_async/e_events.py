# Standard Libraries
import pytz
import requests
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime

# Logging
from loguru import logger

# First Party Libraries
from .metadata import Metadata
from .database import DatabaseOperations
from .utils import Utils

class EventFetcher:
    """
    A class that fetches and processes events data from ESPN API.
    Args:
        date_stub (str): The date stub in the format '%Y%m%d'.
    Attributes:
        date_stub (str): The date stub in the format '%Y%m%d'.
        url (str): The URL to fetch the events data from.
        team_abbreviations (Dict): A list of team abbreviations.
    Methods:
        setup(): Sets up the EventFetcher instance.
        fetch_data(): Fetches the events data from the API.
        process_event(event): Processes a single event.
        extract_events(data): Extracts the events from the data.
        create_dataframe(events_data): Creates a pandas DataFrame from the events data.
        save_to_database(dataframe): Saves the events data to the database.
        run(): Runs the EventFetcher to fetch, process, and save the events data.
    """

    BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
    LIMIT = 1000

    def __init__(self) -> None:
        """
        Initialize the Event class with a date stub.
        Args:
            date_stub (str): The date stub in the format '%Y%m%d'.
        Raises:
            ValueError: If the date_stub is not in a valid format.
        """
        self.team_abbreviations = Metadata.team_abbreviations

    async def setup(self, date_stub: str) -> None:
        self.date_stub = await Utils.normalize_date_stub(date_stub)
        self.url = f"{self.BASE_URL}?limit={self.LIMIT}&dates={self.date_stub}"
        logger.debug(f"Setting up EventFetcher with date_stub: {self.date_stub}")
        self.already_loaded = await DatabaseOperations.check_if_existing_e_events_by_date(self.date_stub)
        logger.debug(f"Data already loaded: {self.already_loaded}")

    async def fetch_data(self) -> Dict:
        """
        Fetches data from the specified URL.

        Returns:
            dict or None: The fetched data as a dictionary if the request is successful, None otherwise.
        """
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to retrieve data: {e}")
            return None

    async def process_event(self, event) -> List:
        """
        Process an event and extract relevant information.

        Args:
            event (dict): The event data.

        Returns:
            List or None: A List containing the extracted information [event_id, ny_time, season_type, short_name, home_team, away_team, home_team_normalized, away_team_normalized]. Returns None if any of the required fields are missing.
        """
        try:
            event_id = event.get('id')
            date = event.get('date')
            short_name = event.get('shortName')
            season_type = event.get('season', {}).get('type')
            # Extract the home team abbreviation
            home_team = next((team['team']['abbreviation'] for team in event.get('competitions', [{}])[0].get('competitors', []) if team.get('homeAway') == 'home'), None)
            # Extract the away team abbreviation
            away_team = next((team['team']['abbreviation'] for team in event.get('competitions', [{}])[0].get('competitors', []) if team.get('homeAway') == 'away'), None)
            
            if not all([event_id, date, short_name, season_type, home_team, away_team]):
                return None
            
            else:
                # Normalize the team abbrevations
                home_team_normalized, away_team_normalized = Utils.extract_teams(f"{away_team} @ {home_team}")
            
                date_no_z = date.rstrip('Z')
                utc_time = datetime.strptime(date_no_z, '%Y-%m-%dT%H:%M')
                utc_time = pytz.utc.localize(utc_time)
                ny_time = utc_time.astimezone(pytz.timezone('America/New_York'))
                return [event_id, ny_time, season_type, short_name, home_team, away_team, home_team_normalized, away_team_normalized]
        except KeyError as e:
            logger.error(f"process_event() KeyError: {e}")
            return None

    async def extract_events(self, data) -> List[Optional[List]]:
        """
        Extracts events from the given data.

        Args:
            data (dict): The data containing events.

        Returns:
            List: A List of processed events.

        """
        events = data.get('events', [])
        processed_events = []
        for event in events:
            processed_event = await self.process_event(event)
            if processed_event:
                processed_events.append(processed_event)
        return processed_events

    async def create_dataframe(self, events_data) -> pd.DataFrame:
        """
        Create a pandas DataFrame from the given events_data.

        Parameters:
        - events_data (list): A list of event data containing the following columns:
            - event_id (int): The ID of the event.
            - date (str): The date of the event.
            - type (str): The type of the event.
            - short_name (str): The short name of the event.
            - home_team (str): The home team of the event.
            - away_team (str): The away team of the event.
            - home_team_normalized (str): The normalized name of the home team.
            - away_team_normalized (str): The normalized name of the away team.

        Returns:
        - pandas.DataFrame: A DataFrame containing the events_data with the specified columns.
        """
        columns = ['event_id', 'date', 'type', 'short_name', 'home_team','away_team', 'home_team_normalized', 'away_team_normalized']
        return pd.DataFrame(events_data, columns=columns)

    async def save_to_database(self, dataframe) -> None:
        """
        Saves the given dataframe to the database.

        Parameters:
        - dataframe: The dataframe to be saved.
        """
        await DatabaseOperations.save_events(dataframe)

    async def run(self, date_stub:str) -> None:
        """
        Runs the EventFetcher.

        This method sets up the necessary configurations, fetches data, extracts events from the data,
        creates a dataframe from the events data, and saves the dataframe to the database.
        """
        await self.setup(date_stub)
        logger.info(f"Starting EventFetcher with date_stub: {self.date_stub}")
        if not self.already_loaded:
            data = await self.fetch_data()
            if data:
                events_data = await self.extract_events(data)
                if events_data:
                    df = await self.create_dataframe(events_data)
                    await self.save_to_database(df)
                    logger.info("Data successfully saved to the database.")
                else:
                    logger.warning("No events data to process.")
            else:
                logger.error("Failed to fetch data.")
        else:
            logger.info("Data already loaded for the given date_stub.")
