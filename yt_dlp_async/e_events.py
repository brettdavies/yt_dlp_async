# Standard Libraries
import pytz
import requests
import pandas as pd
from typing import Dict, List
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
        team_abbreviations (list): A list of team abbreviations.
    Methods:
        setup(): Sets up the EventFetcher instance.
        fetch_data(): Fetches the events data from the API.
        process_event(event): Processes a single event.
        extract_events(data): Extracts the events from the data.
        create_dataframe(events_data): Creates a pandas DataFrame from the events data.
        save_to_database(dataframe): Saves the events data to the database.
        run(): Runs the EventFetcher to fetch, process, and save the events data.
    """
    def __init__(self, date_stub: str) -> None:
        """
        Initialize the Event class with a date stub.
        Args:
            date_stub (str): The date stub in the format '%Y%m%d'.
        Raises:
            ValueError: If the date_stub is not in a valid format.
        Attributes:
            date_stub (str): The date stub in the format '%Y%m%d'.
            url (str): The URL for retrieving the scoreboard data.
            team_abbreviations (list): A list of team abbreviations.
        """
        self.date_stub = date_stub

        # Normalize date_stub to the format '%Y%m%d'
        try:
            if '-' in date_stub:
                date_obj = datetime.strptime(date_stub, '%Y-%m-%d')
            elif '/' in date_stub:
                date_obj = datetime.strptime(date_stub, '%Y/%m/%d')
            else:
                date_obj = datetime.strptime(date_stub, '%Y%m%d')
            
            date_blob = date_obj.strftime('%Y%m%d')
            self.url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?limit=1000&dates={date_blob}"
        except ValueError as e:
            logger.error(f"init() ValueError: {e}")
            raise

        self.team_abbreviations = Metadata.team_abbreviations

    def setup(self) -> None:
        self.already_loaded = DatabaseOperations.check_if_existing_e_events_by_date(self.date_stub)

    def fetch_data(self) -> Dict:
        """
        Fetches data from the specified URL.

        Returns:
            dict or None: The fetched data as a dictionary if the request is successful, None otherwise.
        """
        response = requests.get(self.url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logger.error(f"Failed to retrieve data. Status code: {response.status_code}")
            return None

    def process_event(self, event) -> List:
        """
        Process an event and extract relevant information.

        Args:
            event (dict): The event data.

        Returns:
            list or None: A list containing the extracted information [event_id, ny_time, season_type, short_name, home_team, away_team, home_team_normalized, away_team_normalized]. Returns None if any of the required fields are missing.
        """
        event_id = event.get('id')
        date = event.get('date')
        short_name = event.get('shortName')
        season_type = event.get('season', {}).get('type')
        # Extract the home team abbreviation
        home_team = next((team['team']['abbreviation'] for team in event.get('competitions', [{}])[0].get('competitors', []) if team.get('homeAway') == 'home'), None)
        # Extract the away team abbreviation
        away_team = next((team['team']['abbreviation'] for team in event.get('competitions', [{}])[0].get('competitors', []) if team.get('homeAway') == 'away'), None)
        # Normalize the team abbrevations
        home_team_normalized, away_team_normalized = Utils.extract_teams(f"{away_team} @ {home_team}")

        if event_id and date and short_name and season_type and season_type > 1:
            date_no_z = date.rstrip('Z')
            utc_time = datetime.strptime(date_no_z, '%Y-%m-%dT%H:%M')
            utc_time = pytz.utc.localize(utc_time)
            ny_time = utc_time.astimezone(pytz.timezone('America/New_York'))
            return [event_id, ny_time, season_type, short_name, home_team, away_team, home_team_normalized, away_team_normalized]
        return None

    def extract_events(self, data) -> list:
        """
        Extracts events from the given data.

        Args:
            data (dict): The data containing events.

        Returns:
            list: A list of processed events.

        """
        events_data = []
        for event in data.get('events', []):
            processed_event = self.process_event(event)
            if processed_event:
                events_data.append(processed_event)
        return events_data

    def create_dataframe(self, events_data) -> pd.DataFrame:
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
        return pd.DataFrame(events_data, columns=['event_id', 'date', 'type', 'short_name', 'home_team','away_team', 'home_team_normalized', 'away_team_normalized'])

    def save_to_database(self, dataframe) -> None:
        """
        Saves the given dataframe to the database.

        Parameters:
        - dataframe: The dataframe to be saved.

        Returns:
        None
        """
        DatabaseOperations.insert_e_events(dataframe)

    def run(self) -> None:
        """
        Runs the EventFetcher.

        This method sets up the necessary configurations, fetches data, extracts events from the data,
        creates a dataframe from the events data, and saves the dataframe to the database.

        Returns:
            None
        """
        self.setup()
        logger.info(f"starting EventFetcher date_stub: {self.date_stub}")
        if not self.already_loaded:
            data = self.fetch_data()
            if data:
                events_data = self.extract_events(data)
                df = self.create_dataframe(events_data)
                self.save_to_database(df)
