import requests
import pytz
import pandas as pd
from datetime import datetime
from loguru import logger
from .metadata import Metadata
from .database import DatabaseOperations
from .utils import Utils

class EventFetcher:
    def __init__(self, date_stub: str):
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

    def setup(self):
        self.already_loaded = DatabaseOperations.check_if_existing_e_events_by_date(self.date_stub)

    def fetch_data(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logger.error(f"Failed to retrieve data. Status code: {response.status_code}")
            return None

    def process_event(self, event):
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

    def extract_events(self, data):
        events_data = []
        for event in data.get('events', []):
            processed_event = self.process_event(event)
            if processed_event:
                events_data.append(processed_event)
        return events_data

    def create_dataframe(self, events_data):
        return pd.DataFrame(events_data, columns=['event_id', 'date', 'type', 'short_name', 'home_team','away_team', 'home_team_normalized', 'away_team_normalized'])

    def save_to_database(self, dataframe):
        DatabaseOperations.insert_e_events(dataframe)

    def run(self):
        self.setup()
        logger.info(f"starting EventFetcher date_stub: {self.date_stub}")
        if not self.already_loaded:
            data = self.fetch_data()
            if data:
                events_data = self.extract_events(data)
                df = self.create_dataframe(events_data)
                self.save_to_database(df)