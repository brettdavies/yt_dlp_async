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

    async def setup(self):
        self.already_loaded = await DatabaseOperations.fetch_existing_e_events_by_date(self.date_stub)

    async def fetch_data(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logger.error(f"Failed to retrieve data. Status code: {response.status_code}")
            return None

    async def process_event(self, event):
        event_id = event.get('id')
        date = event.get('date')
        short_name = event.get('shortName')
        season_type = event.get('season', {}).get('type')

        if event_id and date and short_name and season_type and season_type > 1:
            date_no_z = date.rstrip('Z')
            utc_time = datetime.strptime(date_no_z, '%Y-%m-%dT%H:%M')
            utc_time = pytz.utc.localize(utc_time)
            ny_time = utc_time.astimezone(pytz.timezone('America/New_York'))

            normalized_name, short_name = Utils.extract_teams(short_name)
            return [event_id, ny_time, season_type, short_name, normalized_name]
        return None

    async def extract_events(self, data):
        events_data = []
        for event in data.get('events', []):
            processed_event = await self.process_event(event)
            if processed_event:
                events_data.append(processed_event)
        return events_data

    async def create_dataframe(self, events_data):
        return pd.DataFrame(events_data, columns=['event_id', 'date', 'type', 'short_name', 'normalized_name'])

    async def save_to_database(self, dataframe):
        await DatabaseOperations.insert_e_events(dataframe)

    async def run(self):
        await self.setup()
        if not self.already_loaded:
            data = await self.fetch_data()
            if data:
                events_data = await self.extract_events(data)
                df = await self.create_dataframe(events_data)
                await self.save_to_database(df)
