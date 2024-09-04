# Standard Libraries
import os
import re
import json
from datetime import datetime
from typing import Any, Dict, List, Tuple
from dataclasses import dataclass

# Logging
from loguru import logger

# First Party Libraries
from .database import DatabaseOperations
from .metadata import Metadata

class Utils:
    @staticmethod
    async def prep_url(id_val: str, id_type: str) -> str:
        """
        Prepare the URL based on the given ID value and ID type.

        Args:
            id_val (str): The ID value.
            id_type (str): The type of ID.

        Returns:
            str: The prepared URL.

        Raises:
            None

        """
        if id_type == 'user':
            id_val = (f"https://www.youtube.com/@{id_val}/videos")
        elif id_type == 'user_playlist':
            id_val = (f"https://www.youtube.com/@{id_val}/playlists")
        elif id_type == 'playlist':
            id_val = (f"https://www.youtube.com/playlist?list={id_val}")
        return id_val

    @staticmethod
    async def read_ids_from_file(file_path: str) -> List[str]:
        """
        Read IDs from a file.

        Args:
            file_path (str): The path to the file.

        Returns:
            List[str]: A list of IDs read from the file.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        _, file_extension = os.path.splitext(file_path)
        ids: List[str] = []

        try:
            if file_extension == '.txt':
                with open(file_path, 'r') as file:
                    ids = [line.strip() for line in file if line.strip()]
            elif file_extension == '.csv':
                import csv
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    ids = [row[0].strip() for row in reader if row and row[0].strip()]
        except FileNotFoundError as e:
            logger.error(f"File not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            raise

        return ids

    @staticmethod
    async def read_ids_from_cli_argument_insert_db(video_ids: List[str], video_id_files: List[str]) -> None:
        """
        Reads video IDs from command-line arguments and inserts them into the database.

        Args:
            video_ids (List[str]): A list of video IDs provided as command-line arguments.
            video_id_files (List[str]): A list of file paths containing video IDs.

        Returns:
            None
        """
        if video_ids:
            if isinstance(video_ids, str):
                video_ids = video_ids.replace(',', ' ').split()
            await DatabaseOperations.insert_video_ids(video_ids)

        if video_id_files:
            if isinstance(video_id_files, str):
                video_id_files = video_id_files.replace(',', ' ').split()
            for file in video_id_files:
                _, file_extension = os.path.splitext(file)
                if file_extension == '.txt':
                    logger.info(f"Attempting to insert videos from {file}")
                    await DatabaseOperations.insert_video_ids_bulk(file)
                    logger.info(f"Current count of videos to be processed: {await DatabaseOperations.get_count_videos_to_be_processed()}")
                elif file_extension == '.csv':
                    video_ids.extend(await Utils.read_ids_from_file(file))
                    await DatabaseOperations.insert_video_ids(video_ids)
                else:
                    logger.error(f"{file_extension.removeprefix('.').upper()} is not an accepted format. Please use TXT or CSV.")

    @staticmethod
    async def prep_metadata_dictionary(item: json) -> Dict[str, Any]:
        """
        Prepares a metadata dictionary from the given JSON item.
        Args:
            item (json): The JSON item containing the metadata.
        Returns:
            Dict[str, Any]: The prepared metadata dictionary.
        """
        metadata = {
            'video_id': item.get('id', ''),
            'kind': item.get('kind', ),
            'etag': item.get('etag', ),
            'title': item.get('snippet', {}).get('title', ),
            'description': item.get('snippet', {}).get('description', ),
            'published_at': item.get('snippet', {}).get('publishedAt', ),
            'channel_id': item.get('snippet', {}).get('channelId', ),
            'channel_title': item.get('snippet', {}).get('channelTitle', ),
            'category_id': item.get('snippet', {}).get('categoryId', ),
            'live_broadcast_content': item.get('snippet', {}).get('liveBroadcastContent', ),
            'default_language': item.get('snippet', {}).get('defaultLanguage', ),
            'default_audio_language': item.get('snippet', {}).get('defaultAudioLanguage', ),
            'duration': item.get('contentDetails', {}).get('duration', ),
            'dimension': item.get('contentDetails', {}).get('dimension', ),
            'definition': item.get('contentDetails', {}).get('definition', ),
            'caption': item.get('contentDetails', {}).get('caption', ),
            'licensed_content': item.get('contentDetails', {}).get('licensedContent', ),
            'projection': item.get('contentDetails', {}).get('projection', ),
            'upload_status': item.get('status', {}).get('uploadStatus', ),
            'privacy_status': item.get('status', {}).get('privacyStatus', ),
            'license': item.get('status', {}).get('license', ),
            'embeddable': item.get('status', {}).get('embeddable', ),
            'public_stats_viewable': item.get('status', {}).get('publicStatsViewable', ),
            'made_for_kids': item.get('status', {}).get('madeForKids', ),
            'view_count': int(item.get('statistics', {}).get('viewCount', 0)),
            'like_count': int(item.get('statistics', {}).get('likeCount', 0)),
            'dislike_count': 0,  # Default value as it's not present in the provided schema
            'favorite_count': int(item.get('statistics', {}).get('favoriteCount', 0)),
            'comment_count': int(item.get('statistics', {}).get('commentCount', 0)),
            'tags': item.get('snippet', {}).get('tags', ''),
            'default_url': item.get('snippet', {}).get('thumbnails', {}).get('default', {}).get('url', ),
            'default_width': item.get('snippet', {}).get('thumbnails', {}).get('default', {}).get('width', ),
            'default_height': item.get('snippet', {}).get('thumbnails', {}).get('default', {}).get('height', ),
            'medium_url': item.get('snippet', {}).get('thumbnails', {}).get('medium', {}).get('url', ),
            'medium_width': item.get('snippet', {}).get('thumbnails', {}).get('medium', {}).get('width', ),
            'medium_height': item.get('snippet', {}).get('thumbnails', {}).get('medium', {}).get('height', ),
            'high_url': item.get('snippet', {}).get('thumbnails', {}).get('high', {}).get('url', ),
            'high_width': item.get('snippet', {}).get('thumbnails', {}).get('high', {}).get('width', ),
            'high_height': item.get('snippet', {}).get('thumbnails', {}).get('high', {}).get('height', ),
            'standard_url': item.get('snippet', {}).get('thumbnails', {}).get('standard', {}).get('url', ),
            'standard_width': item.get('snippet', {}).get('thumbnails', {}).get('standard', {}).get('width', ),
            'standard_height': item.get('snippet', {}).get('thumbnails', {}).get('standard', {}).get('height', ),
            'maxres_url': item.get('snippet', {}).get('thumbnails', {}).get('maxres', {}).get('url', ),
            'maxres_width': item.get('snippet', {}).get('thumbnails', {}).get('maxres', {}).get('width', ),
            'maxres_height': item.get('snippet', {}).get('thumbnails', {}).get('maxres', {}).get('height', ),
            'language': 'en',  # Default language for localized info; this may need adjustment
            'localized_title': item.get('snippet', {}).get('localized', {}).get('title', ),
            'localized_description': item.get('snippet', {}).get('localized', {}).get('description', ),
            'topic_category': item.get('topicDetails', {}).get('topicCategories', ''),
        }
    
        return metadata

    @staticmethod
    def extract_date(text: str):
        # Extract date in DD.MM.YYYY format
        date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%d.%m.%Y'), text

        # Extract date in MM.DD.YYYY format
        date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%m.%d.%Y'), text

        # Extract date in MM.DD.YY format
        date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{2})', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%m.%d.%y'), text

        # Extract date in MM/DD/YY format
        date_match = re.search(r'(\d{1,2}\/\d{1,2}\/\d{2})', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%m/%d/%y'), text

        # Extract date in MM-DD-YY format
        date_match = re.search(r'(\d{1,2}\-\d{1,2}\-\d{2})', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%m-%d-%y'), text

        # Extract date in "Month DD, YYYY" format
        date_match = re.search(r'(\b\w+\s\d{1,2},\s\d{4}\b)', text)
        if date_match:
            date_str = date_match.group(1)
            text = text.replace(date_match.group(0), '').strip()
            return datetime.strptime(date_str, '%B %d, %Y'), text

        return None, text

    @staticmethod
    async def normalize_date_stub(date_stub: str) -> str:
        """
        Normalize the date stub to the format '%Y%m%d'.
        Args:
            date_stub (str): The date stub in various formats.
        Returns:
            str: The normalized date stub.
        Raises:
            ValueError: If the date_stub is not in a valid format.
        """
        try:
            if '-' in date_stub:
                date_obj = datetime.strptime(date_stub, '%Y-%m-%d')
            elif '/' in date_stub:
                date_obj = datetime.strptime(date_stub, '%Y/%m/%d')
            else:
                date_obj = datetime.strptime(date_stub, '%Y%m%d')
            return date_obj.strftime('%Y%m%d')
        except ValueError as e:
            logger.error(f"normalize_date_stub() ValueError: {e}")
            raise

    @staticmethod
    async def extract_teams(text) -> Tuple[str, str]:
        """
        Returns normalized home_team and away_team as defined in Metadata.team_abbreviations.

        Returns 'Unknown' for one or both teams if the team_abbreviations lookup fails.
        """
        # Normalize the title to lowercase for consistent comparison
        normalized_text = text.lower()
        
        # List of delimiters to look for
        for delimiter in [' at ', ' @ ', ' vs ', ' vs. ']:
            if delimiter in normalized_text:
                # Split the text by the delimiter
                parts = normalized_text.split(delimiter)

                # Initialize sets for candidate teams
                away_team_candidates = set()
                home_team_candidates = set()
                
                team_abbreviations = Metadata.team_abbreviations

                # Function to find team candidates within a part of the text
                def find_team_candidates(part, candidates_set):
                    for team in team_abbreviations:
                        if re.search(r'\b' + re.escape(team) + r'\b', part):
                            candidates_set.add(team_abbreviations.get(team, 'Unknown'))
                
                # Extract team names from parts[0] (LHS of delimiter, away team)
                find_team_candidates(parts[0], away_team_candidates)
                
                # Extract team names from parts[1] (RHS of delimiter, home team)
                find_team_candidates(parts[1], home_team_candidates)
                
                # Handle different candidate scenarios
                if len(away_team_candidates) == 1 and len(home_team_candidates) == 1:
                    # Both sides have exactly one candidate team
                    known_away_team = next(iter(away_team_candidates))
                    known_home_team = next(iter(home_team_candidates))
                    return known_home_team, known_away_team
                
                elif len(away_team_candidates) == 1 and len(home_team_candidates) > 1:
                    known_away_team = next(iter(away_team_candidates))
                    home_team_candidates.discard(known_away_team)
                    
                    if len(home_team_candidates) == 1:
                        known_home_team = next(iter(home_team_candidates))
                        return known_home_team, known_away_team
                    
                elif len(home_team_candidates) == 1 and len(away_team_candidates) > 1:
                    known_home_team = next(iter(home_team_candidates))
                    away_team_candidates.discard(known_home_team)
                    
                    if len(away_team_candidates) == 1:
                        known_away_team = next(iter(away_team_candidates))
                        return known_home_team, known_away_team
                
                if len(away_team_candidates) > 1 and len(home_team_candidates) > 1:
                    return 'Unknown', 'Unknown'

        # Handle cases where no known delimiter is found or the team extraction fails
        return 'Unknown', 'Unknown'
