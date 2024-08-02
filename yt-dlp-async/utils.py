import os
import json
from typing import Any, Dict, List
from .database import DatabaseOperations

class Utils:
    @staticmethod
    async def prep_url(id_val: str, id_type: str):
        if id_type == 'user':
            id_val = (f"https://www.youtube.com/@{id_val}/videos")
        elif id_type == 'user_playlist':
            id_val = (f"https://www.youtube.com/@{id_val}/playlists")
        elif id_type == 'playlist':
            id_val = (f"https://www.youtube.com/playlist?list={id_val}")
        return id_val

    @staticmethod
    async def read_ids_from_file(file_path):
        _, file_extension = os.path.splitext(file_path)
        ids = []

        if file_extension == '.txt':
            with open(file_path, 'r') as file:
                ids = [line.strip() for line in file if line.strip()]
        elif file_extension == '.csv':
            import csv
            with open(file_path, 'r') as file:
                reader = csv.reader(file)
                ids = [row[0].strip() for row in reader if row and row[0].strip()]

        return ids

    @staticmethod
    async def read_ids_from_cli_argument_insert_db(video_ids: List[str], video_id_files: List[str]):
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
                        print(f"Attempting to insert videos from {file}")
                        await DatabaseOperations.insert_video_ids_bulk(file)
                        print(f"Currrent count of videos to be processed: {await DatabaseOperations.get_count_videos_to_be_processed()}")
                    elif file_extension == '.csv':
                        video_ids.extend(Utils.read_ids_from_file(file))
                        await DatabaseOperations.insert_video_ids(video_ids)
                    else:
                        print(f"{file_extension.removeprefix(".").upper()} is not an accepted format. Please use TXT or CSV.")

    @staticmethod
    async def prep_metadata_dictionary(item: json) -> Dict[str, Any]:
        # Extract required fields
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
            # 'rating_type': '',  # Placeholder, as contentRating object structure is unknown
            # 'rating_value': '',  # Placeholder, as contentRating object structure is unknown
            # 'recording_date': datetime.datetime.min,  # Placeholder, as recordingDetails object structure is unknown
            # 'recording_location': '',  # Placeholder, as recordingDetails object structure is unknown
            # 'transcript_text': '',  # Placeholder, as transcripts are not part of the provided schema
            # 'is_auto_generated': '',  # Placeholder, as transcripts are not part of the provided schema
        }
    
        return metadata