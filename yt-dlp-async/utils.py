import sys
import json
from typing import Dict, Any

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
    async def prep_metadata_dictionary(json_blob: json) -> Dict[str, Any]:
        # Extract the first item from the JSON blob
        item = json_blob['items'][0]

        # Extract required fields
        metadata = {
            'video_id': item['id'],
            'kind': item['kind'],
            'etag': item['etag'],
            'title': item['snippet']['title'],
            'description': item['snippet']['description'],
            'published_at': item['snippet']['publishedAt'],
            'channel_id': item['snippet']['channelId'],
            'channel_title': item['snippet']['channelTitle'],
            'category_id': item['snippet']['categoryId'],
            'live_broadcast_content': item['snippet']['liveBroadcastContent'],
            'default_language': item['snippet']['defaultLanguage'],
            'default_audio_language': item['snippet']['defaultAudioLanguage'],
            'duration': item['contentDetails']['duration'],
            'dimension': item['contentDetails']['dimension'],
            'definition': item['contentDetails']['definition'],
            'caption': item['contentDetails']['caption'],
            'licensed_content': item['contentDetails']['licensedContent'],
            'projection': item['contentDetails']['projection'],
            'upload_status': item['status']['uploadStatus'],
            'privacy_status': item['status']['privacyStatus'],
            'license': item['status']['license'],
            'embeddable': item['status']['embeddable'],
            'public_stats_viewable': item['status']['publicStatsViewable'],
            'made_for_kids': item['status']['madeForKids'],
            'view_count': int(item['statistics']['viewCount']),
            'like_count': int(item['statistics']['likeCount']),
            'dislike_count': 0,  # This field might not be available; default to 0
            'favorite_count': int(item['statistics']['favoriteCount']),
            'comment_count': int(item['statistics']['commentCount']),
            'tags': ','.join(item['snippet']['tags']),  # Convert list to comma-separated string
            'default_url': item['snippet']['thumbnails']['default']['url'],
            'default_width': item['snippet']['thumbnails']['default']['width'],
            'default_height': item['snippet']['thumbnails']['default']['height'],
            'medium_url': item['snippet']['thumbnails']['medium']['url'],
            'medium_width': item['snippet']['thumbnails']['medium']['width'],
            'medium_height': item['snippet']['thumbnails']['medium']['height'],
            'high_url': item['snippet']['thumbnails']['high']['url'],
            'high_width': item['snippet']['thumbnails']['high']['width'],
            'high_height': item['snippet']['thumbnails']['high']['height'],
            'standard_url': item['snippet']['thumbnails']['standard']['url'],
            'standard_width': item['snippet']['thumbnails']['standard']['width'],
            'standard_height': item['snippet']['thumbnails']['standard']['height'],
            'maxres_url': item['snippet']['thumbnails']['maxres']['url'],
            'maxres_width': item['snippet']['thumbnails']['maxres']['width'],
            'maxres_height': item['snippet']['thumbnails']['maxres']['height'],
            'language': 'en',  # Default language for localized info; this may need adjustment
            'localized_title': item['snippet']['localized']['title'],
            'localized_description': item['snippet']['localized']['description'],
            'topic_category': ','.join(item['topicDetails']['topicCategories']),  # Convert list to comma-separated string
            'rating_type': '',  # Placeholder, as contentRating object structure is unknown
            'rating_value': '',  # Placeholder, as contentRating object structure is unknown
            'recording_date': '',  # Placeholder, as recordingDetails object structure is unknown
            'recording_location': '',  # Placeholder, as recordingDetails object structure is unknown
            'transcript_text': '',  # Placeholder, as transcripts are not part of the provided schema
            'is_auto_generated': False,  # Placeholder, as transcripts are not part of the provided schema
        }

        return metadata