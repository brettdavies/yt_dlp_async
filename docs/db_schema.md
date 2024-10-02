# Documentation for `db_schema.sql`

## Overview

The `db_schema.sql` script defines the database schema for storing YouTube video metadata and ESPN event information. It includes tables, indexes, functions, and triggers to manage and maintain data integrity and consistency.

---

## YouTube-Related Database Objects

### Tables

#### 1. `yt_videos_to_be_processed`

- **Purpose**: Stores YouTube video IDs that need to be processed for metadata extraction.
- **Columns**:
  - `video_id` (`VARCHAR(255)`, Primary Key): The unique identifier of the YouTube video.
  - `has_failed_metadata` (`BOOLEAN`, Default `FALSE`): Indicates if metadata retrieval has failed for this video.

#### 2. `yt_metadata`

- **Purpose**: Stores detailed metadata for YouTube videos.
- **Columns**:
  - `video_id` (`VARCHAR(255)`, Primary Key): The unique identifier of the YouTube video.
  - `kind` (`VARCHAR(50)`): The kind of resource.
  - `etag` (`VARCHAR(255)`): ETag of the resource.
  - `title` (`TEXT`): Title of the video.
  - `description` (`TEXT`): Description of the video.
  - `published_at` (`TIMESTAMP WITH TIME ZONE`): Publication date and time.
  - `channel_id` (`VARCHAR(255)`): ID of the channel that published the video.
  - `channel_title` (`VARCHAR(255)`): Title of the channel.
  - `category_id` (`VARCHAR(50)`): Category ID of the video.
  - `live_broadcast_content` (`VARCHAR(50)`): Live broadcast content status.
  - `default_language` (`VARCHAR(10)`): Default language of the video.
  - `default_audio_language` (`VARCHAR(10)`): Default audio language.
  - `duration` (`INTERVAL`): Duration of the video.
  - `dimension` (`VARCHAR(20)`): Dimension of the video (e.g., `2d`, `3d`).
  - `definition` (`VARCHAR(20)`): Definition quality (e.g., `hd`, `sd`).
  - `caption` (`VARCHAR(20)`): Caption status.
  - `licensed_content` (`BOOLEAN`): Indicates if the content is licensed.
  - `projection` (`VARCHAR(20)`): Projection format.
  - `upload_status`, `privacy_status`, `license` (`VARCHAR(50)`): Various status fields.
  - `embeddable`, `public_stats_viewable`, `made_for_kids` (`BOOLEAN`): Boolean flags.
  - `view_count`, `like_count`, `dislike_count`, `favorite_count`, `comment_count` (`BIGINT`): Statistics counters.
  - `event_date_local_time` (`DATE`): Local date of the event.
  - `created_at`, `modified_at`, `deleted_at` (`TIMESTAMP WITH TIME ZONE`): Timestamps for record management.

#### 3. `yt_tags`

- **Purpose**: Stores tags associated with YouTube videos.
- **Columns**:
  - `tag_id` (`SERIAL`, Primary Key): Auto-incrementing tag ID.
  - `video_id` (`VARCHAR(255)`, Foreign Key): The video ID this tag belongs to.
  - `tag` (`TEXT`): The tag text.
  - `created_at`, `modified_at`, `deleted_at` (`TIMESTAMP WITH TIME ZONE`): Timestamps.

- **Constraints**:
  - Unique constraint on `(video_id, tag)` to prevent duplicate tags for a video.

#### 4. `yt_thumbnails`

- **Purpose**: Stores thumbnail information for YouTube videos.
- **Columns**:
  - `thumbnail_id` (`SERIAL`, Primary Key): Auto-incrementing thumbnail ID.
  - `video_id` (`VARCHAR(255)`, Foreign Key): The video ID this thumbnail belongs to.
  - `thumbnail_size` (`VARCHAR(20)`): Size category of the thumbnail (e.g., `default`, `medium`, `high`).
  - `url` (`TEXT`): URL of the thumbnail image.
  - `width`, `height` (`INTEGER`): Dimensions of the thumbnail.
  - `created_at`, `modified_at`, `deleted_at` (`TIMESTAMP WITH TIME ZONE`): Timestamps.

- **Constraints**:
  - Unique constraint on `(video_id, thumbnail_size)`.

#### 5. `yt_localized_info`

- **Purpose**: Stores localized titles and descriptions for YouTube videos.
- **Columns**:
  - `localized_id` (`SERIAL`, Primary Key): Auto-incrementing ID.
  - `video_id` (`VARCHAR(255)`, Foreign Key): The video ID this localization belongs to.
  - `language` (`VARCHAR(10)`): Language code (e.g., `en`, `es`).
  - `title`, `description` (`TEXT`): Localized title and description.
  - `created_at`, `modified_at`, `deleted_at` (`TIMESTAMP WITH TIME ZONE`): Timestamps.

- **Constraints**:
  - Unique constraint on `(video_id, language)`.

#### 6. `yt_topic_categories`

- **Purpose**: Stores topic categories associated with YouTube videos.
- **Columns**:
  - `topic_id` (`SERIAL`, Primary Key): Auto-incrementing topic ID.
  - `video_id` (`VARCHAR(255)`, Foreign Key): The video ID this category belongs to.
  - `category` (`TEXT`): Topic category.
  - `created_at`, `modified_at`, `deleted_at` (`TIMESTAMP WITH TIME ZONE`): Timestamps.

- **Constraints**:
  - Unique constraint on `(video_id, category)`.

#### 7. `yt_content_rating`

- **Purpose**: Stores content rating information for YouTube videos.
- **Columns**:
  - `rating_id` (`SERIAL`, Primary Key): Auto-incrementing rating ID.
  - `video_id` (`VARCHAR(255)`, Foreign Key): The video ID this rating belongs to.
  - `rating_type` (`VARCHAR(50)`): Type of rating (e.g., `mpaa`, `tvpg`).
  - `rating_value` (`VARCHAR(50)`): Rating value (e.g., `PG-13`, `TV-MA`).
  - `created_at`, `modified_at`, `deleted_at` (`TIMESTAMP WITH TIME ZONE`): Timestamps.

- **Constraints**:
  - Unique constraint on `(video_id, rating_type)`.

#### 8. `yt_recording_details`

- **Purpose**: Stores recording details for YouTube videos.
- **Columns**:
  - `recording_id` (`SERIAL`, Primary Key): Auto-incrementing recording ID.
  - `video_id` (`VARCHAR(255)`, Foreign Key): The video ID this recording detail belongs to.
  - `recording_date` (`DATE`): Date of recording.
  - `recording_location` (`TEXT`): Location of recording.
  - `created_at`, `modified_at`, `deleted_at` (`TIMESTAMP WITH TIME ZONE`): Timestamps.

- **Constraints**:
  - Unique constraint on `(video_id, recording_date, recording_location)`.

#### 9. `yt_transcripts`

- **Purpose**: Stores transcripts for YouTube videos.
- **Columns**:
  - `transcript_id` (`SERIAL`, Primary Key): Auto-incrementing transcript ID.
  - `video_id` (`VARCHAR(255)`, Foreign Key): The video ID this transcript belongs to.
  - `transcript_text` (`TEXT`): The transcript content.
  - `language` (`VARCHAR(10)`): Language code.
  - `is_auto_generated` (`BOOLEAN`): Indicates if the transcript was auto-generated.
  - `created_at`, `modified_at`, `deleted_at` (`TIMESTAMP WITH TIME ZONE`): Timestamps.

- **Constraints**:
  - Unique constraint on `(video_id, language)`.

#### 10. `yt_video_file`

- **Purpose**: Stores information about downloaded video files.
- **Columns**:
  - `video_file_id` (`SERIAL`, Primary Key): Auto-incrementing file ID.
  - `video_id` (`VARCHAR(255)`, Foreign Key): The video ID this file belongs to.
  - `a_format_id` (`VARCHAR(10)`): Audio format ID.
  - `file_size` (`INTEGER`, Default `0`): Size of the file in bytes.
  - `local_path` (`VARCHAR(255)`): Local file system path to the video file.
  - `created_at`, `modified_at`, `deleted_at` (`TIMESTAMP WITH TIME ZONE`): Timestamps.

- **Constraints**:
  - Unique constraint on `(video_id, a_format_id)`.

### Indexes

Indexes are created on frequently queried columns to improve query performance.

- Examples:
  - `idx_yt_metadata_published_at` on `yt_metadata(published_at)`.
  - `idx_yt_tags_video_id` on `yt_tags(video_id)`.

### Functions and Triggers

#### Function: `update_modified_at_column`

- **Purpose**: Automatically updates the `modified_at` timestamp whenever a record is updated.
- **Usage**: Attached to tables via triggers to ensure the `modified_at` column reflects the last modification time.

#### Trigger: `update_*_modified_at`

- **Purpose**: Invokes `update_modified_at_column` before updating a record in the associated table.
- **Tables Applied To**:
  - `yt_metadata`
  - `yt_tags`
  - `yt_thumbnails`
  - `yt_localized_info`
  - `yt_topic_categories`
  - `yt_content_rating`
  - `yt_recording_details`
  - `yt_transcripts`
  - `yt_video_file`

#### Function: `propagate_soft_delete_to_related_tables`

- **Purpose**: When a video is soft-deleted (i.e., `deleted_at` is set), this function propagates the soft delete to all related tables.
- **Usage**: Ensures data consistency by marking related records as deleted.

#### Trigger: `propagate_yt_metadata_soft_delete`

- **Purpose**: Calls `propagate_soft_delete_to_related_tables` after a video is soft-deleted in `yt_metadata`.

#### Function: `delete_from_yt_videos_to_be_processed`

- **Purpose**: Removes a video ID from `yt_videos_to_be_processed` after it has been successfully inserted into `yt_metadata`.
- **Usage**: Prevents reprocessing of videos that have already been processed.

#### Trigger: `after_yt_metadata_insert`

- **Purpose**: Calls `delete_from_yt_videos_to_be_processed` after a new record is inserted into `yt_metadata`.

---

## ESPN-Related Database Objects

### Table: `e_events`

- **Purpose**: Stores event information fetched from ESPN.
- **Columns**:
  - `event_id` (`VARCHAR(20)`, Primary Key): Unique identifier for the event.
  - `date` (`TIMESTAMP WITH TIME ZONE`): Date and time of the event.
  - `type` (`INTEGER`): Event type.
  - `short_name` (`VARCHAR(12)`): Short name of the event.
  - `home_team`, `away_team` (`VARCHAR(7)`): Abbreviations of the teams.
  - `home_team_normalized`, `away_team_normalized` (`VARCHAR(7)`): Normalized team names.
  - `created_at`, `modified_at`, `deleted_at` (`TIMESTAMP WITH TIME ZONE`): Timestamps.

### Trigger: `update_e_events_modified_at`

- **Purpose**: Updates the `modified_at` timestamp before updating a record in `e_events`.

---

## Permissions

- Grants all privileges on all tables in the `public` schema to the user `itguy`.
- Grants usage and select permissions on all sequences in the `public` schema to `itguy`.
- **Note**: Adjust permissions as needed based on your database security requirements.

---

## Notes and Best Practices

- **Soft Deletes**: Instead of deleting records permanently, the schema uses a `deleted_at` timestamp to mark records as deleted. This practice retains historical data and allows for data recovery if needed.
- **Timestamps**: The `created_at` and `modified_at` columns are automatically managed via triggers and functions to reflect the record's lifecycle.
- **Data Integrity**: Foreign key constraints and unique indexes ensure referential integrity and prevent duplicate entries.
- **Extensibility**: The schema is designed to be extensible. New tables or columns can be added as needed, following the established conventions.
- **Consistency**: By normalizing data (e.g., storing tags, thumbnails, and localized info in separate tables), the schema avoids redundancy and maintains data consistency.

---

## Conclusion

The `db_schema.sql` script sets up a comprehensive database schema for managing YouTube video metadata and ESPN event data. It ensures data integrity through the use of constraints, triggers, and functions. The schema is optimized for performance with appropriate indexing and is designed with best practices like soft deletes and automatic timestamp management.

By following this documentation and the improved in-line comments, developers and database administrators can better understand the structure and functionality of the database, facilitating maintenance and future development.
