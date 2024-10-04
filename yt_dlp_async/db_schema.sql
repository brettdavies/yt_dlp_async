-- ================================================
-- Database Schema for YouTube-related Data Storage
-- ================================================

-- ================================================
-- 1. YouTube-related Database Objects
-- ================================================

-- -----------------------------------------------
-- 1.1. Tables
-- -----------------------------------------------

-- 1.1.1. Table: yt_videos_to_be_processed
-- Stores YouTube video IDs that need to be processed for metadata.
CREATE TABLE yt_videos_to_be_processed (
    video_id VARCHAR(255) PRIMARY KEY,
    has_failed_metadata BOOLEAN DEFAULT FALSE
);

-- 1.1.2. Table: yt_metadata
-- Stores metadata for YouTube videos.
CREATE TABLE yt_metadata (
    video_id VARCHAR(255) PRIMARY KEY,
    kind VARCHAR(50),
    etag VARCHAR(255),
    title TEXT,
    description TEXT,
    published_at TIMESTAMP WITH TIME ZONE,
    channel_id VARCHAR(255),
    channel_title VARCHAR(255),
    category_id VARCHAR(50),
    live_broadcast_content VARCHAR(50),
    default_language VARCHAR(10),
    default_audio_language VARCHAR(10),
    duration INTERVAL,
    dimension VARCHAR(20),
    definition VARCHAR(20),
    caption VARCHAR(20),
    licensed_content BOOLEAN,
    projection VARCHAR(20),
    upload_status VARCHAR(50),
    privacy_status VARCHAR(50),
    license VARCHAR(50),
    embeddable BOOLEAN,
    public_stats_viewable BOOLEAN,
    made_for_kids BOOLEAN,
    view_count BIGINT,
    like_count BIGINT,
    dislike_count BIGINT,
    favorite_count BIGINT,
    comment_count BIGINT,
    event_date_local_time DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- 1.1.3. Table: yt_tags
-- Stores tags associated with YouTube videos.
CREATE TABLE yt_tags (
    tag_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    tag TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (video_id) REFERENCES yt_metadata(video_id),
    CONSTRAINT unique_video_tags UNIQUE (video_id, tag)
);

-- 1.1.4. Table: yt_thumbnails
-- Stores thumbnail information for YouTube videos.
CREATE TABLE yt_thumbnails (
    thumbnail_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    thumbnail_size VARCHAR(20),
    url TEXT,
    width INTEGER,
    height INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (video_id) REFERENCES yt_metadata(video_id),
    CONSTRAINT unique_video_thumbnails UNIQUE (video_id, thumbnail_size)
);

-- 1.1.5. Table: yt_localized_info
-- Stores localized titles and descriptions for YouTube videos.
CREATE TABLE yt_localized_info (
    localized_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    language VARCHAR(10),
    title TEXT,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (video_id) REFERENCES yt_metadata(video_id),
    CONSTRAINT unique_video_localized UNIQUE (video_id, language)
);

-- 1.1.6. Table: yt_topic_categories
-- Stores topic categories associated with YouTube videos.
CREATE TABLE yt_topic_categories (
    topic_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    category TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (video_id) REFERENCES yt_metadata(video_id),
    CONSTRAINT unique_video_topic_categories UNIQUE (video_id, category)
);

-- 1.1.9. Table: yt_video_file
-- Stores information about downloaded video files.
CREATE TABLE yt_video_file (
    video_file_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255),
    a_format_id VARCHAR(10),
    file_size INTEGER DEFAULT 0,
    local_path VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (video_id) REFERENCES yt_metadata(video_id),
    CONSTRAINT unique_video_file UNIQUE (video_id, a_format_id)
);

-- -----------------------------------------------
-- 1.2. Indexes
-- -----------------------------------------------

-- Indexes for yt_metadata
CREATE INDEX idx_yt_metadata_published_at ON yt_metadata(published_at);
CREATE INDEX idx_yt_metadata_channel_id ON yt_metadata(channel_id);

-- Indexes for yt_tags
CREATE INDEX idx_yt_tags_video_id ON yt_tags(video_id);

-- Indexes for yt_thumbnails
CREATE INDEX idx_yt_thumbnails_video_id ON yt_thumbnails(video_id);

-- Indexes for yt_localized_info
CREATE INDEX idx_yt_localized_info_video_id ON yt_localized_info(video_id);

-- Indexes for yt_topic_categories
CREATE INDEX idx_yt_topic_categories_video_id ON yt_topic_categories(video_id);

-- Indexes for yt_video_file
CREATE INDEX idx_yt_video_file_video_id ON yt_video_file(video_id);

-- Unique indexes to enforce constraints
CREATE UNIQUE INDEX yt_tags_video_id_tag_key ON yt_tags(video_id, tag);
CREATE UNIQUE INDEX yt_thumbnails_video_id_size_key ON yt_thumbnails(video_id, thumbnail_size);
CREATE UNIQUE INDEX yt_localized_video_id_language_key ON yt_localized_info(video_id, language);
CREATE UNIQUE INDEX yt_topic_categories_video_id_category_key ON yt_topic_categories(video_id, category);
CREATE UNIQUE INDEX yt_video_file_video_id_format_key ON yt_video_file(video_id, a_format_id);

-- -----------------------------------------------
-- 1.3. Functions and Triggers
-- -----------------------------------------------

-- 1.3.1. Function: update_modified_at_column
-- Updates the 'modified_at' column to the current timestamp before a row is updated.
CREATE OR REPLACE FUNCTION update_modified_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers to update 'modified_at' before updating records in tables
CREATE TRIGGER update_yt_metadata_modified_at
BEFORE UPDATE ON yt_metadata
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_yt_tags_modified_at
BEFORE UPDATE ON yt_tags
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_yt_thumbnails_modified_at
BEFORE UPDATE ON yt_thumbnails
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_yt_localized_info_modified_at
BEFORE UPDATE ON yt_localized_info
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_yt_topic_categories_modified_at
BEFORE UPDATE ON yt_topic_categories
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_yt_video_file_modified_at
BEFORE UPDATE ON yt_video_file
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

-- 1.3.2. Function: propagate_soft_delete_to_related_tables
-- Propagates a soft delete to related tables when a video is soft-deleted.
CREATE OR REPLACE FUNCTION propagate_soft_delete_to_related_tables()
RETURNS TRIGGER AS $$
DECLARE
    tables TEXT[] := ARRAY[
        'yt_tags',
        'yt_thumbnails',
        'yt_localized_info',
        'yt_topic_categories',
        'yt_video_file'
    ];
    table_name TEXT;
BEGIN
    IF NEW.deleted_at IS NOT NULL THEN
        FOREACH table_name IN ARRAY tables LOOP
            EXECUTE format('UPDATE %I SET deleted_at = $1 WHERE video_id = $2', table_name)
            USING NEW.deleted_at, NEW.video_id;
        END LOOP;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger: propagate soft delete on yt_metadata
CREATE TRIGGER propagate_yt_metadata_soft_delete
AFTER UPDATE ON yt_metadata
FOR EACH ROW
WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
EXECUTE FUNCTION propagate_soft_delete_to_related_tables();

-- 1.3.3. Function: delete_from_yt_videos_to_be_processed
-- Removes a video ID from 'yt_videos_to_be_processed' after it has been inserted into 'yt_metadata'.
CREATE OR REPLACE FUNCTION delete_from_yt_videos_to_be_processed()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM yt_videos_to_be_processed WHERE video_id = NEW.video_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger: after insert on yt_metadata
-- Calls 'delete_from_yt_videos_to_be_processed' after a video is inserted into 'yt_metadata'.
CREATE TRIGGER after_yt_metadata_insert
AFTER INSERT ON yt_metadata
FOR EACH ROW
EXECUTE FUNCTION delete_from_yt_videos_to_be_processed();

-- ================================================
-- 2. Permissions
-- ================================================

-- Grant necessary permissions (adjust as needed)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO itguy;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO itguy;
