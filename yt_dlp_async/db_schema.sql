-- """ Youtube related databse objects """
-- Create the videos_to_be_processed table
CREATE TABLE yt_videos_to_be_processed (
    video_id VARCHAR(255) PRIMARY KEY,
    has_failed_metadata BOOLEAN DEFAULT FALSE,
);

-- Create the yt_metadata table
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
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- Create the yt_topic_categories table
CREATE TABLE yt_tags (
    tag_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    tag TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (video_id) REFERENCES yt_metadata(video_id),
    CONSTRAINT unique_video_tags UNIQUE (video_id, tag)  -- Unique constraint
);


-- Create the yt_thumbnails table
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
    CONSTRAINT unique_video_thumbnails UNIQUE (video_id, thumbnail_size)  -- Unique constraint
);

-- Create the yt_localized_info table
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
    CONSTRAINT unique_video_localized UNIQUE (video_id, language)  -- Unique constraint
);

-- Create the yt_topic_categories table
CREATE TABLE yt_topic_categories (
    topic_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    category TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (video_id) REFERENCES yt_metadata(video_id),
    CONSTRAINT unique_video_topic_categories UNIQUE (video_id, category)  -- Unique constraint
);

-- Create the yt_content_rating table
CREATE TABLE yt_content_rating (
    rating_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    rating_type VARCHAR(50),
    rating_value VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (video_id) REFERENCES yt_metadata(video_id),
    CONSTRAINT unique_video_rating UNIQUE (video_id, rating_type)  -- Unique constraint
);

-- Create the yt_recording_details table
CREATE TABLE yt_recording_details (
    recording_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    recording_date DATE,
    recording_location TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (video_id) REFERENCES yt_metadata(video_id),
    CONSTRAINT unique_video_recording_details UNIQUE (video_id, recording_date, recording_location)  -- Unique constraint
);

-- Create the yt_transcripts table
CREATE TABLE yt_transcripts (
    transcript_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    transcript_text TEXT,
    language VARCHAR(10),
    is_auto_generated BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (video_id) REFERENCES yt_metadata(video_id),
    CONSTRAINT unique_video_transcripts UNIQUE (video_id, language)  -- Unique constraint
);

-- Create the video_file table
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
    CONSTRAINT unique_video_file UNIQUE (video_id, a_format_id)  -- Unique constraint
);

-- Create indexes
CREATE INDEX idx_yt_metadata_published_at ON yt_metadata(published_at);
CREATE INDEX idx_yt_etadata_channel_id ON yt_metadata(channel_id);
CREATE INDEX idx_yt_tags_video_id ON yt_tags(video_id);
CREATE INDEX idx_yt_thumbnails_video_id ON yt_thumbnails(video_id);
CREATE INDEX idx_yt_localized_info_video_id ON yt_localized_info(video_id);
CREATE INDEX idx_yt_topic_categories_video_id ON yt_topic_categories(video_id);
CREATE INDEX idx_yt_content_rating_video_id ON yt_content_rating(video_id);
CREATE INDEX idx_yt_recording_details_video_id ON yt_recording_details(video_id);
CREATE INDEX idx_yt_transcripts_video_id ON yt_transcripts(video_id);
CREATE INDEX idx_yt_video_file_video_id ON yt_video_file(video_id);

-- Create custom contstraint indexes
CREATE UNIQUE INDEX yt_tags_video_id_category_key ON yt_tags(video_id, tag);  -- Ensuring uniqueness for (video_id, category)
CREATE UNIQUE INDEX yt_thumbnails_video_id_size_key ON yt_thumbnails(video_id, thumbnail_size);  -- Ensuring uniqueness for (video_id, thumbnail_size)
CREATE UNIQUE INDEX yt_localized_video_id_language_key ON yt_localized_info(video_id, language);  -- Ensuring uniqueness for (video_id, language)
CREATE UNIQUE INDEX yt_topic_categories_video_id_category_key ON yt_topic_categories(video_id, category);  -- Ensuring uniqueness for (video_id, category)
CREATE UNIQUE INDEX yt_content_rating_video_id_rating_key ON yt_content_rating(video_id, rating_type);  -- Ensuring uniqueness for (video_id, rating_type)
CREATE UNIQUE INDEX yt_recording_details_video_id_key ON yt_recording_details(video_id, recording_date, recording_location);  -- Ensuring uniqueness for (video_id, recording_date, recording_location)
CREATE UNIQUE INDEX yt_transcripts_video_id_language_key ON yt_transcripts(video_id, language);  -- Ensuring uniqueness for (video_id, language)
CREATE UNIQUE INDEX yt_transcripts_video_id_format_key ON yt_transcripts(video_id, a_format_id);  -- Ensuring uniqueness for (video_id, a_format_id)

-- Trigger function to update modified_at column
CREATE OR REPLACE FUNCTION update_modified_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to call the update modified_at function on update
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

CREATE TRIGGER update_yt_content_rating_modified_at
BEFORE UPDATE ON yt_content_rating
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_yt_recording_details_modified_at
BEFORE UPDATE ON yt_recording_details
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_yt_transcripts_video_modified_at
BEFORE UPDATE ON yt_transcripts
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_yt_video_file_modified_at
BEFORE UPDATE ON yt_video_file
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

-- Trigger function to propagate soft delete to related tables
CREATE OR REPLACE FUNCTION propagate_soft_delete_to_related_tables()
RETURNS TRIGGER AS $$
DECLARE
    tables TEXT[] := ARRAY['yt_tags', 'yt_thumbnails', 'yt_localized_info', 'yt_topic_categories', 'yt_content_rating', 'yt_recording_details', 'yt_transcripts', 'yt_video_file'];
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

-- Trigger to call the soft delete function on update
CREATE TRIGGER propagate_yt_metadata_soft_delete
AFTER UPDATE ON yt_metadata
FOR EACH ROW
WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
EXECUTE FUNCTION propagate_soft_delete_to_related_tables();

-- Trigger function to remove video_id from videos_to_be_processed
CREATE OR REPLACE FUNCTION delete_from_yt_videos_to_be_processed() RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM yt_videos_to_be_processed WHERE video_id = NEW.video_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to call the delete function on insert
CREATE TRIGGER after_yt_metadata_insert
AFTER INSERT ON yt_metadata
FOR EACH ROW
EXECUTE FUNCTION delete_from_yt_videos_to_be_processed();


-- """ ESPN related databse objects """
-- Create the topic_categories table
CREATE TABLE e_events (
    event_id VARCHAR(20) PRIMARY KEY,
    date TIMESTAMP WITH TIME ZONE,
    type INTEGER,
    short_name VARCHAR(12),
    home_team VARCHAR(4),
    away_team VARCHAR(4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT unique_e_events UNIQUE (event_id)  -- Unique constraint
);

-- Trigger to call the update modified_at function on update
CREATE TRIGGER update_e_events_modified_at
BEFORE UPDATE ON e_events
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();


-- """ Final global permissioning """
-- Grant necessary permissions (adjust as needed)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO itguy;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO itguy;