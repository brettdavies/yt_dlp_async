-- Create the videos_to_be_processed table
CREATE TABLE videos_to_be_processed (
    video_id VARCHAR(255) PRIMARY KEY
);

-- Create the video_metadata table
CREATE TABLE video_metadata (
    video_id VARCHAR(255) PRIMARY KEY,
    kind VARCHAR(50),
    etag VARCHAR(255),
    title TEXT NOT NULL,
    description TEXT,
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
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
    tags TEXT[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- Create the thumbnails table
CREATE TABLE thumbnails (
    thumbnail_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    thumbnail_size VARCHAR(20) NOT NULL,
    url TEXT NOT NULL,
    width INTEGER,
    height INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
    FOREIGN KEY (video_id) REFERENCES video_metadata(video_id)
);

-- Create the localized_info table
CREATE TABLE localized_info (
    localized_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    language VARCHAR(10) NOT NULL,
    title TEXT,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
    FOREIGN KEY (video_id) REFERENCES video_metadata(video_id)
);

-- Create the topic_categories table
CREATE TABLE topic_categories (
    topic_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    category TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
    FOREIGN KEY (video_id) REFERENCES video_metadata(video_id)
);

-- Create the content_rating table
CREATE TABLE content_rating (
    rating_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    rating_type VARCHAR(50),
    rating_value VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
    FOREIGN KEY (video_id) REFERENCES video_metadata(video_id)
);

-- Create the recording_details table
CREATE TABLE recording_details (
    recording_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    recording_date DATE,
    recording_location TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
    FOREIGN KEY (video_id) REFERENCES video_metadata(video_id)
);

-- Create the transcripts table
CREATE TABLE transcripts (
    transcript_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) NOT NULL,
    transcript_text TEXT NOT NULL,
    language VARCHAR(10),
    is_auto_generated BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
    FOREIGN KEY (video_id) REFERENCES video_metadata(video_id)
);

-- Create the video_file table
CREATE TABLE video_file (
    video_id VARCHAR(255) PRIMARY KEY,
    local_path VARCHAR(255),
    file_size INTEGER DEFAULT 0,
    height INTEGER DEFAULT 0,
    width INTEGER DEFAULT 0,
    bit_rate INTEGER DEFAULT 0,    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP WITH TIME ZONE
    FOREIGN KEY (video_id) REFERENCES video_metadata(video_id)
);

-- Create indexes
CREATE INDEX idx_video_metadata_published_at ON video_metadata(published_at);
CREATE INDEX idx_video_metadata_channel_id ON video_metadata(channel_id);
CREATE INDEX idx_thumbnails_video_id ON thumbnails(video_id);
CREATE INDEX idx_localized_info_video_id ON localized_info(video_id);
CREATE INDEX idx_topic_categories_video_id ON topic_categories(video_id);
CREATE INDEX idx_content_rating_video_id ON content_rating(video_id);
CREATE INDEX idx_recording_details_video_id ON recording_details(video_id);
CREATE INDEX idx_transcripts_video_id ON transcripts(video_id);
CREATE INDEX idx_video_file_video_id ON video_file(video_id);

-- Create the view with a default limit of 1000 using LEFT JOIN
CREATE OR REPLACE VIEW videos_to_be_processed_view AS
SELECT vtbp.video_id
FROM videos_to_be_processed vtbp
LEFT JOIN video_metadata vm ON vtbp.video_id = vm.video_id
WHERE vm.video_id IS NULL
LIMIT 50;

-- Trigger function to update modified_at column
CREATE OR REPLACE FUNCTION update_modified_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to call the update modified_at function on update
CREATE TRIGGER update_video_metadata_modified_at
BEFORE UPDATE ON video_metadata
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_thumbnails_modified_at
BEFORE UPDATE ON thumbnails
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_localized_info_modified_at
BEFORE UPDATE ON localized_info
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_topic_categories_modified_at
BEFORE UPDATE ON topic_categories
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_content_rating_modified_at
BEFORE UPDATE ON content_rating
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_recording_details_modified_at
BEFORE UPDATE ON recording_details
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_transcripts_video_modified_at
BEFORE UPDATE ON transcripts_video
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

CREATE TRIGGER update_video_file_modified_at
BEFORE UPDATE ON video_file
FOR EACH ROW
EXECUTE FUNCTION update_modified_at_column();

-- Trigger function to propagate soft delete to related tables
CREATE OR REPLACE FUNCTION propagate_soft_delete_to_related_tables()
RETURNS TRIGGER AS $$
DECLARE
    tables TEXT[] := ARRAY['thumbnails', 'localized_info', 'topic_categories', 'content_rating', 'recording_details', 'transcripts_video', 'video_file'];
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
CREATE TRIGGER propagate_video_metadata_soft_delete
AFTER UPDATE ON video_metadata
FOR EACH ROW
WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
EXECUTE FUNCTION propagate_soft_delete_to_related_tables();

-- Trigger function to remove video_id from videos_to_be_processed
CREATE OR REPLACE FUNCTION delete_from_videos_to_be_processed() RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM videos_to_be_processed WHERE video_id = NEW.video_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to call the delete function on insert
CREATE TRIGGER after_video_metadata_insert
AFTER INSERT ON video_metadata
FOR EACH ROW
EXECUTE FUNCTION delete_from_videos_to_be_processed();

-- Grant necessary permissions (adjust as needed)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO admin;
