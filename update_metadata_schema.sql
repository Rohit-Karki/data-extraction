-- Add new column for last_ingested_time
ALTER TABLE ingestion_metadata 
ADD COLUMN last_ingested_time DATETIME NULL AFTER last_ingested_id;

-- Update existing records with current time if last_ingested_id is not 0
UPDATE ingestion_metadata 
SET last_ingested_time = updated_at 
WHERE last_ingested_id != 0;

-- Set default value for last_ingested_time
ALTER TABLE ingestion_metadata 
ALTER COLUMN last_ingested_time 
SET DEFAULT CURRENT_TIMESTAMP;

-- Drop the old last_ingested_id column
ALTER TABLE ingestion_metadata 
DROP COLUMN last_ingested_id;

-- Reorder columns if needed
ALTER TABLE ingestion_metadata 
CHANGE COLUMN last_ingested_time last_ingested_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER is_running;
