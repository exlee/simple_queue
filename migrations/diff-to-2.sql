-- v2: Add completed_at column to track when jobs finish processing
ALTER TABLE job_queue ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE job_queue_dlq ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE job_queue_archive ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
