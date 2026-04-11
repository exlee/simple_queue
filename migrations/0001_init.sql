
CREATE TABLE IF NOT EXISTS job_queue
(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fingerprint TEXT NULL,
    unique_key TEXT NULL,
    queue VARCHAR(255) NOT NULL,
    job_data JSONB,
    status VARCHAR(255) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    attempt INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_queue_unique_active
ON job_queue (unique_key)
WHERE unique_key IS NOT NULL
AND status IN ('pending', 'running');

CREATE INDEX IF NOT EXISTS idx_job_queue_fingerprint_active
ON job_queue (fingerprint)
WHERE status IN ('pending', 'running');
