-- Date bands of a county already saved, so a stopped county resumes instead of starting over.
ALTER TABLE offmarket_progress ADD COLUMN IF NOT EXISTS done_bands text[] NOT NULL DEFAULT '{}';
