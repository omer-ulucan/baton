-- When the attempt was claimed, for the task duration metric.
ALTER TABLE task_leases ADD COLUMN claimed_at timestamptz NOT NULL DEFAULT now();
