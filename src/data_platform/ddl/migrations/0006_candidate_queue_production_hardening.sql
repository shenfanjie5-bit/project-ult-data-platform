CREATE UNIQUE INDEX IF NOT EXISTS candidate_queue_ex3_delta_id_key
ON data_platform.candidate_queue (
    submitted_by,
    payload_type,
    (payload->>'delta_id')
)
WHERE payload_type = 'Ex-3'
  AND payload ? 'delta_id';
