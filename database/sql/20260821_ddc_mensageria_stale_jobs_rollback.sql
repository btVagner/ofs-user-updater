-- Demanda 02 - rollback de schema
-- IMPORTANTE: reverta primeiro o código da Demanda 02 e somente depois execute este SQL.

ALTER TABLE ddc_mensageria_jobs
    DROP COLUMN abandoned_reason,
    DROP COLUMN abandoned_at,
    DROP COLUMN heartbeat_at;
