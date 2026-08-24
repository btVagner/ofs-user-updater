-- Demanda 02 - validação pós-migration

SELECT
    COLUMN_NAME,
    COLUMN_TYPE,
    IS_NULLABLE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'ddc_mensageria_jobs'
  AND COLUMN_NAME IN ('heartbeat_at', 'abandoned_at', 'abandoned_reason')
ORDER BY ORDINAL_POSITION;

-- Esperado: 3 linhas.

SELECT
    status,
    COUNT(*) AS quantidade
FROM ddc_mensageria_jobs
GROUP BY status
ORDER BY status;

-- Após o código novo entrar em operação, jobs recuperados automaticamente
-- ficam auditáveis como status='abandoned'.
SELECT
    id,
    job_uuid,
    status,
    created_at,
    started_at,
    heartbeat_at,
    finished_at,
    abandoned_at,
    abandoned_reason
FROM ddc_mensageria_jobs
WHERE status IN ('pending', 'running', 'abandoned')
ORDER BY id DESC
LIMIT 20;
