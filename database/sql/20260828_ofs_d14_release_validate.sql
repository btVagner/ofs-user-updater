-- Demanda 14 - validacao final read-only para liberacao de producao.
-- Nao altera dados nem schema.

-- 1) Health operacional + estrutural. hierarchy deve estar recente e status=ok.
SELECT
    source_name,
    status,
    last_started_at,
    last_success_at,
    last_finished_at,
    TIMESTAMPDIFF(SECOND, last_success_at, UTC_TIMESTAMP(6)) AS age_seconds,
    error_code
FROM ofs_operational_sync_state
WHERE source_name IN ('events','activities','calendars','routes','hierarchy')
ORDER BY source_name;

-- 2) Retencao de 7 dias: expired_rows e future_rows devem ser zero.
SELECT
    'ofs_technician_operational_state' AS dataset,
    COUNT(*) AS rows_total,
    COUNT(DISTINCT work_date) AS dates_total,
    MIN(work_date) AS min_work_date,
    MAX(work_date) AS max_work_date,
    SUM(work_date < DATE_SUB(CURDATE(), INTERVAL 6 DAY)) AS expired_rows,
    SUM(work_date > CURDATE()) AS future_rows
FROM ofs_technician_operational_state
UNION ALL
SELECT
    'ofs_activity_operational_state' AS dataset,
    COUNT(*) AS rows_total,
    COUNT(DISTINCT work_date) AS dates_total,
    MIN(work_date) AS min_work_date,
    MAX(work_date) AS max_work_date,
    SUM(work_date < DATE_SUB(CURDATE(), INTERVAL 6 DAY)) AS expired_rows,
    SUM(work_date > CURDATE()) AS future_rows
FROM ofs_activity_operational_state;

-- 3) Hierarquia representa somente snapshot atual: resource_id unico e um batch last_seen corrente.
SELECT
    root_resource_id,
    COUNT(*) AS rows_total,
    COUNT(DISTINCT resource_id) AS distinct_resources,
    COUNT(DISTINCT last_seen_at) AS distinct_last_seen_batches,
    MIN(last_seen_at) AS min_last_seen_at,
    MAX(last_seen_at) AS max_last_seen_at,
    MAX(depth) AS max_depth
FROM ofs_resource_hierarchy
GROUP BY root_resource_id
ORDER BY root_resource_id;

-- 4) PKs/indices das tabelas do monitor/hierarquia para revisao final.
SELECT
    TABLE_NAME,
    INDEX_NAME,
    NON_UNIQUE,
    SEQ_IN_INDEX,
    COLUMN_NAME,
    CARDINALITY
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN (
      'ofs_resource_hierarchy',
      'ofs_technician_operational_state',
      'ofs_activity_operational_state',
      'ofs_event_cursor',
      'ofs_operational_sync_state'
  )
ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;

-- 5) Cursor existe sem expor seu conteudo opaco.
SELECT
    cursor_key,
    (subscription_id IS NOT NULL AND subscription_id <> '') AS has_subscription,
    (next_page IS NOT NULL AND next_page <> '') AS has_next_page,
    CHAR_LENGTH(next_page) AS next_page_length,
    subscription_created_at,
    baseline_completed_at,
    last_poll_success_at,
    last_event_at,
    last_error_code,
    updated_at
FROM ofs_event_cursor
ORDER BY cursor_key;
