-- Demanda 10 - validação final de hardening do read model operacional.
-- Somente leitura. Não altera schema nem dados.
-- Não retorna nextPage, subscriptionId, credenciais ou payloads funcionais.

-- 1) Volume atual e janela de retenção. A janela válida é hoje .. hoje-6 dias.
SELECT
    'technician_state' AS dataset,
    COUNT(*) AS rows_total,
    COUNT(DISTINCT work_date) AS dates_total,
    MIN(work_date) AS min_work_date,
    MAX(work_date) AS max_work_date,
    SUM(work_date < DATE_SUB(CURDATE(), INTERVAL 6 DAY)) AS expired_rows,
    SUM(work_date > CURDATE()) AS future_rows
FROM ofs_technician_operational_state
UNION ALL
SELECT
    'activity_state' AS dataset,
    COUNT(*) AS rows_total,
    COUNT(DISTINCT work_date) AS dates_total,
    MIN(work_date) AS min_work_date,
    MAX(work_date) AS max_work_date,
    SUM(work_date < DATE_SUB(CURDATE(), INTERVAL 6 DAY)) AS expired_rows,
    SUM(work_date > CURDATE()) AS future_rows
FROM ofs_activity_operational_state;

-- 2) Distribuição diária para estimativa de crescimento em sete dias.
SELECT work_date, COUNT(*) AS technician_rows
FROM ofs_technician_operational_state
GROUP BY work_date
ORDER BY work_date;

SELECT work_date, COUNT(*) AS activity_rows
FROM ofs_activity_operational_state
GROUP BY work_date
ORDER BY work_date;

-- 3) Hierarquia é estado atual, não histórico infinito.
SELECT
    root_resource_id,
    COUNT(*) AS hierarchy_rows,
    COUNT(DISTINCT resource_id) AS distinct_resources,
    MAX(depth) AS max_depth,
    MIN(last_seen_at) AS oldest_last_seen_at,
    MAX(last_seen_at) AS newest_last_seen_at
FROM ofs_resource_hierarchy
GROUP BY root_resource_id
ORDER BY root_resource_id;

-- 4) Health/freshness persistido por fonte. Routes não possui threshold temporal minuto a minuto.
SELECT
    source_name,
    status,
    last_started_at,
    last_success_at,
    last_finished_at,
    TIMESTAMPDIFF(SECOND, last_success_at, UTC_TIMESTAMP(6)) AS age_seconds,
    error_code,
    updated_at
FROM ofs_operational_sync_state
WHERE source_name IN ('events', 'activities', 'calendars', 'routes')
ORDER BY source_name;

-- 5) Cursor/subscription: somente presença/metadados; não expõe valores opacos.
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
FROM ofs_event_cursor;

-- 6) Tamanho físico aproximado das tabelas relevantes (InnoDB).
SELECT
    TABLE_NAME,
    TABLE_ROWS,
    DATA_LENGTH,
    INDEX_LENGTH,
    DATA_LENGTH + INDEX_LENGTH AS total_bytes
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN (
      'ofs_resource_hierarchy',
      'ofs_technician_operational_state',
      'ofs_activity_operational_state',
      'ofs_event_cursor',
      'ofs_operational_sync_state'
  )
ORDER BY TABLE_NAME;

-- 7) PKs e índices efetivos.
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
