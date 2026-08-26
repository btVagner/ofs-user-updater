-- Demanda 06 - validação do read model operacional
SELECT TABLE_NAME, ENGINE, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN (
    'ofs_technician_operational_state',
    'ofs_activity_operational_state',
    'ofs_event_cursor',
    'ofs_operational_sync_state'
  )
ORDER BY TABLE_NAME;

SELECT TABLE_NAME, INDEX_NAME,
       GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS indexed_columns
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('ofs_technician_operational_state','ofs_activity_operational_state')
GROUP BY TABLE_NAME, INDEX_NAME
ORDER BY TABLE_NAME, INDEX_NAME;

SELECT work_date, COUNT(*) AS technicians,
       SUM(pending_count) AS pending,
       SUM(enroute_count) AS enroute,
       SUM(started_count) AS started,
       SUM(suspended_count) AS suspended,
       SUM(completed_count) AS completed,
       SUM(notdone_count) AS notdone,
       SUM(cancelled_count) AS cancelled,
       SUM(open_activity_count) AS open_activities,
       MAX(updated_at) AS updated_at
FROM ofs_technician_operational_state
GROUP BY work_date
ORDER BY work_date DESC;

SELECT work_date, status, COUNT(*) AS activities
FROM ofs_activity_operational_state
GROUP BY work_date, status
ORDER BY work_date DESC, status;

SELECT cursor_key,
       CHAR_LENGTH(subscription_id) AS subscription_id_length,
       CHAR_LENGTH(next_page) AS next_page_length,
       subscription_created_at, baseline_completed_at,
       last_poll_success_at, last_event_at,
       last_error_code, updated_at
FROM ofs_event_cursor;

SELECT source_name, last_started_at, last_success_at, last_finished_at,
       status, error_code, error_message, updated_at
FROM ofs_operational_sync_state
ORDER BY source_name;

-- Retenção esperada: nenhuma linha anterior a CURDATE()-6 dias.
SELECT 'technician' AS model, MIN(work_date) AS min_date, MAX(work_date) AS max_date, COUNT(*) AS rows_total
FROM ofs_technician_operational_state
UNION ALL
SELECT 'activity', MIN(work_date), MAX(work_date), COUNT(*)
FROM ofs_activity_operational_state;
