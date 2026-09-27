SELECT column_name,column_type,is_nullable,column_default
FROM information_schema.columns
WHERE table_schema=DATABASE()
  AND table_name='ofs_atividades_notdone'
  AND column_name='state_province';

SELECT index_name,GROUP_CONCAT(column_name ORDER BY seq_in_index) AS indexed_columns
FROM information_schema.statistics
WHERE table_schema=DATABASE()
  AND table_name='ofs_atividades_notdone'
  AND index_name='idx_notdone_date_state_treated'
GROUP BY index_name;

SELECT
  COUNT(*) AS total_rows,
  SUM(state_province IS NOT NULL AND TRIM(state_province) <> '') AS rows_with_state,
  COUNT(DISTINCT NULLIF(TRIM(state_province), '')) AS distinct_states
FROM ofs_atividades_notdone;
