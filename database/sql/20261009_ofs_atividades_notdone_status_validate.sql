SELECT column_name, column_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'ofs_atividades_notdone'
  AND column_name = 'activity_status';

SELECT activity_status, COUNT(*) AS total
FROM ofs_atividades_notdone
GROUP BY activity_status
ORDER BY activity_status;
