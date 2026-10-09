-- Preserva registros históricos, todos importados antes apenas com status notdone.
SET @schema_name = DATABASE();

SET @ddl = IF(
  EXISTS(
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = 'ofs_atividades_notdone'
      AND column_name = 'activity_status'
  ),
  'DO 0',
  'ALTER TABLE ofs_atividades_notdone ADD COLUMN activity_status VARCHAR(16) NOT NULL DEFAULT ''notdone'' AFTER activity_id'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;
