-- Executar apenas após voltar o código para uma versão que não usa activity_status.
SET @schema_name = DATABASE();

SET @ddl = IF(
  EXISTS(
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = 'ofs_atividades_notdone'
      AND column_name = 'activity_status'
  ),
  'ALTER TABLE ofs_atividades_notdone DROP COLUMN activity_status',
  'DO 0'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;
