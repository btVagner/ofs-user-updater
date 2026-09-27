-- Remove somente a extensão de UF. Não altera atividades nem tratativas.

SET @schema_name = DATABASE();

SET @ddl = IF(
  EXISTS(
    SELECT 1
    FROM information_schema.statistics
    WHERE table_schema=@schema_name
      AND table_name='ofs_atividades_notdone'
      AND index_name='idx_notdone_date_state_treated'
  ),
  'ALTER TABLE ofs_atividades_notdone DROP INDEX idx_notdone_date_state_treated',
  'DO 0'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @ddl = IF(
  EXISTS(
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema=@schema_name
      AND table_name='ofs_atividades_notdone'
      AND column_name='state_province'
  ),
  'ALTER TABLE ofs_atividades_notdone DROP COLUMN state_province',
  'DO 0'
);
PREPARE stmt FROM @ddl; EXECUTE stmt; DEALLOCATE PREPARE stmt;
