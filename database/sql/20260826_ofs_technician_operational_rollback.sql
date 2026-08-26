-- Demanda 06 - rollback de schema.
-- Pare o worker e reverta o código antes de executar.
DROP TABLE IF EXISTS ofs_operational_sync_state;
DROP TABLE IF EXISTS ofs_event_cursor;
DROP TABLE IF EXISTS ofs_activity_operational_state;
DROP TABLE IF EXISTS ofs_technician_operational_state;
