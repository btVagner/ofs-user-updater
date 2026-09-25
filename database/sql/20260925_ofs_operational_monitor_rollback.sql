DELETE pp
FROM perfil_permissao pp
JOIN permissoes pm ON pm.id=pp.permissao_id
WHERE pm.recurso='ofs.monitor_operacional';

DELETE FROM permissoes WHERE recurso='ofs.monitor_operacional';

DROP TABLE IF EXISTS ofs_operational_monitor_refresh_log;
DROP TABLE IF EXISTS ofs_operational_monitor_snapshot;

ALTER TABLE ofs_activity_operational_state
    DROP COLUMN customer_name,
    DROP COLUMN is_black,
    DROP COLUMN time_slot,
    DROP COLUMN duration_minutes,
    DROP COLUMN start_time,
    DROP COLUMN record_type;

