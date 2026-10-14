-- Requires stopped new worker and web requests, explicit confirmation and backup.
DELETE pp FROM perfil_permissao pp JOIN permissoes pm ON pm.id=pp.permissao_id
WHERE pm.recurso IN ('ofs.os_repetidas.visualizar','ofs.os_repetidas.atualizar');
DELETE FROM permissoes WHERE recurso IN ('ofs.os_repetidas.visualizar','ofs.os_repetidas.atualizar');
DROP TABLE IF EXISTS ofs_repeated_orders_snapshot;
DROP TABLE IF EXISTS ofs_repeated_orders_run;
DROP TABLE IF EXISTS ofs_repeated_orders_control;
