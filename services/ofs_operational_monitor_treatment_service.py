"""Tratativas locais do monitor: nenhuma operação deste módulo chama o OFS."""

from __future__ import annotations

import secrets
import os
from datetime import date, datetime, timedelta
from typing import Callable, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from services.ofs_operational_monitor_service import MySQLOperationalMonitorRepository, operational_work_date, resolve_scope, utc_now_naive


TREAT_PERMISSION = "ofs.monitor_operacional.tratar"
SUPERVISE_PERMISSION = "ofs.monitor_operacional.supervisionar"
LEASE_SECONDS = 5 * 60
ELIGIBLE_VIEWS = {
    "late": ("late_candidates", "id"),
    "idle": ("idle", "resource_id"),
    "notStarted": ("not_started_candidates", "resource_id"),
    "slot": ("slot", "id"),
}
FINAL_STATUSES = {"resolved", "waiting", "open"}


class TreatmentError(ValueError):
    def __init__(self, code: str, message: str, http_status: int = 400, current: Optional[dict] = None):
        super().__init__(message)
        self.code = code
        self.http_status = http_status
        self.current = current


def _date_text(value):
    return value.isoformat() if isinstance(value, date) else str(value or "")


def _time_text(value):
    return value.isoformat(timespec="seconds") if isinstance(value, datetime) else None


def _serialize(row: Optional[dict], now: datetime) -> Optional[dict]:
    if not row:
        return None
    status = row["status"]
    if status == "analysis" and (not row.get("lease_expires_at") or row["lease_expires_at"] <= now):
        status = "open"
    return {
        "indicator": row["indicator"],
        "item_key": row["item_key"],
        "status": status,
        "actor_username": row.get("actor_username") if status == "analysis" else None,
        "lease_expires_at": _time_text(row.get("lease_expires_at")) if status == "analysis" else None,
        "resolved_by_username": row.get("resolved_by_username") if status == "resolved" else None,
        "resolved_at": _time_text(row.get("resolved_at")) if status == "resolved" else None,
        "note": row.get("note") or "",
        "revision": row.get("revision") or 0,
    }


class MonitorTreatmentService:
    def __init__(self, connection_factory: Optional[Callable] = None):
        if connection_factory is None:
            from database.connection import get_connection

            connection_factory = get_connection
        self.connection_factory = connection_factory

    def _snapshot_item(self, scope_key: str, work_date: str, indicator: str, item_key: str) -> dict:
        if indicator not in ELIGIBLE_VIEWS or not item_key or len(item_key) > 64:
            raise TreatmentError("INVALID_ITEM", "Indicador ou identificador inválido.")
        snapshot = MySQLOperationalMonitorRepository(connection_factory=self.connection_factory).load_snapshot(scope_key)
        payload = (snapshot or {}).get("payload") or {}
        if not snapshot or not snapshot.get("has_payload") or _date_text(snapshot.get("work_date")) != work_date:
            raise TreatmentError("STALE_SNAPSHOT", "O snapshot mudou. Atualize a tela antes de tratar este caso.", 409)
        source, field = ELIGIBLE_VIEWS[indicator]
        for row in payload.get(source) or []:
            if str(row.get(field) or "") == item_key:
                return row
        raise TreatmentError("ITEM_NOT_FOUND", "Este caso não consta mais no snapshot. Atualize a tela.", 409)

    @staticmethod
    def _identity(scope_key: str, work_date: str, indicator: str, item_key: str):
        scope = resolve_scope(scope_key)
        try:
            parsed_date = date.fromisoformat(work_date)
        except (ValueError, TypeError):
            raise TreatmentError("INVALID_DATE", "Data operacional inválida.") from None
        if indicator not in ELIGIBLE_VIEWS or not item_key or len(item_key) > 64:
            raise TreatmentError("INVALID_ITEM", "Indicador ou identificador inválido.")
        return scope["key"], parsed_date, indicator, item_key

    @staticmethod
    def _require_actor(actor: dict):
        if not actor.get("id") or not actor.get("username"):
            raise TreatmentError("INVALID_ACTOR", "Sessão sem identificação do usuário. Entre novamente.", 403)

    @staticmethod
    def _event(cur, row: dict, action: str, actor: dict, now: datetime, *, old_status: str, note: str = ""):
        cur.execute(
            """INSERT INTO ofs_operational_monitor_treatment_event
               (treatment_id,scope_key,work_date,indicator,item_key,action,old_status,new_status,
                actor_user_id,actor_username,note,occurred_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (row["id"], row["scope_key"], row["work_date"], row["indicator"], row["item_key"],
             action, old_status, row["status"], actor["id"], actor["username"], note or None, now),
        )

    def list_current(self, scope_key: str) -> dict:
        scope = resolve_scope(scope_key)
        now = utc_now_naive()
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT work_date,refreshed_at FROM ofs_operational_monitor_snapshot WHERE scope_key=%s", (scope["key"],))
            snapshot = cur.fetchone() or {}
            work_date = snapshot.get("work_date")
            if not work_date:
                return {"work_date": None, "snapshot_refreshed_at": None, "items": []}
            cur.execute(
                """SELECT indicator,item_key,status,actor_username,lease_expires_at,
                          resolved_by_username,resolved_at,note,revision
                   FROM ofs_operational_monitor_treatment
                   WHERE scope_key=%s AND work_date=%s""",
                (scope["key"], work_date),
            )
            return {"work_date": _date_text(work_date),
                    "snapshot_refreshed_at": _time_text(snapshot.get("refreshed_at")),
                    "items": [_serialize(row, now) for row in cur.fetchall()]}
        finally:
            cur.close()
            conn.close()

    def claim(self, scope_key: str, work_date: str, indicator: str, item_key: str, actor: dict) -> dict:
        self._require_actor(actor)
        identity = self._identity(scope_key, work_date, indicator, item_key)
        source = self._snapshot_item(identity[0], work_date, indicator, item_key)
        now = utc_now_naive()
        token = secrets.token_urlsafe(32)
        lease_until = now + timedelta(seconds=LEASE_SECONDS)
        state_uf = source.get("state") or ", ".join(source.get("states") or []) or "Sem UF"
        label = source.get("appt") or source.get("tech") or item_key
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            # A chave única e o SELECT FOR UPDATE serializam claims concorrentes,
            # inclusive quando o caso ainda não tinha linha de tratativa.
            cur.execute(
                """INSERT INTO ofs_operational_monitor_treatment
                   (scope_key,work_date,indicator,item_key,status,item_label,technician,area,state_uf,updated_at)
                   VALUES (%s,%s,%s,%s,'open',%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE id=id""",
                (*identity, str(label)[:255], str(source.get("tech") or "")[:255],
                 str(source.get("area") or "")[:255], str(state_uf)[:64], now),
            )
            cur.execute(
                """SELECT * FROM ofs_operational_monitor_treatment
                   WHERE scope_key=%s AND work_date=%s AND indicator=%s AND item_key=%s FOR UPDATE""",
                identity,
            )
            row = cur.fetchone()
            if row["status"] == "resolved":
                raise TreatmentError("ALREADY_RESOLVED", "Este caso já foi resolvido.", 409, _serialize(row, now))
            if row["status"] == "analysis" and row.get("lease_expires_at") and row["lease_expires_at"] > now:
                raise TreatmentError("ALREADY_CLAIMED", "Este caso já está em análise por outro agente.", 409, _serialize(row, now))
            old_status = row["status"]
            cur.execute(
                """UPDATE ofs_operational_monitor_treatment
                   SET status='analysis',actor_user_id=%s,actor_username=%s,lease_token=%s,
                       lease_expires_at=%s,revision=revision+1,updated_at=%s
                   WHERE id=%s""",
                (actor["id"], actor["username"], token, lease_until, now, row["id"]),
            )
            row.update(status="analysis", actor_user_id=actor["id"], actor_username=actor["username"],
                       lease_token=token, lease_expires_at=lease_until, revision=row["revision"] + 1)
            self._event(cur, row, "claim", actor, now, old_status=old_status)
            conn.commit()
            return {"item": _serialize(row, now), "token": token, "lease_seconds": LEASE_SECONDS}
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def change(self, scope_key: str, work_date: str, indicator: str, item_key: str,
               actor: dict, token: str, action: str, note: str = "") -> dict:
        self._require_actor(actor)
        identity = self._identity(scope_key, work_date, indicator, item_key)
        if not token or len(token) > 128:
            raise TreatmentError("INVALID_TOKEN", "Reserva inválida. Abra a tratativa novamente.")
        if action not in FINAL_STATUSES | {"renew", "release"}:
            raise TreatmentError("INVALID_ACTION", "Ação de tratativa inválida.")
        note = str(note or "").strip()
        if len(note) > 500:
            raise TreatmentError("INVALID_NOTE", "A observação deve ter até 500 caracteres.")
        now = utc_now_naive()
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """SELECT * FROM ofs_operational_monitor_treatment
                   WHERE scope_key=%s AND work_date=%s AND indicator=%s AND item_key=%s FOR UPDATE""",
                identity,
            )
            row = cur.fetchone()
            if (not row or row["status"] != "analysis" or row.get("actor_user_id") != actor["id"]
                    or not secrets.compare_digest(str(row.get("lease_token") or ""), token)
                    or not row.get("lease_expires_at") or row["lease_expires_at"] <= now):
                raise TreatmentError("CLAIM_LOST", "A reserva expirou ou mudou de agente. Atualize a tela.", 409, _serialize(row, now))
            if action == "renew":
                until = now + timedelta(seconds=LEASE_SECONDS)
                cur.execute("UPDATE ofs_operational_monitor_treatment SET lease_expires_at=%s,updated_at=%s WHERE id=%s",
                            (until, now, row["id"]))
                row["lease_expires_at"] = until
                conn.commit()
                return {"item": _serialize(row, now), "token": token, "lease_seconds": LEASE_SECONDS}
            status = "open" if action == "release" else action
            cur.execute(
                """UPDATE ofs_operational_monitor_treatment
                   SET status=%s,actor_user_id=NULL,actor_username=NULL,lease_token=NULL,
                       lease_expires_at=NULL,note=%s,resolved_by_user_id=%s,resolved_by_username=%s,
                       resolved_at=%s,revision=revision+1,updated_at=%s WHERE id=%s""",
                (status, note or row.get("note"), actor["id"] if status == "resolved" else None,
                 actor["username"] if status == "resolved" else None,
                 now if status == "resolved" else None, now, row["id"]),
            )
            row.update(status=status, actor_user_id=None, actor_username=None, lease_token=None,
                       lease_expires_at=None, note=note or row.get("note"),
                       resolved_by_username=actor["username"] if status == "resolved" else None,
                       resolved_at=now if status == "resolved" else None, revision=row["revision"] + 1)
            self._event(cur, row, action, actor, now, old_status="analysis", note=note)
            conn.commit()
            return {"item": _serialize(row, now)}
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    def supervision(self, scope_key: str, preset: str, top: int,
                    start: Optional[str] = None, end: Optional[str] = None,
                    action: Optional[str] = None, agent: Optional[str] = None) -> dict:
        scope = resolve_scope(scope_key)
        if top not in {5, 10, 20}:
            raise TreatmentError("INVALID_TOP", "Quantidade de agentes inválida.")
        action = str(action or "").strip()
        agent = str(agent or "").strip()
        if action not in {"", "resolved", "waiting", "open"}:
            raise TreatmentError("INVALID_ACTION", "Filtro de ação inválido.")
        if len(agent) > 150:
            raise TreatmentError("INVALID_AGENT", "Filtro de agente inválido.")
        now = utc_now_naive()
        if preset == "hour":
            since, until = now - timedelta(hours=1), now
        elif preset == "six_hours":
            since, until = now - timedelta(hours=6), now
        elif preset == "day":
            since, until = now - timedelta(days=1), now
        elif preset == "week":
            since, until = now - timedelta(days=7), now
        elif preset == "custom":
            try:
                first, last = date.fromisoformat(start or ""), date.fromisoformat(end or "")
            except ValueError:
                raise TreatmentError("INVALID_PERIOD", "Informe um período válido.") from None
            try:
                zone = ZoneInfo(os.getenv("OFS_MONITOR_DEFAULT_TIMEZONE") or "America/Sao_Paulo")
            except (ZoneInfoNotFoundError, ValueError):
                zone = ZoneInfo("America/Sao_Paulo")
            local_today = now.replace(tzinfo=ZoneInfo("UTC")).astimezone(zone).date()
            if last < first or (last - first).days > 90 or last > local_today:
                raise TreatmentError("INVALID_PERIOD", "Use um período de até 90 dias, sem datas futuras.")
            since = datetime.combine(first, datetime.min.time(), zone).astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
            until = datetime.combine(last + timedelta(days=1), datetime.min.time(), zone).astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
        else:
            raise TreatmentError("INVALID_PERIOD", "Período inválido.")
        conn = self.connection_factory()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """SELECT resolved_by_username AS username,COUNT(*) AS total
                   FROM ofs_operational_monitor_treatment
                   WHERE scope_key=%s AND status='resolved' AND resolved_at>=%s AND resolved_at<%s
                   GROUP BY resolved_by_user_id,resolved_by_username ORDER BY total DESC,username LIMIT %s""",
                (scope["key"], since, until, top),
            )
            ranking = cur.fetchall()
            cur.execute(
                """SELECT COUNT(*) AS total FROM ofs_operational_monitor_treatment
                   WHERE scope_key=%s AND status='resolved' AND resolved_at>=%s AND resolved_at<%s""",
                (scope["key"], since, until),
            )
            total = (cur.fetchone() or {}).get("total") or 0
            cur.execute(
                """SELECT COUNT(*) AS total FROM ofs_operational_monitor_treatment
                   WHERE scope_key=%s AND status='analysis' AND lease_expires_at>%s""",
                (scope["key"], now),
            )
            active_total = (cur.fetchone() or {}).get("total") or 0
            current_work_date = operational_work_date(now.replace(tzinfo=ZoneInfo("UTC")))
            cur.execute(
                """SELECT COUNT(*) AS total FROM ofs_operational_monitor_treatment
                   WHERE scope_key=%s AND work_date=%s AND status='waiting'""",
                (scope["key"], current_work_date),
            )
            waiting_total = (cur.fetchone() or {}).get("total") or 0
            cur.execute(
                """SELECT DISTINCT actor_username FROM ofs_operational_monitor_treatment_event
                   WHERE scope_key=%s AND occurred_at>=%s AND occurred_at<%s
                     AND action IN ('resolved','waiting','open')
                   ORDER BY actor_username LIMIT 200""",
                (scope["key"], since, until),
            )
            agents = [row["actor_username"] for row in cur.fetchall() if row.get("actor_username")]
            recent_filters = ""
            recent_params = [scope["key"], since, until]
            if action:
                recent_filters += " AND e.action=%s"
                recent_params.append(action)
            if agent:
                recent_filters += " AND e.actor_username=%s"
                recent_params.append(agent)
            cur.execute(
                """SELECT e.indicator,e.item_key,e.action,e.actor_username,e.note,e.occurred_at,
                          t.item_label,t.technician,t.area,t.state_uf
                   FROM ofs_operational_monitor_treatment_event e
                   JOIN ofs_operational_monitor_treatment t ON t.id=e.treatment_id
                   WHERE e.scope_key=%s AND e.occurred_at>=%s AND e.occurred_at<%s
                     AND e.action IN ('resolved','waiting','open')
                   """ + recent_filters + " ORDER BY e.occurred_at DESC,e.id DESC LIMIT 30",
                tuple(recent_params),
            )
            recent = cur.fetchall()
            for row in recent:
                row["occurred_at"] = _time_text(row["occurred_at"])
            cur.execute(
                """SELECT indicator,item_key,item_label,technician,area,state_uf,actor_username,lease_expires_at
                   FROM ofs_operational_monitor_treatment
                   WHERE scope_key=%s AND status='analysis' AND lease_expires_at>%s
                   ORDER BY lease_expires_at LIMIT 30""",
                (scope["key"], now),
            )
            active = cur.fetchall()
            for row in active:
                row["lease_expires_at"] = _time_text(row["lease_expires_at"])
            return {"preset": preset, "since": _time_text(since), "until": _time_text(until),
                    "total_resolved": total, "active_total": active_total,
                    "waiting_total": waiting_total, "current_work_date": current_work_date.isoformat(),
                    "agents": agents, "ranking": ranking, "recent": recent, "active": active}
        finally:
            cur.close()
            conn.close()
