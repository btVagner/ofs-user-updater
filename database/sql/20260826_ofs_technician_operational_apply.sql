-- Demanda 06 - Read model operacional de técnicos OFS
-- Aplicar antes de iniciar o worker independente.

CREATE TABLE IF NOT EXISTS ofs_technician_operational_state (
    work_date DATE NOT NULL,
    resource_id VARCHAR(64) NOT NULL,
    route_state VARCHAR(32) NOT NULL DEFAULT 'unknown',
    route_state_raw VARCHAR(64) NULL,
    route_started_at DATETIME NULL,
    route_reactivated_at DATETIME NULL,
    route_ended_at DATETIME NULL,
    route_last_event_at DATETIME NULL,
    route_last_event_type VARCHAR(64) NULL,
    route_last_event_fingerprint VARCHAR(1000) NULL,
    calendar_record_type VARCHAR(32) NULL,
    calendar_start_at DATETIME NULL,
    calendar_end_at DATETIME NULL,
    non_working_reason VARCHAR(255) NULL,
    resource_timezone VARCHAR(128) NULL,
    resource_timezone_iana VARCHAR(128) NULL,
    pending_count INT UNSIGNED NOT NULL DEFAULT 0,
    enroute_count INT UNSIGNED NOT NULL DEFAULT 0,
    started_count INT UNSIGNED NOT NULL DEFAULT 0,
    suspended_count INT UNSIGNED NOT NULL DEFAULT 0,
    completed_count INT UNSIGNED NOT NULL DEFAULT 0,
    notdone_count INT UNSIGNED NOT NULL DEFAULT 0,
    cancelled_count INT UNSIGNED NOT NULL DEFAULT 0,
    open_activity_count INT UNSIGNED NOT NULL DEFAULT 0,
    last_reconciled_at DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (work_date, resource_id),
    KEY idx_ofs_tech_state_resource_date (resource_id, work_date),
    KEY idx_ofs_tech_state_date_route (work_date, route_state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ofs_activity_operational_state (
    activity_id VARCHAR(64) NOT NULL,
    work_date DATE NOT NULL,
    resource_id VARCHAR(64) NULL,
    status VARCHAR(32) NOT NULL,
    appt_number VARCHAR(64) NULL,
    activity_type VARCHAR(64) NULL,
    resource_timezone_iana VARCHAR(128) NULL,
    last_event_at DATETIME NULL,
    last_event_type VARCHAR(64) NULL,
    last_event_fingerprint VARCHAR(1000) NULL,
    last_reconciled_at DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (activity_id),
    KEY idx_ofs_activity_state_date_resource_status (work_date, resource_id, status),
    KEY idx_ofs_activity_state_resource_date (resource_id, work_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ofs_event_cursor (
    cursor_key VARCHAR(64) NOT NULL,
    subscription_id VARCHAR(128) NOT NULL,
    next_page VARCHAR(48) NOT NULL,
    subscription_created_at DATETIME(6) NOT NULL,
    baseline_completed_at DATETIME(6) NULL,
    last_poll_success_at DATETIME(6) NULL,
    last_event_at DATETIME NULL,
    last_error_code VARCHAR(64) NULL,
    last_error_message VARCHAR(500) NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (cursor_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ofs_operational_sync_state (
    source_name VARCHAR(32) NOT NULL,
    last_started_at DATETIME(6) NULL,
    last_success_at DATETIME(6) NULL,
    last_finished_at DATETIME(6) NULL,
    status VARCHAR(16) NOT NULL,
    error_code VARCHAR(64) NULL,
    error_message VARCHAR(500) NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (source_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
