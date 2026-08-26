CREATE TABLE IF NOT EXISTS ofs_resource_hierarchy (
    resource_id VARCHAR(64) NOT NULL,
    parent_resource_id VARCHAR(64) NULL,
    resource_name VARCHAR(255) NOT NULL,
    resource_type VARCHAR(64) NULL,
    status VARCHAR(32) NOT NULL,
    timezone VARCHAR(128) NULL,
    depth SMALLINT UNSIGNED NOT NULL,
    root_resource_id VARCHAR(64) NOT NULL,
    last_seen_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (resource_id),
    KEY idx_ofs_resource_hierarchy_root_parent (root_resource_id, parent_resource_id),
    KEY idx_ofs_resource_hierarchy_root_depth (root_resource_id, depth)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
