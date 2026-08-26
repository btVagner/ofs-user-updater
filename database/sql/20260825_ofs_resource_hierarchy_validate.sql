SELECT
    TABLE_NAME,
    ENGINE,
    TABLE_COLLATION
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'ofs_resource_hierarchy';

SELECT
    COLUMN_NAME,
    COLUMN_TYPE,
    IS_NULLABLE,
    COLUMN_KEY
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'ofs_resource_hierarchy'
ORDER BY ORDINAL_POSITION;

SELECT
    INDEX_NAME,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS indexed_columns
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'ofs_resource_hierarchy'
GROUP BY INDEX_NAME
ORDER BY INDEX_NAME;

SELECT
    root_resource_id,
    COUNT(*) AS resources_total,
    SUM(status = 'active') AS active_total,
    SUM(status <> 'active') AS inactive_total,
    MAX(depth) AS max_depth,
    MAX(last_seen_at) AS last_seen_at
FROM ofs_resource_hierarchy
GROUP BY root_resource_id
ORDER BY root_resource_id;

SELECT child.resource_id, child.parent_resource_id, child.depth
FROM ofs_resource_hierarchy child
LEFT JOIN ofs_resource_hierarchy parent
       ON parent.resource_id = child.parent_resource_id
      AND parent.root_resource_id = child.root_resource_id
WHERE child.depth > 0
  AND parent.resource_id IS NULL;
