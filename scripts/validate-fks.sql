-- Validate foreign key integrity after bulk import
-- Usage: sudo mariadb entitybase < scripts/validate-fks.sql
--
-- Run this BEFORE create-indexes.sql to ensure data integrity.
-- If any query returns > 0, there are orphaned references that must be resolved.

-- Re-enable foreign key checks first
SET GLOBAL foreign_key_checks = 1;

-- Check entity_backlinks -> entity_id_mapping (referenced_internal_id)
SELECT 'entity_backlinks.referenced_internal_id' AS fk_column,
       COUNT(*) AS orphaned_rows
FROM entity_backlinks eb
LEFT JOIN entity_id_mapping m ON eb.referenced_internal_id = m.internal_id
WHERE m.internal_id IS NULL;

-- Check entity_backlinks -> entity_id_mapping (referencing_internal_id)
SELECT 'entity_backlinks.referencing_internal_id' AS fk_column,
       COUNT(*) AS orphaned_rows
FROM entity_backlinks eb
LEFT JOIN entity_id_mapping m ON eb.referencing_internal_id = m.internal_id
WHERE m.internal_id IS NULL;

-- Check entity_backlinks -> statement_content (statement_hash)
SELECT 'entity_backlinks.statement_hash' AS fk_column,
       COUNT(*) AS orphaned_rows
FROM entity_backlinks eb
LEFT JOIN statement_content sc ON eb.statement_hash = sc.content_hash
WHERE sc.content_hash IS NULL;

-- Summary
SELECT CASE
    WHEN EXISTS (
        SELECT 1 FROM entity_backlinks eb
        LEFT JOIN entity_id_mapping m1 ON eb.referenced_internal_id = m1.internal_id
        LEFT JOIN entity_id_mapping m2 ON eb.referencing_internal_id = m2.internal_id
        LEFT JOIN statement_content sc ON eb.statement_hash = sc.content_hash
        WHERE m1.internal_id IS NULL OR m2.internal_id IS NULL OR sc.content_hash IS NULL
    ) THEN 'FAIL: Orphaned references found - investigate before recreating FKs'
    ELSE 'PASS: No orphaned references - safe to recreate FKs'
END AS validation_result;
