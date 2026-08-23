-- Drop foreign keys and secondary indexes before bulk import
-- Usage: sudo mariadb entitybase < scripts/drop-indexes.sql

SET GLOBAL foreign_key_checks = 0;
SET GLOBAL unique_checks = 0;
SET GLOBAL autocommit = 0;

-- Drop foreign keys FIRST (indexes used by FKs cannot be dropped until FK is removed)
ALTER TABLE entity_backlinks DROP FOREIGN KEY `1`;
ALTER TABLE entity_backlinks DROP FOREIGN KEY `2`;
ALTER TABLE entity_backlinks DROP FOREIGN KEY `3`;

-- Now drop secondary indexes
ALTER TABLE entity_backlinks DROP INDEX idx_backlinks_property;
ALTER TABLE metadata_content DROP INDEX idx_type_hash;
ALTER TABLE metadata_content DROP INDEX idx_ref_count;
ALTER TABLE lexeme_terms DROP INDEX idx_entity;
ALTER TABLE lexeme_terms DROP INDEX idx_hash;
ALTER TABLE lexeme_terms DROP INDEX idx_language;
ALTER TABLE statements DROP INDEX idx_ref_count;
ALTER TABLE qualifiers DROP INDEX idx_ref_count;
ALTER TABLE refs DROP INDEX idx_ref_count;
ALTER TABLE snaks DROP INDEX idx_ref_count;
ALTER TABLE sitelinks DROP INDEX idx_ref_count;
ALTER TABLE entity_terms DROP INDEX idx_ref_count;

SELECT 'Checks disabled, FKs and indexes dropped' AS status;
