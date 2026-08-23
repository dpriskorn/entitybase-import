-- Recreate foreign keys and secondary indexes after bulk import
-- Usage: sudo mariadb entitybase < scripts/create-indexes.sql

-- Recreate foreign keys
ALTER TABLE entity_backlinks ADD FOREIGN KEY (referenced_internal_id) REFERENCES entity_id_mapping(internal_id);
ALTER TABLE entity_backlinks ADD FOREIGN KEY (referencing_internal_id) REFERENCES entity_id_mapping(internal_id);
ALTER TABLE entity_backlinks ADD FOREIGN KEY (statement_hash) REFERENCES statement_content(content_hash);

-- Recreate secondary indexes
ALTER TABLE entity_backlinks ADD INDEX idx_backlinks_property (referencing_internal_id, property_id);
ALTER TABLE metadata_content ADD INDEX idx_type_hash (content_type, content_hash);
ALTER TABLE metadata_content ADD INDEX idx_ref_count (ref_count DESC);
ALTER TABLE lexeme_terms ADD INDEX idx_entity (entity_id);
ALTER TABLE lexeme_terms ADD INDEX idx_hash (term_hash);
ALTER TABLE lexeme_terms ADD INDEX idx_language (language);
ALTER TABLE statements ADD INDEX idx_ref_count (ref_count DESC);
ALTER TABLE qualifiers ADD INDEX idx_ref_count (ref_count DESC);
ALTER TABLE refs ADD INDEX idx_ref_count (ref_count DESC);
ALTER TABLE snaks ADD INDEX idx_ref_count (ref_count DESC);
ALTER TABLE sitelinks ADD INDEX idx_ref_count (ref_count DESC);
ALTER TABLE entity_terms ADD INDEX idx_ref_count (ref_count DESC);

-- Re-enable checks
SET GLOBAL foreign_key_checks = 1;
SET GLOBAL unique_checks = 1;
SET GLOBAL autocommit = 1;

SELECT 'FKs and indexes recreated, checks re-enabled' AS status;
