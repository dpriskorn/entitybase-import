# Bulk Import Recommendations

Guide for tuning MariaDB/MySQL for large Wikidata imports (9M+ entities).

## Quick Reference

### 16GB RAM, No Swap, HDD

#### MariaDB Config: `/etc/my.cnf.d/bulk-import.cnf`

```ini
[mysqld]
# Buffer pool - 6GB for 16GB RAM
innodb_buffer_pool_size = 6G

# Logs - larger for bulk inserts
innodb_log_file_size = 1G
innodb_log_buffer_size = 64M

# Write performance (HDD-safe)
innodb_flush_log_at_trx_commit = 2
innodb_flush_method = O_DIRECT
innodb_io_capacity = 200
innodb_io_capacity_max = 400

# Bulk insert optimizations
bulk_insert_buffer_size = 256M
innodb_autoinc_lock_mode = 2

# Buffers
sort_buffer_size = 4M
join_buffer_size = 4M
tmp_table_size = 256M
max_heap_table_size = 256M
thread_cache_size = 16
table_open_cache = 4096
max_allowed_packet = 64M
```

#### Apply Config

```bash
sudo tee /etc/my.cnf.d/bulk-import.cnf << 'EOF'
[mysqld]
innodb_buffer_pool_size = 6G
innodb_log_file_size = 1G
innodb_log_buffer_size = 64M
innodb_flush_log_at_trx_commit = 2
innodb_flush_method = O_DIRECT
innodb_io_capacity = 200
innodb_io_capacity_max = 400
bulk_insert_buffer_size = 256M
innodb_autoinc_lock_mode = 2
sort_buffer_size = 4M
join_buffer_size = 4M
tmp_table_size = 256M
max_heap_table_size = 256M
thread_cache_size = 16
table_open_cache = 4096
max_allowed_packet = 64M
EOF

sudo systemctl restart mariadb
```

#### Before Import

```bash
# Disable checks + drop foreign keys and indexes
sudo mariadb entitybase -e "
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
"
```

#### Run Import

```bash
cd libs/entitybase-import
just import-lexemes
```

#### After Import

```bash
# Recreate foreign keys and indexes + re-enable checks
sudo mariadb entitybase -e "
-- Recreate foreign keys
ALTER TABLE entity_backlinks ADD FOREIGN KEY (referenced_internal_id) REFERENCES entity_id_mapping(internal_id);
ALTER TABLE entity_backlinks ADD FOREIGN KEY (referencing_internal_id) REFERENCES entity_id_mapping(internal_id);
ALTER TABLE entity_backlinks ADD FOREIGN KEY (statement_hash) REFERENCES statement_content(content_hash);

-- Recreate secondary indexes
ALTER TABLE metadata_content ADD INDEX idx_type_hash (content_type, content_hash);
ALTER TABLE metadata_content ADD INDEX idx_ref_count (ref_count DESC);
ALTER TABLE lexeme_terms ADD INDEX idx_entity (entity_id);
ALTER TABLE lexeme_terms ADD INDEX idx_hash (term_hash);
ALTER TABLE lexeme_terms ADD INDEX idx_language (language);
ALTER TABLE entity_backlinks ADD INDEX idx_backlinks_property (referencing_internal_id, property_id);
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

SELECT 'Indexes and FKs recreated, checks re-enabled' AS status;
"
```

#### Cleanup

```bash
sudo rm /etc/my.cnf.d/bulk-import.cnf
sudo systemctl restart mariadb
```

## Memory Budget

| Component      | RAM   |
|----------------|-------|
| OS + services  | 2GB   |
| MariaDB pool   | 6GB   |
| MariaDB other  | 1GB   |
| entitybase-api | 1GB   |
| Headroom       | 6GB   |

## Settings Explained

### innodb_buffer_pool_size

Most important setting. Caches data + indexes in RAM.

- 16GB RAM: 6GB (leaves room for OS + other processes)
- 32GB RAM: 16-20GB
- 64GB RAM: 40-48GB

### innodb_log_file_size

WAL log size. Larger = fewer checkpoints during bulk inserts.

- Bulk import: 1-2GB
- Production: 256-512MB

### innodb_flush_log_at_trx_commit

- `1`: Full ACID (slowest, safest)
- `2`: Flush to OS cache only (fast, safe on crash, lose 1s on power failure)
- `0`: Never flush (fastest, unsafe)

For bulk import: use `2`
For production: use `1`

### innodb_flush_method

`O_DIRECT`: Avoid double-buffering with OS page cache.

### innodb_io_capacity

- HDD: 200-400
- SSD: 2000-10000

### bulk_insert_buffer_size

Buffer for bulk INSERT operations. 256M recommended for large imports.

## Other Tips

### Drop indexes before import, recreate after

```bash
sudo mariadb -e "
SHOW PROCESSLIST;
SHOW ENGINE INNODB STATUS\G
"
```

### Disk space

Ensure at least 2x the dump file size is available for:
- Decompressed dump file
- MariaDB data files
- InnoDB logs
