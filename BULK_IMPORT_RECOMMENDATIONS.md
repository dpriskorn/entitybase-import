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
sudo mariadb entitybase -e "
SET GLOBAL foreign_key_checks = 0;
SET GLOBAL unique_checks = 0;
SET GLOBAL autocommit = 0;
SELECT 'Checks disabled, ready for import' AS status;
"
```

#### Run Import

```bash
cd libs/entitybase-import
just import-lexemes
```

#### After Import

```bash
sudo mariadb entitybase -e "
SET GLOBAL foreign_key_checks = 1;
SET GLOBAL unique_checks = 1;
SET GLOBAL autocommit = 1;
SELECT 'Checks re-enabled' AS status;
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
