-- Drop all foreign keys and secondary indexes before bulk import
-- Usage: sudo mariadb entitybase < scripts/drop-indexes.sql

SET GLOBAL foreign_key_checks = 0;
SET GLOBAL unique_checks = 0;
SET GLOBAL autocommit = 0;

DELIMITER //

-- Drop all foreign keys using cursor
CREATE PROCEDURE drop_all_fks()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_table VARCHAR(64);
    DECLARE v_fk VARCHAR(64);
    DECLARE cur CURSOR FOR
        SELECT TABLE_NAME, CONSTRAINT_NAME
        FROM information_schema.TABLE_CONSTRAINTS
        WHERE TABLE_SCHEMA = 'entitybase'
          AND CONSTRAINT_TYPE = 'FOREIGN KEY';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO v_table, v_fk;
        IF done THEN LEAVE read_loop; END IF;
        SET @sql = CONCAT('ALTER TABLE `', v_table, '` DROP FOREIGN KEY `', v_fk, '`');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;
    CLOSE cur;
END//

-- Drop all secondary indexes using cursor
CREATE PROCEDURE drop_all_indexes()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_table VARCHAR(64);
    DECLARE v_idx VARCHAR(64);
    DECLARE cur CURSOR FOR
        SELECT TABLE_NAME, INDEX_NAME
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = 'entitybase'
          AND INDEX_NAME != 'PRIMARY'
          AND SEQ_IN_INDEX = 1;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO v_table, v_idx;
        IF done THEN LEAVE read_loop; END IF;
        SET @sql = CONCAT('ALTER TABLE `', v_table, '` DROP INDEX `', v_idx, '`');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;
    CLOSE cur;
END//

DELIMITER ;

CALL drop_all_fks();
DROP PROCEDURE drop_all_fks;

CALL drop_all_indexes();
DROP PROCEDURE drop_all_indexes;

SELECT 'Checks disabled, all FKs and indexes dropped' AS status;
