-- name: test_minmax_partition_column_rewrite
DROP DATABASE IF EXISTS test_minmax_partition_column_rewrite;
-- result:
-- !result
CREATE DATABASE test_minmax_partition_column_rewrite;
-- result:
-- !result
USE test_minmax_partition_column_rewrite;
-- result:
-- !result
CREATE TABLE `t1_partial_empty` (
    `dt` date NOT NULL COMMENT "",
    `id` int(11) NULL COMMENT "",
    `value` bigint NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
PARTITION BY RANGE(`dt`)
(
    PARTITION p202501 VALUES [("2025-01-01"), ("2025-02-01")),
    PARTITION p202502 VALUES [("2025-02-01"), ("2025-03-01")),
    PARTITION p202503 VALUES [("2025-03-01"), ("2025-04-01"))
)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);
-- result:
-- !result
INSERT INTO t1_partial_empty VALUES('2025-02-15', 1, 100);
-- result:
-- !result
SELECT MIN(dt) FROM t1_partial_empty;
-- result:
2025-02-15
-- !result
SELECT MAX(dt) FROM t1_partial_empty;
-- result:
2025-02-15
-- !result
SELECT MIN(dt), MAX(dt) FROM t1_partial_empty;
-- result:
2025-02-15	2025-02-15
-- !result
CREATE TABLE `t2_schema_change_minmax` (
    `dt` date NOT NULL COMMENT "",
    `id` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
PARTITION BY RANGE(`dt`)
(
    PARTITION p202501 VALUES [("2025-01-01"), ("2025-02-01")),
    PARTITION p202502 VALUES [("2025-02-01"), ("2025-03-01"))
)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);
-- result:
-- !result
INSERT INTO t2_schema_change_minmax VALUES('2025-01-15', 1);
-- result:
-- !result
INSERT INTO t2_schema_change_minmax VALUES('2025-02-15', 2);
-- result:
-- !result
SELECT MIN(dt), MAX(dt) FROM t2_schema_change_minmax;
-- result:
2025-01-15	2025-02-15
-- !result
ALTER TABLE t2_schema_change_minmax ADD COLUMN new_col INT NULL DEFAULT "0";
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT MIN(dt), MAX(dt) FROM t2_schema_change_minmax;
-- result:
2025-01-15	2025-02-15
-- !result
INSERT INTO t2_schema_change_minmax VALUES('2025-01-20', 3, 100);
-- result:
-- !result
INSERT INTO t2_schema_change_minmax VALUES('2025-02-20', 4, 200);
-- result:
-- !result
SELECT MIN(dt), MAX(dt) FROM t2_schema_change_minmax;
-- result:
2025-01-15	2025-02-20
-- !result
CREATE TABLE `t3_list_minmax` (
    `c1` int NOT NULL,
    `c2` int,
    `c3` varchar(100)
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
PARTITION BY LIST(`c1`)
(
    PARTITION p1 VALUES IN ('1', '2', '3'),
    PARTITION p2 VALUES IN ('4', '5', '6'),
    PARTITION p3 VALUES IN ('7', '8', '9')
)
DISTRIBUTED BY HASH(`c1`)
PROPERTIES (
    "replication_num" = "1"
);
-- result:
-- !result
INSERT INTO t3_list_minmax VALUES(1, 100, 'a'), (5, 200, 'b'), (9, 300, 'c');
-- result:
-- !result
SELECT MIN(c1), MAX(c1) FROM t3_list_minmax;
-- result:
1	9
-- !result
ALTER TABLE t3_list_minmax ADD COLUMN new_col DOUBLE NULL;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT MIN(c1), MAX(c1) FROM t3_list_minmax;
-- result:
1	9
-- !result
CREATE TABLE `t4_prune_with_minmax` (
    `dt` date NOT NULL COMMENT "",
    `id` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
PARTITION BY RANGE(`dt`)
(
    PARTITION p202501 VALUES [("2025-01-01"), ("2025-02-01")),
    PARTITION p202502 VALUES [("2025-02-01"), ("2025-03-01")),
    PARTITION p202503 VALUES [("2025-03-01"), ("2025-04-01")),
    PARTITION p202504 VALUES [("2025-04-01"), ("2025-05-01"))
)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);
-- result:
-- !result
INSERT INTO t4_prune_with_minmax VALUES('2025-01-15', 1);
-- result:
-- !result
INSERT INTO t4_prune_with_minmax VALUES('2025-02-15', 2);
-- result:
-- !result
INSERT INTO t4_prune_with_minmax VALUES('2025-03-15', 3);
-- result:
-- !result
INSERT INTO t4_prune_with_minmax VALUES('2025-04-15', 4);
-- result:
-- !result
SELECT MIN(dt) FROM t4_prune_with_minmax;
-- result:
2025-01-15
-- !result
SELECT MAX(dt) FROM t4_prune_with_minmax;
-- result:
2025-04-15
-- !result
ALTER TABLE t4_prune_with_minmax ADD COLUMN col1 STRING NULL;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT MIN(dt) FROM t4_prune_with_minmax;
-- result:
2025-01-15
-- !result
SELECT MAX(dt) FROM t4_prune_with_minmax;
-- result:
2025-04-15
-- !result
CREATE TABLE `t5_filter_minmax` (
    `dt` date NOT NULL COMMENT "",
    `id` int(11) NULL COMMENT "",
    `value` bigint NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
PARTITION BY RANGE(`dt`)
(
    PARTITION p202501 VALUES [("2025-01-01"), ("2025-02-01")),
    PARTITION p202502 VALUES [("2025-02-01"), ("2025-03-01"))
)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);
-- result:
-- !result
INSERT INTO t5_filter_minmax VALUES('2025-01-15', 1, 100);
-- result:
-- !result
INSERT INTO t5_filter_minmax VALUES('2025-02-15', 2, 200);
-- result:
-- !result
SELECT MIN(dt), MAX(dt) FROM t5_filter_minmax;
-- result:
2025-01-15	2025-02-15
-- !result
ALTER TABLE t5_filter_minmax ADD COLUMN col1 INT NULL;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT MIN(dt), MAX(dt) FROM t5_filter_minmax;
-- result:
2025-01-15	2025-02-15
-- !result
DROP DATABASE IF EXISTS test_minmax_partition_column_rewrite;
-- result:
-- !result