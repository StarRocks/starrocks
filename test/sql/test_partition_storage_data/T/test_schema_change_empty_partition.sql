-- name: test_schema_change_empty_partition

DROP DATABASE IF EXISTS test_schema_change_empty_partition;
CREATE DATABASE test_schema_change_empty_partition;
USE test_schema_change_empty_partition;

CREATE TABLE `t1_alter_with_data` (
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

INSERT INTO t1_alter_with_data VALUES('2025-01-15', 1, 100);
INSERT INTO t1_alter_with_data VALUES('2025-02-15', 2, 200);

SELECT COUNT(*) FROM t1_alter_with_data;

SELECT MIN(dt), MAX(dt) FROM t1_alter_with_data;

ALTER TABLE t1_alter_with_data ADD COLUMN new_col INT NULL DEFAULT "0";
function: wait_alter_table_finish()

SELECT COUNT(*) FROM t1_alter_with_data;

SELECT MIN(dt), MAX(dt) FROM t1_alter_with_data;

CREATE TABLE `t2_multi_alter` (
    `dt` date NOT NULL COMMENT "",
    `id` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`)
PARTITION BY RANGE(`dt`)
(
    PARTITION p1 VALUES [("2025-01-01"), ("2025-02-01"))
)
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "1"
);

INSERT INTO t2_multi_alter VALUES('2025-01-15', 1);

ALTER TABLE t2_multi_alter ADD COLUMN col1 INT NULL;
function: wait_alter_table_finish()

SELECT COUNT(*) FROM t2_multi_alter;

ALTER TABLE t2_multi_alter ADD COLUMN col2 STRING NULL;
function: wait_alter_table_finish()

SELECT COUNT(*) FROM t2_multi_alter;

ALTER TABLE t2_multi_alter ADD COLUMN col3 DOUBLE NULL;
function: wait_alter_table_finish()

SELECT COUNT(*) FROM t2_multi_alter;

SELECT * FROM t2_multi_alter;

CREATE TABLE `t3_alter_then_insert` (
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

INSERT INTO t3_alter_then_insert VALUES('2025-01-15', 1);

SELECT COUNT(*) FROM t3_alter_then_insert;

ALTER TABLE t3_alter_then_insert ADD COLUMN new_col INT NULL;
function: wait_alter_table_finish()

INSERT INTO t3_alter_then_insert VALUES('2025-01-20', 2, 100);
INSERT INTO t3_alter_then_insert VALUES('2025-02-15', 3, 200);

SELECT COUNT(*) FROM t3_alter_then_insert;

SELECT MIN(dt), MAX(dt) FROM t3_alter_then_insert;

CREATE TABLE `t4_query_after_alter` (
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

INSERT INTO t4_query_after_alter VALUES
('2025-01-15', 1, 100),
('2025-02-15', 2, 200),
('2025-03-15', 3, 300);

SELECT * FROM t4_query_after_alter WHERE dt >= '2025-02-01' ORDER BY dt;

ALTER TABLE t4_query_after_alter ADD COLUMN col1 STRING NULL;
function: wait_alter_table_finish()

SELECT * FROM t4_query_after_alter WHERE dt >= '2025-02-01' ORDER BY dt;

SELECT COUNT(*) FROM t4_query_after_alter;

CREATE TABLE `t5_list_alter` (
    `c1` int NOT NULL,
    `c2` int
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
PARTITION BY LIST(`c1`)
(
    PARTITION p1 VALUES IN ('1', '2'),
    PARTITION p2 VALUES IN ('3', '4')
)
DISTRIBUTED BY HASH(`c1`)
PROPERTIES (
    "replication_num" = "1"
);

INSERT INTO t5_list_alter VALUES(1, 100), (3, 200);

SELECT COUNT(*) FROM t5_list_alter;

ALTER TABLE t5_list_alter ADD COLUMN col1 STRING NULL;
function: wait_alter_table_finish()

SELECT COUNT(*) FROM t5_list_alter;

SELECT MIN(c1), MAX(c1) FROM t5_list_alter;

DROP DATABASE IF EXISTS test_schema_change_empty_partition;
