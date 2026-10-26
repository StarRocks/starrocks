-- name: test_minmax_partition_column_rewrite

DROP DATABASE IF EXISTS test_minmax_partition_column_rewrite;
CREATE DATABASE test_minmax_partition_column_rewrite;
USE test_minmax_partition_column_rewrite;

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

INSERT INTO t1_partial_empty VALUES('2025-02-15', 1, 100);

SELECT MIN(dt) FROM t1_partial_empty;

SELECT MAX(dt) FROM t1_partial_empty;

SELECT MIN(dt), MAX(dt) FROM t1_partial_empty;

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

INSERT INTO t2_schema_change_minmax VALUES('2025-01-15', 1);
INSERT INTO t2_schema_change_minmax VALUES('2025-02-15', 2);

SELECT MIN(dt), MAX(dt) FROM t2_schema_change_minmax;

ALTER TABLE t2_schema_change_minmax ADD COLUMN new_col INT NULL DEFAULT "0";

function: wait_alter_table_finish()

SELECT MIN(dt), MAX(dt) FROM t2_schema_change_minmax;

INSERT INTO t2_schema_change_minmax VALUES('2025-01-20', 3, 100);
INSERT INTO t2_schema_change_minmax VALUES('2025-02-20', 4, 200);

SELECT MIN(dt), MAX(dt) FROM t2_schema_change_minmax;

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

INSERT INTO t3_list_minmax VALUES(1, 100, 'a'), (5, 200, 'b'), (9, 300, 'c');

SELECT MIN(c1), MAX(c1) FROM t3_list_minmax;

ALTER TABLE t3_list_minmax ADD COLUMN new_col DOUBLE NULL;

function: wait_alter_table_finish()

SELECT MIN(c1), MAX(c1) FROM t3_list_minmax;

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

INSERT INTO t4_prune_with_minmax VALUES('2025-01-15', 1);
INSERT INTO t4_prune_with_minmax VALUES('2025-02-15', 2);
INSERT INTO t4_prune_with_minmax VALUES('2025-03-15', 3);
INSERT INTO t4_prune_with_minmax VALUES('2025-04-15', 4);

SELECT MIN(dt) FROM t4_prune_with_minmax;

SELECT MAX(dt) FROM t4_prune_with_minmax;

ALTER TABLE t4_prune_with_minmax ADD COLUMN col1 STRING NULL;

function: wait_alter_table_finish()

SELECT MIN(dt) FROM t4_prune_with_minmax;
SELECT MAX(dt) FROM t4_prune_with_minmax;

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

INSERT INTO t5_filter_minmax VALUES('2025-01-15', 1, 100);
INSERT INTO t5_filter_minmax VALUES('2025-02-15', 2, 200);

SELECT MIN(dt), MAX(dt) FROM t5_filter_minmax;

ALTER TABLE t5_filter_minmax ADD COLUMN col1 INT NULL;

function: wait_alter_table_finish()

SELECT MIN(dt), MAX(dt) FROM t5_filter_minmax;

-- A LIST partition's values say what its rows may hold, not what they hold: MIN/MAX must come from the data.
CREATE TABLE `t6_list_partial_values` (
    `c1` int NOT NULL,
    `c2` int
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
PARTITION BY LIST(`c1`)
(
    PARTITION p1 VALUES IN ('1', '2'),
    PARTITION p2 VALUES IN ('3', '4'),
    PARTITION p3 VALUES IN ('5', '6')
)
DISTRIBUTED BY HASH(`c1`)
PROPERTIES (
    "replication_num" = "1"
);

INSERT INTO t6_list_partial_values VALUES(2, 1), (3, 2), (5, 3);

SELECT MIN(c1), MAX(c1) FROM t6_list_partial_values;

CREATE TABLE `t7_list_interleaved` (
    `c1` int NOT NULL,
    `c2` int
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
PARTITION BY LIST(`c1`)
(
    PARTITION p1 VALUES IN ('1', '10'),
    PARTITION p2 VALUES IN ('5', '6')
)
DISTRIBUTED BY HASH(`c1`)
PROPERTIES (
    "replication_num" = "1"
);

INSERT INTO t7_list_interleaved VALUES(1, 1), (5, 2);

SELECT MIN(c1), MAX(c1) FROM t7_list_interleaved;

CREATE TABLE `t8_list_null_partitions` (
    `c1` int NULL,
    `c2` int
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
PARTITION BY LIST(`c1`)
(
    PARTITION pn VALUES IN (NULL),
    PARTITION p1 VALUES IN ('1'),
    PARTITION p2 VALUES IN ('2')
)
DISTRIBUTED BY HASH(`c2`)
PROPERTIES (
    "replication_num" = "1"
);

INSERT INTO t8_list_null_partitions VALUES(NULL, 0), (1, 1), (2, 2);

SELECT MIN(c1), MAX(c1) FROM t8_list_null_partitions;

CREATE TABLE `t9_list_null_mixed` (
    `c1` int NULL,
    `c2` int
) ENGINE=OLAP
DUPLICATE KEY(`c1`)
PARTITION BY LIST(`c1`)
(
    PARTITION p0 VALUES IN (NULL, '7'),
    PARTITION p1 VALUES IN ('1')
)
DISTRIBUTED BY HASH(`c2`)
PROPERTIES (
    "replication_num" = "1"
);

INSERT INTO t9_list_null_mixed VALUES(NULL, 0), (1, 1);

SELECT MIN(c1), MAX(c1) FROM t9_list_null_mixed;

DROP DATABASE IF EXISTS test_minmax_partition_column_rewrite;
