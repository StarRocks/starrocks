-- name: test_partition_storage_data_schema_change

DROP DATABASE IF EXISTS test_partition_storage_data_schema_change;
CREATE DATABASE test_partition_storage_data_schema_change;
USE test_partition_storage_data_schema_change;

CREATE TABLE `t1_range_basic` (
    `dt` date NULL COMMENT "",
    `id` int(11) NULL COMMENT "",
    `name` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `id`, `name`)
PARTITION BY RANGE(`dt`)
(
    PARTITION p20250428 VALUES [("2025-04-28"), ("2025-04-29")),
    PARTITION p20250429 VALUES [("2025-04-29"), ("2025-04-30"))
)
DISTRIBUTED BY HASH(`id`, `name`)
PROPERTIES (
    "replication_num" = "1"
);

INSERT INTO t1_range_basic VALUES('2025-04-29', 1, 'bar');

SELECT MIN(dt), MAX(dt) FROM t1_range_basic;

ALTER TABLE t1_range_basic ADD COLUMN new_col INT NULL DEFAULT "0";
function: wait_alter_table_finish()

SELECT MIN(dt), MAX(dt) FROM t1_range_basic;

INSERT INTO t1_range_basic VALUES('2025-04-29', 2, 'baz', 100);

SELECT MIN(dt), MAX(dt) FROM t1_range_basic;

CREATE TABLE `t2_list_rollup` (
    `c1` int,
    `c2` int,
    `c3` varchar(100)
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

INSERT INTO t2_list_rollup VALUES(1, 100, 'a'), (3, 200, 'b'), (5, 300, 'c');

SELECT MIN(c1), MAX(c1) FROM t2_list_rollup;

ALTER TABLE t2_list_rollup ADD ROLLUP r1(c1, c2);

function: wait_alter_table_finish()

SELECT MIN(c1), MAX(c1) FROM t2_list_rollup;

CREATE TABLE `t3_range_empty` (
    `dt` date NULL COMMENT "",
    `id` int(11) NULL COMMENT "",
    `value` bigint NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `id`)
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

INSERT INTO t3_range_empty VALUES('2025-02-15', 1, 100), ('2025-02-20', 2, 200);

SELECT MIN(dt), MAX(dt) FROM t3_range_empty;

ALTER TABLE t3_range_empty ADD COLUMN new_value DOUBLE NULL;

function: wait_alter_table_finish()

SELECT MIN(dt), MAX(dt) FROM t3_range_empty;

CREATE TABLE `t4_prune_after_alter` (
    `dt` date NULL COMMENT "",
    `id` int(11) NULL COMMENT "",
    `name` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`dt`, `id`)
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

INSERT INTO t4_prune_after_alter VALUES
('2025-01-15', 1, 'jan'),
('2025-02-15', 2, 'feb'),
('2025-03-15', 3, 'mar'),
('2025-04-15', 4, 'apr');

SELECT * FROM t4_prune_after_alter WHERE dt >= '2025-02-01' AND dt < '2025-04-01' ORDER BY dt;

ALTER TABLE t4_prune_after_alter ADD COLUMN description STRING NULL;

function: wait_alter_table_finish()

SELECT * FROM t4_prune_after_alter WHERE dt >= '2025-02-01' AND dt < '2025-04-01' ORDER BY dt;

CREATE TABLE `t5_multi_alter` (
    `dt` date NULL COMMENT "",
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

INSERT INTO t5_multi_alter VALUES('2025-01-15', 1), ('2025-02-15', 2);

ALTER TABLE t5_multi_alter ADD COLUMN col1 INT NULL;
function: wait_alter_table_finish()

SELECT MIN(dt), MAX(dt) FROM t5_multi_alter;

ALTER TABLE t5_multi_alter ADD COLUMN col2 STRING NULL;
function: wait_alter_table_finish()

SELECT MIN(dt), MAX(dt) FROM t5_multi_alter;

INSERT INTO t5_multi_alter VALUES('2025-01-20', 3, 100, 'test');

SELECT MIN(dt), MAX(dt) FROM t5_multi_alter;

CREATE TABLE `t6_explain_check` (
    `dt` date NULL COMMENT "",
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

INSERT INTO t6_explain_check VALUES('2025-02-15', 1);

EXPLAIN SELECT MIN(dt), MAX(dt) FROM t6_explain_check;

ALTER TABLE t6_explain_check ADD COLUMN new_col BIGINT NULL;
function: wait_alter_table_finish()

EXPLAIN SELECT MIN(dt), MAX(dt) FROM t6_explain_check;

DROP DATABASE IF EXISTS test_partition_storage_data_schema_change;
