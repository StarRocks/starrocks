-- name: test_partition_storage_data_schema_change
DROP DATABASE IF EXISTS test_partition_storage_data_schema_change;
-- result:
-- !result
CREATE DATABASE test_partition_storage_data_schema_change;
-- result:
-- !result
USE test_partition_storage_data_schema_change;
-- result:
-- !result
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
-- result:
-- !result
INSERT INTO t1_range_basic VALUES('2025-04-29', 1, 'bar');
-- result:
-- !result
SELECT MIN(dt), MAX(dt) FROM t1_range_basic;
-- result:
2025-04-29	2025-04-29
-- !result
ALTER TABLE t1_range_basic ADD COLUMN new_col INT NULL DEFAULT "0";
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT MIN(dt), MAX(dt) FROM t1_range_basic;
-- result:
2025-04-29	2025-04-29
-- !result
INSERT INTO t1_range_basic VALUES('2025-04-29', 2, 'baz', 100);
-- result:
-- !result
SELECT MIN(dt), MAX(dt) FROM t1_range_basic;
-- result:
2025-04-29	2025-04-29
-- !result
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
-- result:
-- !result
INSERT INTO t2_list_rollup VALUES(1, 100, 'a'), (3, 200, 'b'), (5, 300, 'c');
-- result:
-- !result
SELECT MIN(c1), MAX(c1) FROM t2_list_rollup;
-- result:
1	6
-- !result
ALTER TABLE t2_list_rollup ADD ROLLUP r1(c1, c2);
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT MIN(c1), MAX(c1) FROM t2_list_rollup;
-- result:
1	6
-- !result
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
-- result:
-- !result
INSERT INTO t3_range_empty VALUES('2025-02-15', 1, 100), ('2025-02-20', 2, 200);
-- result:
-- !result
SELECT MIN(dt), MAX(dt) FROM t3_range_empty;
-- result:
2025-02-15	2025-02-20
-- !result
ALTER TABLE t3_range_empty ADD COLUMN new_value DOUBLE NULL;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT MIN(dt), MAX(dt) FROM t3_range_empty;
-- result:
2025-02-15	2025-02-20
-- !result
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
-- result:
-- !result
INSERT INTO t4_prune_after_alter VALUES
('2025-01-15', 1, 'jan'),
('2025-02-15', 2, 'feb'),
('2025-03-15', 3, 'mar'),
('2025-04-15', 4, 'apr');
-- result:
-- !result
SELECT * FROM t4_prune_after_alter WHERE dt >= '2025-02-01' AND dt < '2025-04-01' ORDER BY dt;
-- result:
2025-02-15	2	feb
2025-03-15	3	mar
-- !result
ALTER TABLE t4_prune_after_alter ADD COLUMN description STRING NULL;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT * FROM t4_prune_after_alter WHERE dt >= '2025-02-01' AND dt < '2025-04-01' ORDER BY dt;
-- result:
2025-02-15	2	feb	None
2025-03-15	3	mar	None
-- !result
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
-- result:
-- !result
INSERT INTO t5_multi_alter VALUES('2025-01-15', 1), ('2025-02-15', 2);
-- result:
-- !result
ALTER TABLE t5_multi_alter ADD COLUMN col1 INT NULL;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT MIN(dt), MAX(dt) FROM t5_multi_alter;
-- result:
2025-01-15	2025-02-15
-- !result
ALTER TABLE t5_multi_alter ADD COLUMN col2 STRING NULL;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT MIN(dt), MAX(dt) FROM t5_multi_alter;
-- result:
2025-01-15	2025-02-15
-- !result
INSERT INTO t5_multi_alter VALUES('2025-01-20', 3, 100, 'test');
-- result:
-- !result
SELECT MIN(dt), MAX(dt) FROM t5_multi_alter;
-- result:
2025-01-15	2025-02-15
-- !result
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
-- result:
-- !result
INSERT INTO t6_explain_check VALUES('2025-02-15', 1);
-- result:
-- !result
EXPLAIN SELECT MIN(dt), MAX(dt) FROM t6_explain_check;
-- result:
PLAN FRAGMENT 0
 OUTPUT EXPRS:6: min | 7: max
  PARTITION: UNPARTITIONED

  RESULT SINK

  3:AGGREGATE (merge finalize)
  |  output: min(6: min), max(7: max)
  |  group by: 
  |  
  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update serialize)
  |  output: min(1: dt), max(1: dt)
  |  group by: 
  |  
  0:OlapScanNode
     TABLE: t6_explain_check
     PREAGGREGATION: ON
     partitions=1/2
     rollup: t6_explain_check
     tabletRatio=2/2
     tabletList=11425,11427
     cardinality=1
     avgRowSize=4.0
-- !result
ALTER TABLE t6_explain_check ADD COLUMN new_col BIGINT NULL;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
EXPLAIN SELECT MIN(dt), MAX(dt) FROM t6_explain_check;
-- result:
PLAN FRAGMENT 0
 OUTPUT EXPRS:7: min | 8: max
  PARTITION: UNPARTITIONED

  RESULT SINK

  3:AGGREGATE (merge finalize)
  |  output: min(7: min), max(8: max)
  |  group by: 
  |  
  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update serialize)
  |  output: min(1: dt), max(1: dt)
  |  group by: 
  |  
  0:OlapScanNode
     TABLE: t6_explain_check
     PREAGGREGATION: ON
     partitions=1/2
     rollup: t6_explain_check
     tabletRatio=2/2
     tabletList=11425,11427
     cardinality=1
     avgRowSize=4.0
-- !result
DROP DATABASE IF EXISTS test_partition_storage_data_schema_change;
-- result:
-- !result