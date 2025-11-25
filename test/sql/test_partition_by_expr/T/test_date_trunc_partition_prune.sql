-- name: test_date_trunc_partition_prune
DROP DATABASE IF EXISTS test_date_trunc_partition_prune;
CREATE DATABASE test_date_trunc_partition_prune;
use test_date_trunc_partition_prune;

CREATE TABLE `date_trunc_test` (
  `pk` int(11) NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `col1` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`pk`, `dt`)
PARTITION BY date_trunc('month', dt)
DISTRIBUTED BY HASH(`pk`)
PROPERTIES (
"replication_num" = "1"
);

-- Insert test data covering multiple months
INSERT INTO date_trunc_test VALUES 
(1, '2020-01-15 10:30:00', 'jan'),
(2, '2020-01-25 14:20:00', 'jan'),
(3, '2020-02-10 09:15:00', 'feb'),
(4, '2020-02-28 16:45:00', 'feb'),
(5, '2020-03-05 11:00:00', 'mar'),
(6, '2020-03-20 13:30:00', 'mar'),
(7, '2020-04-12 08:20:00', 'apr'),
(8, '2020-04-28 17:10:00', 'apr'),
(9, '2020-06-15 12:40:00', 'jun'),
(10, '2020-07-08 15:25:00', 'jul');

-- Test EQ aligned with month start + column predicate
select pk, dt from date_trunc_test where date_trunc('month', dt) = '2020-01-01' and dt < '2020-01-20' order by pk;

-- Test EQ unaligned (should be empty due to unaligned constant)
select pk, dt from date_trunc_test where date_trunc('month', dt) = '2020-01-15 12:00:00' and dt >= '2020-01-01' order by pk;

-- Test GE aligned: >= Feb 1st and before Mar 15th
select pk, dt from date_trunc_test where date_trunc('month', dt) >= '2020-02-01' and dt < '2020-03-15' order by pk;

-- Test GE unaligned: >= Feb 15th means from Mar 1st, combined with dt < Apr 1st
select pk, dt from date_trunc_test where date_trunc('month', dt) >= '2020-02-15' and dt < '2020-04-01' order by pk;

-- Test GT: > Jan means from Feb 1st, combined with dt < Mar 1st
select pk, dt from date_trunc_test where date_trunc('month', dt) > '2020-01-01' and dt < '2020-03-01' order by pk;

-- Test LE aligned: <= Mar means up to Apr 1st (exclusive), combined with dt >= Feb 15th
select pk, dt from date_trunc_test where date_trunc('month', dt) <= '2020-03-01' and dt >= '2020-02-15' order by pk;

-- Test LE unaligned: <= Mar 15th means up to Apr 1st (exclusive), combined with dt >= Mar 1st
select pk, dt from date_trunc_test where date_trunc('month', dt) <= '2020-03-15' and dt >= '2020-03-01' order by pk;

-- Test LT aligned: < Apr means before Apr 1st, combined with dt >= Mar 1st
select pk, dt from date_trunc_test where date_trunc('month', dt) < '2020-04-01' and dt >= '2020-03-01' order by pk;

-- Test LT unaligned: < Apr 15th means before May 1st, combined with dt >= Apr 1st
select pk, dt from date_trunc_test where date_trunc('month', dt) < '2020-04-15' and dt >= '2020-04-01' order by pk;

-- Test different granularities

-- Day granularity table
CREATE TABLE `date_trunc_day_test` (
  `pk` int(11) NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `col1` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`pk`, `dt`)
PARTITION BY date_trunc('day', dt)
DISTRIBUTED BY HASH(`pk`)
PROPERTIES (
"replication_num" = "1"
);

INSERT INTO date_trunc_day_test VALUES 
(1, '2020-01-15 10:30:00', 'day1'),
(2, '2020-01-15 14:20:00', 'day1'),
(3, '2020-01-16 09:15:00', 'day2'),
(4, '2020-01-17 16:45:00', 'day3');

-- Test day granularity EQ aligned
select pk, dt from date_trunc_day_test where date_trunc('day', dt) = '2020-01-15' and dt >= '2020-01-15 12:00:00' order by pk;

-- Test day granularity EQ unaligned (should be empty)
select pk, dt from date_trunc_day_test where date_trunc('day', dt) = '2020-01-15 10:30:00' and dt >= '2020-01-15' order by pk;

-- Year granularity table
CREATE TABLE `date_trunc_year_test` (
  `pk` int(11) NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `col1` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`pk`, `dt`)
PARTITION BY date_trunc('year', dt)
DISTRIBUTED BY HASH(`pk`)
PROPERTIES (
"replication_num" = "1"
);

INSERT INTO date_trunc_year_test VALUES 
(1, '2020-03-15 10:30:00', 'year2020'),
(2, '2020-08-25 14:20:00', 'year2020'),
(3, '2021-02-10 09:15:00', 'year2021'),
(4, '2021-11-28 16:45:00', 'year2021');

-- Test year granularity EQ aligned
select pk, dt from date_trunc_year_test where date_trunc('year', dt) = '2020-01-01' and dt < '2020-06-01' order by pk;

-- Test year granularity EQ unaligned (should be empty)
select pk, dt from date_trunc_year_test where date_trunc('year', dt) = '2020-01-01 01:00:00' and dt >= '2020-01-01' order by pk;

-- Quarter granularity table
CREATE TABLE `date_trunc_quarter_test` (
  `pk` int(11) NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `col1` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`pk`, `dt`)
PARTITION BY date_trunc('quarter', dt)
DISTRIBUTED BY HASH(`pk`)
PROPERTIES (
"replication_num" = "1"
);

INSERT INTO date_trunc_quarter_test VALUES 
(1, '2020-01-15 10:30:00', 'q1'),
(2, '2020-02-25 14:20:00', 'q1'),
(3, '2020-04-10 09:15:00', 'q2'),
(4, '2020-05-28 16:45:00', 'q2'),
(5, '2020-07-15 11:00:00', 'q3'),
(6, '2020-08-20 13:30:00', 'q3'),
(7, '2020-10-12 08:20:00', 'q4'),
(8, '2020-11-28 17:10:00', 'q4');

-- Test quarter granularity GE unaligned: >= Q2 15th means from Q3 start (July 1st)
select pk, dt from date_trunc_quarter_test where date_trunc('quarter', dt) >= '2020-05-15' and dt < '2020-10-01' order by pk;

-- Test quarter granularity LE aligned: <= Q2 start means before Q3 start (July 1st)
select pk, dt from date_trunc_quarter_test where date_trunc('quarter', dt) <= '2020-04-01' and dt >= '2020-04-01' order by pk;

-- Edge cases: test with multiple date_trunc predicates (should still work correctly)
select pk, dt from date_trunc_test where date_trunc('month', dt) >= '2020-02-01' and date_trunc('month', dt) < '2020-04-01' and dt >= '2020-02-15' order by pk;

-- Test minute granularity
CREATE TABLE `date_trunc_minute_test` (
  `pk` int(11) NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `col1` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`pk`, `dt`)
PARTITION BY date_trunc('minute', dt)
DISTRIBUTED BY HASH(`pk`)
PROPERTIES (
"replication_num" = "1"
);

INSERT INTO date_trunc_minute_test VALUES 
(1, '2020-01-15 10:30:15', 'min30'),
(2, '2020-01-15 10:30:45', 'min30'),
(3, '2020-01-15 10:31:20', 'min31'),
(4, '2020-01-15 10:32:50', 'min32');

-- Test minute granularity EQ aligned
select pk, dt from date_trunc_minute_test where date_trunc('minute', dt) = '2020-01-15 10:30:00' and dt >= '2020-01-15 10:30:30' order by pk;

-- Test minute granularity EQ unaligned (should be empty)
select pk, dt from date_trunc_minute_test where date_trunc('minute', dt) = '2020-01-15 10:30:15' and dt >= '2020-01-15 10:30:00' order by pk;

-- Test second granularity
CREATE TABLE `date_trunc_second_test` (
  `pk` int(11) NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `col1` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`pk`, `dt`)
PARTITION BY date_trunc('second', dt)
DISTRIBUTED BY HASH(`pk`)
PROPERTIES (
"replication_num" = "1"
);

INSERT INTO date_trunc_second_test VALUES 
(1, '2020-01-15 10:30:15', 'sec15'),
(2, '2020-01-15 10:30:16', 'sec16'),
(3, '2020-01-15 10:30:17', 'sec17');

-- Test second granularity EQ aligned
select pk, dt from date_trunc_second_test where date_trunc('second', dt) = '2020-01-15 10:30:15' and dt = '2020-01-15 10:30:15' order by pk;

-- Test second granularity GE
select pk, dt from date_trunc_second_test where date_trunc('second', dt) >= '2020-01-15 10:30:16' and dt <= '2020-01-15 10:30:17' order by pk;

-- Test hour granularity
CREATE TABLE `date_trunc_hour_test` (
  `pk` int(11) NOT NULL COMMENT "",
  `dt` datetime NOT NULL COMMENT "",
  `col1` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`pk`, `dt`)
PARTITION BY date_trunc('hour', dt)
DISTRIBUTED BY HASH(`pk`)
PROPERTIES (
"replication_num" = "1"
);

INSERT INTO date_trunc_hour_test VALUES 
(1, '2020-01-15 10:15:30', 'hour10'),
(2, '2020-01-15 10:45:20', 'hour10'),
(3, '2020-01-15 11:20:15', 'hour11'),
(4, '2020-01-15 11:50:45', 'hour11');

-- Test hour granularity EQ aligned
select pk, dt from date_trunc_hour_test where date_trunc('hour', dt) = '2020-01-15 10:00:00' and dt >= '2020-01-15 10:30:00' order by pk;

-- Test hour granularity EQ unaligned (should be empty)
select pk, dt from date_trunc_hour_test where date_trunc('hour', dt) = '2020-01-15 10:15:00' and dt >= '2020-01-15 10:00:00' order by pk;

