CREATE TABLE `t_drive` (
  `d_time` datetime NULL,
  `part_key` bigint(20) NOT NULL,
  `fk_id` bigint(20) NULL,
  `d_id` bigint(20) NULL,
  `amt` float NULL
) ENGINE=OLAP
DUPLICATE KEY(`d_time`)
DISTRIBUTED BY HASH(`part_key`, `fk_id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1"
);
