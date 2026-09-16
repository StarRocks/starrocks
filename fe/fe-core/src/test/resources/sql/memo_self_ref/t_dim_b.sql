CREATE TABLE `t_dim_b` (
  `id` bigint(20) NOT NULL,
  `part_key` bigint(20) NOT NULL,
  `b1` varchar(65533) NULL,
  `b2` varchar(65533) NULL,
  `b3` double NULL,
  `b4` datetime NULL
) ENGINE=OLAP
PRIMARY KEY(`id`, `part_key`)
DISTRIBUTED BY HASH(`part_key`, `id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1"
);
