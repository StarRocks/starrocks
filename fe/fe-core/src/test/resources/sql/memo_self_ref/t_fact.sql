CREATE TABLE `t_fact` (
  `id` bigint(20) NOT NULL,
  `part_key` bigint(20) NOT NULL,
  `name` varchar(1048576) NULL,
  `s1` varchar(1048576) NULL,
  `s2` varchar(30) NULL,
  `d1` double NULL,
  `t1` datetime NULL
) ENGINE=OLAP
PRIMARY KEY(`id`, `part_key`)
DISTRIBUTED BY HASH(`part_key`, `id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"unique_constraints" = "default_catalog.test_prune.t_fact.id,part_key"
);
