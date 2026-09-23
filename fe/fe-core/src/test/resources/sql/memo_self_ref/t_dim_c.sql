CREATE TABLE `t_dim_c` (
  `id` bigint(20) NOT NULL,
  `part_key` bigint(20) NOT NULL,
  `c1` json NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`part_key`, `id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"foreign_key_constraints" = "(id,part_key) REFERENCES default_catalog.test_prune.t_fact(id,part_key)",
"unique_constraints" = "default_catalog.test_prune.t_dim_c.part_key,id"
);
