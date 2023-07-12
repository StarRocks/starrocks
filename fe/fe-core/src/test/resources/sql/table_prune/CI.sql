-- CI
CREATE TABLE if not exists CI
(
ci_pk INT NOT NULL,
ci_c0 INT NOT NULL,
ci_c1 INT NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`ci_pk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ci_pk`) BUCKETS 10
PROPERTIES(
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "default"
);
