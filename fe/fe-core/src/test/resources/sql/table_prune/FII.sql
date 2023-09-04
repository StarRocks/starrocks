-- FII
CREATE TABLE if not exists FII
(
fii_pk0 INT NOT NULL,
fii_pk1 INT NOT NULL,
fii_c0 INT NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`fii_pk0`, `fii_pk1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`fii_pk0`, `fii_pk1`) BUCKETS 10
PROPERTIES(
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "default"
);
