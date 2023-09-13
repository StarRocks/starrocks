-- CII
CREATE TABLE if not exists CII
(
cii_pk0 INT NOT NULL,
cii_pk1 INT NOT NULL,
cii_c0 INT NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`cii_pk0`, `cii_pk1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`cii_pk0`, `cii_pk1`) BUCKETS 10
PROPERTIES(
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "default"
);
