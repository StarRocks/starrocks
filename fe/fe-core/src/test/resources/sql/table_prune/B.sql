-- B
CREATE TABLE if not exists B
(
b_pk INT NOT NULL,
b_ci_fk INT NOT NULL,
b_cii_fk0 INT NOT NULL,
b_cii_fk1 INT NOT NULL,
b_c0 INT NOT NULL,
b_c1 INT NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`b_pk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`b_pk`) BUCKETS 10
PROPERTIES(
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "default"
);
