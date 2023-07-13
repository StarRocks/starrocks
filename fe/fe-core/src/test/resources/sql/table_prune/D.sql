-- D
CREATE TABLE if not exists D
(
d_pk INT NOT NULL,
d_e_fk INT NOT NULL,
d_c0 INT NOT NULL,
d_c1 INT NOT NULL,
d_c2 INT NOT NULL,
d_c3 INT NOT NULL,
d_c4 INT NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`d_pk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`d_pk`) BUCKETS 10
PROPERTIES(
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "default"
);
