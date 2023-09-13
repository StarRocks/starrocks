-- E
CREATE TABLE if not exists E
(
e_pk INT NOT NULL,
e_fi_fk INT NOT NULL,
e_fii_fk0 INT NOT NULL,
e_fii_fk1 INT NOT NULL,
e_c0 INT NOT NULL,
e_c1 INT NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`e_pk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`e_pk`) BUCKETS 10
PROPERTIES(
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "default"
);
