-- A
CREATE TABLE if not exists A
(
a_pk INT NOT NULL,
a_b_fk INT NOT NULL,
a_c0 INT NOT NULL,
a_c1 INT NOT NULL,
a_c2 INT NOT NULL,
a_c3 INT NOT NULL,
a_c4 INT NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`a_pk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`a_pk`) BUCKETS 10
PROPERTIES(
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "default"
);
