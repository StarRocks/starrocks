-- FI
CREATE TABLE if not exists FI
(
fi_pk INT NOT NULL,
fi_c0 INT NOT NULL,
fi_c1 INT NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`fi_pk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`fi_pk`) BUCKETS 10
PROPERTIES(
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "default"
);
