CREATE TABLE part (
    p_partkey     BIGINT NOT NULL,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        CHAR(25) NOT NULL,
    p_brand       CHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        INT NOT NULL,
    p_container   CHAR(10) NOT NULL,
    p_retailprice DOUBLE NOT NULL,
    p_comment     VARCHAR(23) NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);
