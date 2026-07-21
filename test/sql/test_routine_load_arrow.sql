-- test_routine_load_arrow
-- Verify syntax for CREATE ROUTINE LOAD with format="arrow"

CREATE DATABASE IF NOT EXISTS test_arrow_db;
USE test_arrow_db;

CREATE TABLE IF NOT EXISTS arrow_table (
    id BIGINT,
    name VARCHAR(50),
    value DOUBLE
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

CREATE ROUTINE LOAD test_arrow_db.arrow_load_job ON arrow_table
PROPERTIES
(
    "format" = "arrow",
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "10",
    "max_error_number" = "100"
)
FROM KAFKA
(
    "kafka_broker_list" = "127.0.0.1:9092",
    "kafka_topic" = "arrow_topic",
    "property.group.id" = "arrow_group"
);

SHOW ROUTINE LOAD FOR arrow_load_job;

STOP ROUTINE LOAD FOR arrow_load_job;
DROP TABLE arrow_table;
DROP DATABASE test_arrow_db;
