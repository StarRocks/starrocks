-- Seed data for ADBC catalog E2E tests
-- Run against DuckDB before starting sqlflite:
--
-- Usage:
--   1. duckdb test.db < setup_flightsql_data.sql
--   2. docker run -p 31337:31337 -v $(pwd)/test.db:/data/test.db voltrondata/sqlflite:latest /data/test.db
--   3. Configure sr.conf: external_flightsql_ip=127.0.0.1, external_flightsql_port=31337
--   4. cd test && python3 run.py -d sql/test_adbc_catalog -v

CREATE SCHEMA IF NOT EXISTS test_db;

-- Basic types table for query tests
CREATE TABLE test_db.test_types (
    id INTEGER,
    name VARCHAR,
    value DOUBLE,
    created_date DATE,
    flag BOOLEAN
);
INSERT INTO test_db.test_types VALUES
    (1, 'alpha', 1.5, '2024-01-01', true),
    (2, 'beta', 2.5, '2024-01-02', false),
    (3, 'gamma', 3.5, '2024-01-03', true),
    (4, 'delta', 4.5, '2024-01-04', false),
    (5, 'epsilon', 5.5, '2024-01-05', true);

-- Join target table
CREATE TABLE test_db.test_join (
    id INTEGER,
    category VARCHAR,
    score INTEGER
);
INSERT INTO test_db.test_join VALUES
    (1, 'A', 100),
    (2, 'B', 200),
    (3, 'A', 150);
