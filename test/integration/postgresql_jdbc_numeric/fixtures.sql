CREATE TABLE {schema}.values_ok (id int, amount numeric);
INSERT INTO {schema}.values_ok VALUES
(1, 1), (2, 2), (3, 10), (4, -1.2), (5, 0.000000000000000001),
(6, NULL), (7, 1.1234567890123456780000),
(8, 99999999999999999999.999999999999999999),
(9, -99999999999999999999.999999999999999999), (10, 0.000000000000000000000);
CREATE TABLE {schema}.small (id int, amount numeric);
INSERT INTO {schema}.small VALUES (1, 1), (2, 2), (3, 10), (4, 1.00), (5, NULL);
CREATE VIEW {schema}.numeric_view AS SELECT id, amount + 0 AS amount FROM {schema}.small;
CREATE TABLE {schema}.many (id int, amount numeric);
INSERT INTO {schema}.many SELECT i, CASE WHEN i % 7 = 0 THEN NULL ELSE i::numeric/100 END
FROM generate_series(1,2059) i;
CREATE TABLE {schema}.bounded (id int, amount numeric(30,2), exact numeric(38,18), n bigint);
INSERT INTO {schema}.bounded VALUES (1, 123456789012345678901.25, 1.25, 2), (2, 0.25, 2.5, 3);
CREATE TABLE {schema}.cancelled (amount numeric);
INSERT INTO {schema}.cancelled VALUES (-1e20), (1e20);
