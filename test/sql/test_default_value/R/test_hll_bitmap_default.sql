-- name: test_hll_bitmap_default
DROP DATABASE IF EXISTS test_hll_bitmap_default;
-- result:
-- !result
CREATE DATABASE test_hll_bitmap_default;
-- result:
-- !result
USE test_hll_bitmap_default;
-- result:
-- !result
CREATE TABLE test_bitmap_create (
    id INT,
    bm BITMAP BITMAP_UNION DEFAULT ""
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_bitmap_create (id) VALUES (1);
-- result:
-- !result
INSERT INTO test_bitmap_create (id) VALUES (2);
-- result:
-- !result
INSERT INTO test_bitmap_create VALUES (3, to_bitmap(100));
-- result:
-- !result
SELECT id, bitmap_to_string(bm), bitmap_count(bm) FROM test_bitmap_create ORDER BY id;
-- result:
1		0
2		0
3	100	1
-- !result
CREATE TABLE test_hll_create (
    id INT,
    h HLL HLL_UNION DEFAULT ""
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_hll_create (id) VALUES (1);
-- result:
-- !result
INSERT INTO test_hll_create (id) VALUES (2);
-- result:
-- !result
INSERT INTO test_hll_create VALUES (3, hll_hash(100));
-- result:
-- !result
SELECT id, hll_cardinality(h) FROM test_hll_create ORDER BY id;
-- result:
1	0
2	0
3	1
-- !result
CREATE TABLE test_bitmap_alter (
    id INT,
    name VARCHAR(50)
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_bitmap_alter VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- result:
-- !result
ALTER TABLE test_bitmap_alter ADD COLUMN bm BITMAP BITMAP_UNION DEFAULT "";
-- result:
-- !result
SELECT id, name, bitmap_to_string(bm), bitmap_count(bm) FROM test_bitmap_alter ORDER BY id;
-- result:
1	alice		0
2	bob		0
3	charlie		0
-- !result
CREATE TABLE test_hll_alter (
    id INT,
    name VARCHAR(50)
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_hll_alter VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- result:
-- !result
ALTER TABLE test_hll_alter ADD COLUMN h HLL HLL_UNION DEFAULT "";
-- result:
-- !result
SELECT id, name, hll_cardinality(h) FROM test_hll_alter ORDER BY id;
-- result:
1	alice	0
2	bob	0
3	charlie	0
-- !result
CREATE TABLE test_bitmap_nonempty (
    id INT,
    bm BITMAP BITMAP_UNION DEFAULT "test"
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: Invalid default value for \'bm\': Type \'BITMAP\' only supports empty string "" as default value.')
-- !result
CREATE TABLE test_hll_nonempty (
    id INT,
    h HLL HLL_UNION DEFAULT "test"
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: Invalid default value for \'h\': Type \'HLL\' only supports empty string "" as default value.')
-- !result
CREATE TABLE test_bitmap_with_default (
    id INT,
    name VARCHAR(50),
    bm BITMAP BITMAP_UNION DEFAULT ""
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
CREATE TABLE test_bitmap_without_default (
    id INT,
    name VARCHAR(50),
    bm BITMAP BITMAP_UNION
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_bitmap_with_default (id, name) VALUES (1, 'alice');
-- result:
-- !result
INSERT INTO test_bitmap_without_default (id, name) VALUES (1, 'alice');
-- result:
-- !result
SELECT 'with_default' as type, id, name, bitmap_count(bm) FROM test_bitmap_with_default
UNION ALL
SELECT 'without_default', id, name, bitmap_count(bm) FROM test_bitmap_without_default
ORDER BY type, id;
-- result:
with_default	1	alice	0
without_default	1	alice	0
-- !result