-- name: test_hll_bitmap_default

-- Test HLL and BITMAP default values after code change
-- Default value "" creates empty object (same behavior as not specifying default)

DROP DATABASE IF EXISTS test_hll_bitmap_default;
CREATE DATABASE test_hll_bitmap_default;
USE test_hll_bitmap_default;

-- =====================================================
-- Test 1: BITMAP with DEFAULT "" in CREATE TABLE
-- =====================================================
CREATE TABLE test_bitmap_create (
    id INT,
    bm BITMAP BITMAP_UNION DEFAULT ""
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Insert only id (should use default)
INSERT INTO test_bitmap_create (id) VALUES (1);
INSERT INTO test_bitmap_create (id) VALUES (2);

-- Insert with explicit bitmap
INSERT INTO test_bitmap_create VALUES (3, to_bitmap(100));

SELECT id, bitmap_to_string(bm), bitmap_count(bm) FROM test_bitmap_create ORDER BY id;


-- =====================================================
-- Test 2: HLL with DEFAULT "" in CREATE TABLE  
-- =====================================================
CREATE TABLE test_hll_create (
    id INT,
    h HLL HLL_UNION DEFAULT ""
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Insert only id (should use default)
INSERT INTO test_hll_create (id) VALUES (1);
INSERT INTO test_hll_create (id) VALUES (2);

-- Insert with explicit hll
INSERT INTO test_hll_create VALUES (3, hll_hash(100));

SELECT id, hll_cardinality(h) FROM test_hll_create ORDER BY id;


-- =====================================================
-- Test 3: ALTER TABLE ADD COLUMN with BITMAP DEFAULT
-- =====================================================
CREATE TABLE test_bitmap_alter (
    id INT,
    name VARCHAR(50)
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO test_bitmap_alter VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');

-- Add BITMAP column with default (Fast Schema Evolution)
ALTER TABLE test_bitmap_alter ADD COLUMN bm BITMAP BITMAP_UNION DEFAULT "";

-- Read old data with new column
SELECT id, name, bitmap_to_string(bm), bitmap_count(bm) FROM test_bitmap_alter ORDER BY id;


-- =====================================================
-- Test 4: ALTER TABLE ADD COLUMN with HLL DEFAULT
-- =====================================================
CREATE TABLE test_hll_alter (
    id INT,
    name VARCHAR(50)
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO test_hll_alter VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');

-- Add HLL column with default (Fast Schema Evolution)
ALTER TABLE test_hll_alter ADD COLUMN h HLL HLL_UNION DEFAULT "";

-- Read old data with new column
SELECT id, name, hll_cardinality(h) FROM test_hll_alter ORDER BY id;


-- =====================================================
-- Test 5: Verify non-empty default value is rejected
-- =====================================================
-- This should fail (only empty string "" is allowed)
CREATE TABLE test_bitmap_nonempty (
    id INT,
    bm BITMAP BITMAP_UNION DEFAULT "test"
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- This should also fail
CREATE TABLE test_hll_nonempty (
    id INT,
    h HLL HLL_UNION DEFAULT "test"
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- =====================================================
-- Test 6: Compare behavior with and without DEFAULT
-- =====================================================
CREATE TABLE test_bitmap_with_default (
    id INT,
    name VARCHAR(50),
    bm BITMAP BITMAP_UNION DEFAULT ""
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

CREATE TABLE test_bitmap_without_default (
    id INT,
    name VARCHAR(50),
    bm BITMAP BITMAP_UNION
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Both should behave the same when inserting partial columns
INSERT INTO test_bitmap_with_default (id, name) VALUES (1, 'alice');
INSERT INTO test_bitmap_without_default (id, name) VALUES (1, 'alice');

SELECT 'with_default' as type, id, name, bitmap_count(bm) FROM test_bitmap_with_default
UNION ALL
SELECT 'without_default', id, name, bitmap_count(bm) FROM test_bitmap_without_default
ORDER BY type, id;
