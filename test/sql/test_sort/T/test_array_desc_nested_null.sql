-- name: test_array_desc_nested_null
DROP DATABASE IF EXISTS test_array_desc_nested_null;
CREATE DATABASE test_array_desc_nested_null;
USE test_array_desc_nested_null;

CREATE TABLE t_arr (
    id INT,
    arr ARRAY<INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO t_arr VALUES
    (1, [NULL]), (2, [5]), (3, [4]), (4, [3]),
    (5, [NULL, 2]), (6, [7]), (7, [NULL, 9]), (8, [8]);

set pipeline_dop = 1;
set chunk_size = 2;

-- A sort buffering everything into a single run
set full_sort_max_buffered_rows = 1073741824;
SELECT id FROM t_arr ORDER BY arr;
SELECT id FROM t_arr ORDER BY arr DESC;
SELECT id FROM t_arr ORDER BY arr DESC NULLS FIRST;

-- The same sorts, but buffered into several runs that have to be merged.
-- The order must not depend on how many runs the input was cut into.
set full_sort_max_buffered_rows = 2;
SELECT id FROM t_arr ORDER BY arr;
SELECT id FROM t_arr ORDER BY arr DESC;
SELECT id FROM t_arr ORDER BY arr DESC NULLS FIRST;

-- Top-n takes another comparison path
set full_sort_max_buffered_rows = 1073741824;
SELECT id FROM t_arr ORDER BY arr DESC LIMIT 3;
set full_sort_max_buffered_rows = 2;
SELECT id FROM t_arr ORDER BY arr DESC LIMIT 3;

-- A spilled sort merges one sorted run per spilled block
set full_sort_max_buffered_rows = 1073741824;
set enable_spill = true;
set spill_mode = "force";
SELECT id FROM t_arr ORDER BY arr;
SELECT id FROM t_arr ORDER BY arr DESC;
SELECT id FROM t_arr ORDER BY arr DESC LIMIT 3;
set enable_spill = false;

-- array_agg and group_concat sort their input through the same path
SELECT array_agg(id ORDER BY arr) FROM t_arr;
SELECT array_agg(id ORDER BY arr DESC) FROM t_arr;
SELECT array_agg(id ORDER BY arr DESC NULLS FIRST) FROM t_arr;
SELECT group_concat(cast(id as string) ORDER BY arr DESC SEPARATOR ',') FROM t_arr;

-- A NULL nested in a STRUCT is compared the same way
CREATE TABLE t_struct (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO t_struct VALUES
    (1, row(NULL, 1)), (2, row(1, 1)), (3, row(NULL, 9)),
    (4, row(2, 0)), (5, row(1, NULL)), (6, row(3, 3));

set full_sort_max_buffered_rows = 1073741824;
SELECT id FROM t_struct ORDER BY s;
SELECT id FROM t_struct ORDER BY s DESC;
set full_sort_max_buffered_rows = 2;
SELECT id FROM t_struct ORDER BY s;
SELECT id FROM t_struct ORDER BY s DESC;

DROP DATABASE test_array_desc_nested_null;
