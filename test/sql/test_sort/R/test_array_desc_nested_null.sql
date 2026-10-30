-- name: test_array_desc_nested_null
DROP DATABASE IF EXISTS test_array_desc_nested_null;
-- result:
-- !result
CREATE DATABASE test_array_desc_nested_null;
-- result:
-- !result
USE test_array_desc_nested_null;
-- result:
-- !result
CREATE TABLE t_arr (
    id INT,
    arr ARRAY<INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t_arr VALUES
    (1, [NULL]), (2, [5]), (3, [4]), (4, [3]),
    (5, [NULL, 2]), (6, [7]), (7, [NULL, 9]), (8, [8]);
-- result:
-- !result
set pipeline_dop = 1;
-- result:
-- !result
set chunk_size = 2;
-- result:
-- !result
set full_sort_max_buffered_rows = 1073741824;
-- result:
-- !result
SELECT id FROM t_arr ORDER BY arr;
-- result:
1
5
7
4
3
2
6
8
-- !result
SELECT id FROM t_arr ORDER BY arr DESC;
-- result:
8
6
2
3
4
7
5
1
-- !result
SELECT id FROM t_arr ORDER BY arr DESC NULLS FIRST;
-- result:
7
5
1
8
6
2
3
4
-- !result
set full_sort_max_buffered_rows = 2;
-- result:
-- !result
SELECT id FROM t_arr ORDER BY arr;
-- result:
1
5
7
4
3
2
6
8
-- !result
SELECT id FROM t_arr ORDER BY arr DESC;
-- result:
8
6
2
3
4
7
5
1
-- !result
SELECT id FROM t_arr ORDER BY arr DESC NULLS FIRST;
-- result:
7
5
1
8
6
2
3
4
-- !result
set full_sort_max_buffered_rows = 1073741824;
-- result:
-- !result
SELECT id FROM t_arr ORDER BY arr DESC LIMIT 3;
-- result:
8
6
2
-- !result
set full_sort_max_buffered_rows = 2;
-- result:
-- !result
SELECT id FROM t_arr ORDER BY arr DESC LIMIT 3;
-- result:
8
6
2
-- !result
set full_sort_max_buffered_rows = 1073741824;
-- result:
-- !result
set enable_spill = true;
-- result:
-- !result
set spill_mode = "force";
-- result:
-- !result
SELECT id FROM t_arr ORDER BY arr;
-- result:
1
5
7
4
3
2
6
8
-- !result
SELECT id FROM t_arr ORDER BY arr DESC;
-- result:
8
6
2
3
4
7
5
1
-- !result
SELECT id FROM t_arr ORDER BY arr DESC LIMIT 3;
-- result:
8
6
2
-- !result
set enable_spill = false;
-- result:
-- !result
SELECT array_agg(id ORDER BY arr) FROM t_arr;
-- result:
[1,5,7,4,3,2,6,8]
-- !result
SELECT array_agg(id ORDER BY arr DESC) FROM t_arr;
-- result:
[8,6,2,3,4,7,5,1]
-- !result
SELECT array_agg(id ORDER BY arr DESC NULLS FIRST) FROM t_arr;
-- result:
[7,5,1,8,6,2,3,4]
-- !result
SELECT group_concat(cast(id as string) ORDER BY arr DESC SEPARATOR ',') FROM t_arr;
-- result:
8,6,2,3,4,7,5,1
-- !result
CREATE TABLE t_struct (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t_struct VALUES
    (1, row(NULL, 1)), (2, row(1, 1)), (3, row(NULL, 9)),
    (4, row(2, 0)), (5, row(1, NULL)), (6, row(3, 3));
-- result:
-- !result
set full_sort_max_buffered_rows = 1073741824;
-- result:
-- !result
SELECT id FROM t_struct ORDER BY s;
-- result:
1
3
5
2
4
6
-- !result
SELECT id FROM t_struct ORDER BY s DESC;
-- result:
6
4
2
5
3
1
-- !result
set full_sort_max_buffered_rows = 2;
-- result:
-- !result
SELECT id FROM t_struct ORDER BY s;
-- result:
1
3
5
2
4
6
-- !result
SELECT id FROM t_struct ORDER BY s DESC;
-- result:
6
4
2
5
3
1
-- !result
DROP DATABASE test_array_desc_nested_null;
-- result:
-- !result