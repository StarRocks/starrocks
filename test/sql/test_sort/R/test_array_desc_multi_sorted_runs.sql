-- name: test_array_desc_multi_sorted_runs
drop database if exists test_array_desc_multi_sorted_runs;
-- result:
-- !result
create database test_array_desc_multi_sorted_runs;
-- result:
-- !result
use test_array_desc_multi_sorted_runs;
-- result:
-- !result
CREATE TABLE t_arr (
    id INT,
    arr ARRAY<INT>,
    arr_nonull ARRAY<INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t_arr VALUES
    (1, [NULL], [1]),
    (2, [5], [5]),
    (3, [4], [4]),
    (4, [3], [3]),
    (5, [NULL, 2], [9, 2]),
    (6, [7], [7]),
    (7, [NULL, 9], [6]),
    (8, [8], [8]),
    (9, NULL, [2, 2]);
-- result:
-- !result
SET pipeline_dop = 1;
-- result:
-- !result
SET chunk_size = 2;
-- result:
-- !result
SET full_sort_max_buffered_rows = 1073741824;
-- result:
-- !result
SELECT id, arr FROM t_arr ORDER BY arr DESC;
-- result:
7	[null,9]
5	[null,2]
1	[null]
8	[8]
6	[7]
2	[5]
3	[4]
4	[3]
9	None
-- !result
SELECT id, arr FROM t_arr ORDER BY arr;
-- result:
9	None
1	[null]
5	[null,2]
7	[null,9]
4	[3]
3	[4]
2	[5]
6	[7]
8	[8]
-- !result
SELECT id, arr_nonull FROM t_arr ORDER BY arr_nonull DESC;
-- result:
5	[9,2]
8	[8]
6	[7]
7	[6]
2	[5]
3	[4]
4	[3]
9	[2,2]
1	[1]
-- !result
SELECT id FROM t_arr ORDER BY id DESC;
-- result:
9
8
7
6
5
4
3
2
1
-- !result
SET full_sort_max_buffered_rows = 2;
-- result:
-- !result
SELECT id, arr FROM t_arr ORDER BY arr DESC;
-- result:
7	[null,9]
5	[null,2]
1	[null]
8	[8]
6	[7]
2	[5]
3	[4]
4	[3]
9	None
-- !result
SELECT id, arr FROM t_arr ORDER BY arr;
-- result:
9	None
1	[null]
5	[null,2]
7	[null,9]
4	[3]
3	[4]
2	[5]
6	[7]
8	[8]
-- !result
SELECT id, arr_nonull FROM t_arr ORDER BY arr_nonull DESC;
-- result:
5	[9,2]
8	[8]
6	[7]
7	[6]
2	[5]
3	[4]
4	[3]
9	[2,2]
1	[1]
-- !result
SELECT id FROM t_arr ORDER BY id DESC;
-- result:
9
8
7
6
5
4
3
2
1
-- !result