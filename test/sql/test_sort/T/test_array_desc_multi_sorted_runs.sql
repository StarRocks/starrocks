-- name: test_array_desc_multi_sorted_runs

drop database if exists test_array_desc_multi_sorted_runs;
create database test_array_desc_multi_sorted_runs;
use test_array_desc_multi_sorted_runs;

-- ORDER BY an ARRAY column compares the array element by element, and the sort of every buffered run
-- has to agree with the merge of those runs on where a NULL element goes. Run the same rows through a
-- single run, which never merges, and through several merged runs, and check the output is the same.
CREATE TABLE t_arr (
    id INT,
    arr ARRAY<INT>,
    arr_nonull ARRAY<INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

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

SET pipeline_dop = 1;
SET chunk_size = 2;

-- A single sorted run, the merge is never reached.
SET full_sort_max_buffered_rows = 1073741824;
SELECT id, arr FROM t_arr ORDER BY arr DESC;
SELECT id, arr FROM t_arr ORDER BY arr;
SELECT id, arr_nonull FROM t_arr ORDER BY arr_nonull DESC;
SELECT id FROM t_arr ORDER BY id DESC;

-- Five sorted runs that have to be merged. Every result above must be reproduced exactly.
SET full_sort_max_buffered_rows = 2;
SELECT id, arr FROM t_arr ORDER BY arr DESC;
SELECT id, arr FROM t_arr ORDER BY arr;
SELECT id, arr_nonull FROM t_arr ORDER BY arr_nonull DESC;
SELECT id FROM t_arr ORDER BY id DESC;
