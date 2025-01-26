-- name: test_agg_state_table_basic_with_mv
CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
PARTITION BY (dt)
DISTRIBUTED BY HASH(id) BUCKETS 4;

insert into t1 select generate_series, generate_series, generate_series % 10, "2024-07-24" from table(generate_series(1, 100));

 -- test synchronized materailized view
CREATE MATERIALIZED VIEW test_mv1 
as
select 
    dt,
    min(id) as min_id,
    max(id) as max_id,
    sum(id) as sum_id,
    bitmap_union(to_bitmap(id)) as bitmap_union_id,
    hll_union(hll_hash(id)) as hll_union_id,
    percentile_union(percentile_hash(id)) as percentile_union_id,
    ds_hll_count_distinct_union(ds_hll_count_distinct_state(id)) as hll_id,
    avg_union(avg_state(id)) as avg_id,
    array_agg_union(array_agg_state(id)) as array_agg_id,
    min_by_union(min_by_state(province, id)) as min_by_province_id
from t1
group by dt;

function: wait_materialized_view_finish()
set new_planner_optimize_timeout=10000;

function: print_hit_materialized_view("select dt, min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5), ds_hll_count_distinct(id), avg(id), array_agg(id), min_by(province, id) from t1 where dt >= '2024-01-01' group by dt;", "test_mv1")
function: print_hit_materialized_view("select dt, min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5) from t1 where dt >= '2024-01-01' group by dt;", "test_mv1")
function: print_hit_materialized_view("select dt, ds_hll_count_distinct(id), avg(id), array_agg(id), min_by(province, id) from t1 where dt >= '2024-01-01' group by dt;", "test_mv1")
function: print_hit_materialized_view("select min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5) from t1 where dt >= '2024-01-01';", "test_mv1")
function: print_hit_materialized_view("select ds_hll_count_distinct(id), avg(id), array_agg(id), min_by(province, id) from t1 where dt >= '2024-01-01';", "test_mv1")
function: print_hit_materialized_view("select min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5), ds_hll_count_distinct(id), avg(id), array_agg(id), min_by(province, id) from t1 where dt >= '2024-01-01';", "test_mv1")

select dt, min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5), ds_hll_count_distinct(id), avg(id), min_by(province, id) from t1 where dt >= '2024-01-01' group by dt;
select dt, min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5) from t1 where dt >= '2024-01-01' group by dt;
select dt, ds_hll_count_distinct(id), avg(id), min_by(province, id) from t1 where dt >= '2024-01-01' group by dt;
select min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5) from t1 where dt >= '2024-01-01';
select ds_hll_count_distinct(id), avg(id), min_by(province, id) from t1 where dt >= '2024-01-01';
select min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5), ds_hll_count_distinct(id), avg(id), min_by(province, id) from t1 where dt >= '2024-01-01';

drop materialized view test_mv1;

-- test async synchronized materailized view
CREATE MATERIALIZED VIEW test_mv2
PARTITION BY (dt)
DISTRIBUTED BY RANDOM
as
select 
    dt,
    min(id) as min_id,
    max(id) as max_id,
    sum(id) as sum_id,
    bitmap_union(to_bitmap(id)) as bitmap_union_id,
    hll_union(hll_hash(id)) as hll_union_id,
    percentile_union(percentile_hash(id)) as percentile_union_id,
    ds_hll_count_distinct_union(ds_hll_count_distinct_state(id)) as hll_id,
    avg_union(avg_state(id)) as avg_id,
    array_agg_union(array_agg_state(id)) as array_agg_id,
    min_by_union(min_by_state(province, id)) as min_by_province_id
from t1
group by dt;

refresh materialized view test_mv2 with sync mode;

function: print_hit_materialized_view("select dt, min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5), ds_hll_count_distinct(id), avg(id), array_agg(id), min_by(province, id) from t1 where dt >= '2024-01-01' group by dt;", "test_mv2")
function: print_hit_materialized_view("select dt, min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5) from t1 where dt >= '2024-01-01' group by dt;", "test_mv2")
function: print_hit_materialized_view("select dt, ds_hll_count_distinct(id), avg(id), array_agg(id), min_by(province, id) from t1 where dt >= '2024-01-01' group by dt;", "test_mv2")
function: print_hit_materialized_view("select min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5) from t1 where dt >= '2024-01-01';", "test_mv2")
function: print_hit_materialized_view("select ds_hll_count_distinct(id), avg(id), array_agg(id), min_by(province, id) from t1 where dt >= '2024-01-01';", "test_mv2")
function: print_hit_materialized_view("select min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5), ds_hll_count_distinct(id), avg(id), array_agg(id), min_by(province, id) from t1 where dt >= '2024-01-01';", "test_mv2")

select dt, min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5), ds_hll_count_distinct(id), avg(id), min_by(province, id) from t1 where dt >= '2024-01-01' group by dt;
select dt, min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5) from t1 where dt >= '2024-01-01' group by dt;
select dt, ds_hll_count_distinct(id), avg(id), min_by(province, id) from t1 where dt >= '2024-01-01' group by dt;
select min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5) from t1 where dt >= '2024-01-01';
select ds_hll_count_distinct(id), avg(id), min_by(province, id) from t1 where dt >= '2024-01-01';
select min(id), max(id), sum(id), bitmap_union_count(to_bitmap(id)), hll_union_agg(hll_hash(id)), percentile_approx(id, 0.5), ds_hll_count_distinct(id), avg(id), min_by(province, id) from t1 where dt >= '2024-01-01';

drop materialized view test_mv2;