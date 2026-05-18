-- name: test_ds_theta_sketch_ops
CREATE TABLE t1 (
  id BIGINT NOT NULL,
  grp VARCHAR(8) NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4;

insert into t1 select generate_series, "A" from table(generate_series(1, 50000));
insert into t1 select generate_series, "B" from table(generate_series(25001, 75000));

-- accumulate: produces serialized compact theta sketch per group
select grp, length(ds_theta_accumulate(id)) > 0 from t1 group by grp order by grp;

-- estimate(accumulate) == count_distinct (within theta error)
select abs(ds_theta_estimate(ds_theta_accumulate(id)) - ds_theta_count_distinct(id)) < 5000 from t1;

-- combine: union sketches at the agg layer, estimate matches union of inputs
with sk as (
  select grp, ds_theta_accumulate(id) as s from t1 group by grp
)
select ds_theta_estimate(ds_theta_combine(s)) between 70000 and 80000 from sk;

-- pairwise scalar union
with sk as (
  select grp, ds_theta_accumulate(id) as s from t1 group by grp
),
pivoted as (
  select max(case when grp='A' then s end) as a, max(case when grp='B' then s end) as b from sk
)
select ds_theta_estimate(ds_theta_union(a, b)) between 70000 and 80000 from pivoted;

-- pairwise scalar intersection
with sk as (
  select grp, ds_theta_accumulate(id) as s from t1 group by grp
),
pivoted as (
  select max(case when grp='A' then s end) as a, max(case when grp='B' then s end) as b from sk
)
select ds_theta_estimate(ds_theta_intersect(a, b)) between 22500 and 27500 from pivoted;

-- pairwise A-not-B
with sk as (
  select grp, ds_theta_accumulate(id) as s from t1 group by grp
),
pivoted as (
  select max(case when grp='A' then s end) as a, max(case when grp='B' then s end) as b from sk
)
select ds_theta_estimate(ds_theta_a_not_b(a, b)) between 22500 and 27500 from pivoted;

-- NULL propagation: all set ops return NULL when either input is NULL
select ds_theta_estimate(ds_theta_union(NULL, ds_theta_accumulate(id))) from t1;
select ds_theta_intersect(NULL, NULL) is NULL;
select ds_theta_a_not_b(NULL, ds_theta_accumulate(id)) is NULL from t1;
