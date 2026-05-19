-- name: test_ds_theta_sketch_ops
CREATE TABLE t1 (
  id BIGINT NOT NULL,
  grp VARCHAR(8) NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4;

insert into t1 select generate_series, "A" from table(generate_series(1, 50000));
insert into t1 select generate_series, "B" from table(generate_series(25001, 75000));

-- ds_theta_accumulate produces serialized compact theta sketch per group
select grp, cast(length(ds_theta_accumulate(id)) > 0 as int) from t1 group by grp order by grp;

-- ds_theta_estimate is a SCALAR fn on serialized sketches; matches count_distinct within theta error
with sk as (select ds_theta_accumulate(id) as s from t1)
select cast(abs(ds_theta_estimate(s) - 50000) < 5000 as int) from sk
union all
select cast(abs((select ds_theta_count_distinct(id) from t1) - 50000) < 5000 as int);

-- ds_theta_combine is an AGGREGATE over a VARBINARY column: union sketches across rows
with grp_sk as (select grp, ds_theta_accumulate(id) as s from t1 group by grp)
select cast(ds_theta_estimate(ds_theta_combine(s)) between 70000 and 80000 as int) from grp_sk;

-- pairwise scalar set ops — estimate composes directly (no state-type plumbing needed)
with grp_sk as (select grp, ds_theta_accumulate(id) as s from t1 group by grp),
     pivoted as (
       select max(case when grp='A' then s end) as a, max(case when grp='B' then s end) as b from grp_sk
     )
select cast(ds_theta_estimate(ds_theta_union(a, b)) between 70000 and 80000 as int),
       cast(ds_theta_estimate(ds_theta_intersect(a, b)) between 22500 and 27500 as int),
       cast(ds_theta_estimate(ds_theta_a_not_b(a, b)) between 22500 and 27500 as int)
from pivoted;

-- ds_theta_combine handles raw VARBINARY produced by scalar set ops too
-- (proves the AggState/raw-varbinary gap is closed)
with grp_sk as (select grp, ds_theta_accumulate(id) as s from t1 group by grp),
     pivoted as (
       select max(case when grp='A' then s end) as a, max(case when grp='B' then s end) as b from grp_sk
     ),
     unioned as (
       select ds_theta_union(a, b) as u, ds_theta_intersect(a, b) as i from pivoted
     ),
     stacked as (select u as s from unioned union all select i from unioned)
select cast(ds_theta_estimate(ds_theta_combine(s)) between 70000 and 80000 as int) from stacked;

-- NULL propagation
select ds_theta_estimate(NULL);
select cast(ds_theta_union(NULL, NULL) is NULL as int);
select cast(ds_theta_intersect(NULL, NULL) is NULL as int);
select cast(ds_theta_a_not_b(NULL, NULL) is NULL as int);
