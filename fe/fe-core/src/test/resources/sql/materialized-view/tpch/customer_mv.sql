-- customer_order_mv (used to match query22)
-- query22 needs avg & rollup -> sum/count
create materialized view customer_mv
distributed by hash(c_custkey) buckets 20
refresh manual
properties (
    "replication_num" = "1"
)
as select
              c_custkey, c_phone, c_acctbal, count(1) as c_count, sum(c_acctbal) as c_sum
   from
              customer
   group by c_custkey, c_phone, c_acctbal;