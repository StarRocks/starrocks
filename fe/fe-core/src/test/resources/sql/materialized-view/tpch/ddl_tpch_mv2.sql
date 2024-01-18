-- query1
<<<<<<< HEAD
=======
-- query9?
>>>>>>> branch-2.5-mrs
create materialized view lineitem_agg_mv1
distributed by hash(l_orderkey,
               l_shipdate,
               l_returnflag,
               l_linestatus) buckets 96
partition by l_shipdate
refresh manual
properties (
    "replication_num" = "1",
    "partition_refresh_number" = "1"
)
as select  /*+ SET_VAR(query_timeout = 7200) */
              l_orderkey,
              l_shipdate,
              l_returnflag,
              l_linestatus,
<<<<<<< HEAD
              count(1) as total_cnt,
              sum(l_quantity) as sum_qty,
              sum(l_extendedprice) as sum_base_price,
              sum(l_discount) as sum_discount,
              sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
              sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge
=======
              sum(l_quantity) as sum_qty,
              count(l_quantity) as count_qty,
              sum(l_extendedprice) as sum_base_price,
              count(l_extendedprice) as count_base_price,
              sum(l_discount) as sum_discount,
              count(l_discount) as count_discount,
              sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
              sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
              count(*) as count_order
>>>>>>> branch-2.5-mrs
   from
              lineitem
   group by
       l_orderkey,
       l_shipdate,
       l_returnflag,
       l_linestatus
;

-- query 15,20
create materialized view lineitem_agg_mv2
distributed by hash(l_suppkey, l_shipdate, l_partkey) buckets 96
partition by l_shipdate
refresh manual
properties (
    "replication_num" = "1",
    "partition_refresh_number" = "1"
)
as select  /*+ SET_VAR(query_timeout = 7200) */
              l_suppkey, l_shipdate, l_partkey,
              sum(l_quantity) as sum_qty,
<<<<<<< HEAD
              sum(l_extendedprice) as sum_base_price,
              sum(l_discount) as sum_discount,
=======
              count(l_quantity) as count_qty,
              sum(l_extendedprice) as sum_base_price,
              count(l_extendedprice) as count_base_price,
              sum(l_discount) as sum_discount,
              count(l_discount) as count_discount,
>>>>>>> branch-2.5-mrs
              sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
              sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge
   from
              lineitem
   group by
       l_suppkey, l_shipdate, l_partkey
;

create materialized view lineitem_agg_mv3
distributed by hash(l_shipdate, l_discount, l_quantity) buckets 24
partition by l_shipdate
refresh manual
properties (
    "replication_num" = "1",
    "partition_refresh_number" = "1"
)
as select  /*+ SET_VAR(query_timeout = 7200) */
              l_shipdate, l_discount, l_quantity,
              sum(l_extendedprice * l_discount) as revenue
   from
              lineitem
   group by
       l_shipdate, l_discount, l_quantity
;


-- customer_order_mv (used to match query22)
-- query22 needs avg & rollup -> sum/count
create materialized view customer_agg_mv1
distributed by hash(c_custkey) buckets 24
refresh manual
properties (
    "replication_num" = "1"
)
as select
              c_custkey,
              c_phone,
              c_acctbal,
              substring(c_phone, 1  ,2) as substring_phone,
              count(c_acctbal) as c_count,
              sum(c_acctbal) as c_sum
   from
              customer
   group by c_custkey, c_phone, c_acctbal, substring(c_phone, 1  ,2);
