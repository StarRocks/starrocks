-- v_partsupp_mv
create materialized view v_partsupp_mv
distributed by hash(ps_partkey) buckets 20
refresh manual
properties (
    "replication_num" = "1"
)
as select
              ps_partkey, ps_availqty, ps_comment, ps_suppkey, ps_supplycost, ps_partvalue,
              s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, s_suppkey,
              p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment,
              n_nationkey, n_name, n_regionkey, n_comment,
              r_regionkey, r_name, r_comment
from
    (select
         partsupp.*,
         ps_supplycost * ps_availqty as ps_partvalue
     from
         partsupp) as v_partsupp
    inner join supplier
    inner join part
    inner join nation
    inner join region
on
  v_partsupp.ps_suppkey = supplier.s_suppkey
  and v_partsupp.ps_partkey=part.p_partkey
  and supplier.s_nationkey=nation.n_nationkey
  and nation.n_regionkey=region.r_regionkey;