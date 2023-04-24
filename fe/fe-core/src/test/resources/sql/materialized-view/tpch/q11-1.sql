[sql]
with cte
as (

     select
              n_name,
              p_mfgr,p_size,p_type,
              ps_partkey, ps_suppkey,ps_supplycost,
              r_name,
              s_acctbal,s_address,s_comment,s_name,s_nationkey,s_phone,
              ps_supplycost * ps_availqty as ps_partvalue
   from
              partsupp
                  inner join supplier
                  inner join part
                  inner join nation
                  inner join region
   where
                  partsupp.ps_suppkey = supplier.s_suppkey
     and partsupp.ps_partkey=part.p_partkey
     and supplier.s_nationkey=nation.n_nationkey
     and nation.n_regionkey=region.r_regionkey)
select a.n_name as a1, b.n_name as a2, a.ps_supplycost as b1, b.ps_supplycost as b2 from cte as a join cte as b on a.n_name=b.n_name;
[result]
INNER JOIN (join-predicate [52: n_name = 81: n_name] post-join-predicate [null])
    EXCHANGE SHUFFLE[52]
        SCAN (mv[partsupp_mv] columns[174: n_name, 180: ps_supplycost] predicate[null])
    EXCHANGE SHUFFLE[81]
        SCAN (mv[partsupp_mv] columns[159: n_name, 165: ps_supplycost] predicate[null])
[end]

