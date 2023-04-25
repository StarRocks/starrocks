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
select * from cte union all select * from cte;
[result]
UNION
    SCAN (mv[partsupp_mv] columns[174: n_name, 175: p_mfgr, 176: p_size, 177: p_type, 178: ps_partkey, 179: ps_suppkey, 180: ps_supplycost, 181: r_name, 182: s_acctbal, 183: s_address, 184: s_comment, 185: s_name, 186: s_nationkey, 187: s_phone, 188: ps_partvalue] predicate[null])
    SCAN (mv[partsupp_mv] columns[189: n_name, 190: p_mfgr, 191: p_size, 192: p_type, 193: ps_partkey, 194: ps_suppkey, 195: ps_supplycost, 196: r_name, 197: s_acctbal, 198: s_address, 199: s_comment, 200: s_name, 201: s_nationkey, 202: s_phone, 203: ps_partvalue] predicate[null])
[end]

