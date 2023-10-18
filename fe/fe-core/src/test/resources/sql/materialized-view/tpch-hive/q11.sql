[sql]
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    hive0.tpch.partsupp,
    hive0.tpch.supplier,
    hive0.tpch.nation
where
    ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'PERU'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
    select
    sum(ps_supplycost * ps_availqty) * 0.0001000000
    from
    hive0.tpch.partsupp,
    hive0.tpch.supplier,
    hive0.tpch.nation
    where
    ps_suppkey = s_suppkey
                  and s_nationkey = n_nationkey
                  and n_name = 'PERU'
    )
order by
    value desc ;
[result]
TOP-N (order by [[18: sum DESC NULLS LAST]])
    TOP-N (order by [[18: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [cast(18: sum as double) > cast(37: expr as double)] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{: sum=sum(: sum)}] group by [[: ps_partkey]] having [null]
                EXCHANGE SHUFFLE[]
                    AGGREGATE ([LOCAL] aggregate [{: sum=sum(: expr)}] group by [[: ps_partkey]] having [null]
                        SCAN (mv[partsupp_mv] columns[: n_name, : ps_partkey, : ps_partvalue] predicate[: n_name = PERU])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{36: sum=sum(36: sum)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            AGGREGATE ([LOCAL] aggregate [{36: sum=sum(35: expr)}] group by [[]] having [null]
                                SCAN (mv[partsupp_mv] columns[114: n_name, 128: ps_partvalue] predicate[114: n_name = PERU])
[end]

