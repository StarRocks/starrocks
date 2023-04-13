[sql]
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey) as c_count
        from
            customer left outer join orders on
                        c_custkey = o_custkey
                    and o_comment not like '%unusual%deposits%'
        group by
            c_custkey
    ) a
group by
    c_count
order by
    custdist desc,
    c_count desc ;
[result]
TOP-N (order by [[: count DESC NULLS LAST, : count DESC NULLS LAST]])
    TOP-N (order by [[: count DESC NULLS LAST, : count DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{: count=count(: count)}] group by [[: count]] having [null]
            EXCHANGE SHUFFLE[]
                AGGREGATE ([LOCAL] aggregate [{: count=count()}] group by [[: count]] having [null]
                    AGGREGATE ([GLOBAL] aggregate [{: count=count(: o_orderkey)}] group by [[: c_custkey]] having [null]
                        RIGHT OUTER JOIN (join-predicate [: o_custkey = : c_custkey] post-join-predicate [null])
                            EXCHANGE SHUFFLE[]
                                SCAN (table[orders] columns[: o_comment, : o_orderkey, : o_custkey] predicate[NOT : o_comment LIKE %unusual%deposits%])
                            EXCHANGE SHUFFLE[]
                                SCAN (table[customer] columns[: c_custkey] predicate[null])
[end]

