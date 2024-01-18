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
<<<<<<< HEAD
TOP-N (order by [[19: count DESC NULLS LAST, 18: count DESC NULLS LAST]])
    TOP-N (order by [[19: count DESC NULLS LAST, 18: count DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{19: count=count(19: count)}] group by [[18: count]] having [null]
            EXCHANGE SHUFFLE[18]
                AGGREGATE ([LOCAL] aggregate [{19: count=count()}] group by [[18: count]] having [null]
                    AGGREGATE ([GLOBAL] aggregate [{18: count=count(9: o_orderkey)}] group by [[1: c_custkey]] having [null]
                        RIGHT OUTER JOIN (join-predicate [11: o_custkey = 1: c_custkey] post-join-predicate [null])
                            EXCHANGE SHUFFLE[11]
                                SCAN (table[orders] columns[17: o_comment, 9: o_orderkey, 11: o_custkey] predicate[NOT 17: o_comment LIKE %unusual%deposits%])
                            EXCHANGE SHUFFLE[1]
                                SCAN (table[customer] columns[1: c_custkey] predicate[null])
=======
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
>>>>>>> branch-2.5-mrs
[end]

