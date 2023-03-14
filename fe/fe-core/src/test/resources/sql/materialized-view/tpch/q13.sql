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
TOP-N (order by [[19: count DESC NULLS LAST, 18: count DESC NULLS LAST]])
    TOP-N (order by [[19: count DESC NULLS LAST, 18: count DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{19: count=count(19: count)}] group by [[18: count]] having [null]
            EXCHANGE SHUFFLE[18]
                AGGREGATE ([LOCAL] aggregate [{19: count=count()}] group by [[18: count]] having [null]
                    AGGREGATE ([GLOBAL] aggregate [{89: count=sum(89: count)}] group by [[81: c_custkey]] having [null]
                        EXCHANGE SHUFFLE[81]
                            AGGREGATE ([LOCAL] aggregate [{89: count=sum(83: c_count)}] group by [[81: c_custkey]] having [null]
                                SCAN (mv[customer_order_mv_agg_mv1] columns[81: c_custkey, 82: o_comment, 83: c_count] predicate[NOT 82: o_comment LIKE %unusual%deposits%])
[end]

