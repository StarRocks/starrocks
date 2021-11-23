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
TOP-N (order by [[21: count DESC NULLS LAST, 20: count DESC NULLS LAST]])
    TOP-N (order by [[21: count DESC NULLS LAST, 20: count DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{21: count=count(21: count)}] group by [[20: count]] having [null]
            EXCHANGE SHUFFLE[20]
                AGGREGATE ([LOCAL] aggregate [{21: count=count()}] group by [[20: count]] having [null]
                    AGGREGATE ([GLOBAL] aggregate [{20: count=count(20: count)}] group by [[1: C_CUSTKEY]] having [null]
                        AGGREGATE ([LOCAL] aggregate [{20: count=count(10: O_ORDERKEY)}] group by [[1: C_CUSTKEY]] having [null]
                            LEFT OUTER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    SCAN (columns[18: O_COMMENT, 10: O_ORDERKEY, 11: O_CUSTKEY] predicate[NOT 18: O_COMMENT LIKE %unusual%deposits%])
[end]

