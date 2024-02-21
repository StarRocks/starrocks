[sql]
select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
        s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
        select
            *
        from
            lineitem l2
        where
                l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
    )
  and not exists (
        select
            *
        from
            lineitem l3
        where
                l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          and l3.l_receiptdate > l3.l_commitdate
    )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
group by
    s_name
order by
    numwait desc,
    s_name limit 100;
[planCount]
2
[plan-1]
TOP-N (order by [[77: count DESC NULLS LAST, 2: S_NAME ASC NULLS FIRST]])
    TOP-N (order by [[77: count DESC NULLS LAST, 2: S_NAME ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{77: count=count()}] group by [[2: S_NAME]] having [null]
            EXCHANGE SHUFFLE[2]
                LEFT ANTI JOIN (join-predicate [9: L_ORDERKEY = 59: L_ORDERKEY AND 61: L_SUPPKEY != 11: L_SUPPKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[9]
                        RIGHT SEMI JOIN (join-predicate [41: L_ORDERKEY = 9: L_ORDERKEY AND 43: L_SUPPKEY != 11: L_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[41: L_ORDERKEY, 43: L_SUPPKEY] predicate[null])
                            EXCHANGE SHUFFLE[9]
                                INNER JOIN (join-predicate [26: O_ORDERKEY = 9: L_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[26: O_ORDERKEY, 28: O_ORDERSTATUS] predicate[28: O_ORDERSTATUS = F])
                                    EXCHANGE SHUFFLE[9]
                                        INNER JOIN (join-predicate [11: L_SUPPKEY = 1: S_SUPPKEY] post-join-predicate [null])
                                            SCAN (columns[20: L_COMMITDATE, 21: L_RECEIPTDATE, 9: L_ORDERKEY, 11: L_SUPPKEY] predicate[21: L_RECEIPTDATE > 20: L_COMMITDATE])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [4: S_NATIONKEY = 36: N_NATIONKEY] post-join-predicate [null])
                                                    SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 4: S_NATIONKEY] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        SCAN (columns[36: N_NATIONKEY, 37: N_NAME] predicate[37: N_NAME = CANADA])
                    EXCHANGE SHUFFLE[59]
                        SCAN (columns[70: L_COMMITDATE, 71: L_RECEIPTDATE, 59: L_ORDERKEY, 61: L_SUPPKEY] predicate[71: L_RECEIPTDATE > 70: L_COMMITDATE])
[end]


