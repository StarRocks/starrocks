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
[result]
TOP-N (order by [[71: count DESC NULLS LAST, 2: s_name ASC NULLS FIRST]])
    TOP-N (order by [[71: count DESC NULLS LAST, 2: s_name ASC NULLS FIRST]])
<<<<<<< HEAD
        SCAN (mv[query21_mv] columns[108: s_name, 109: o_orderstatus, 110: n_name, 111: cnt_star] predicate[109: o_orderstatus = F AND 110: n_name = CANADA])
=======
        AGGREGATE ([GLOBAL] aggregate [{189: count=sum(189: count)}] group by [[131: s_name]] having [null]
            EXCHANGE SHUFFLE[131]
                AGGREGATE ([LOCAL] aggregate [{189: count=sum(134: cnt_star)}] group by [[131: s_name]] having [null]
                    SCAN (mv[query21_mv] columns[131: s_name, 132: o_orderstatus, 133: n_name, 134: cnt_star] predicate[132: o_orderstatus = F AND 133: n_name = CANADA])
>>>>>>> branch-2.5-mrs
[end]

