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
        AGGREGATE ([GLOBAL] aggregate [{409: count=sum(409: count)}] group by [[157: s_name]] having [null]
            EXCHANGE SHUFFLE[157]
                AGGREGATE ([LOCAL] aggregate [{409: count=sum(160: cnt_star)}] group by [[157: s_name]] having [null]
                    SCAN (mv[query21_mv] columns[157: s_name, 158: o_orderstatus, 159: n_name, 160: cnt_star] predicate[158: o_orderstatus = F AND 159: n_name = CANADA])
[end]

