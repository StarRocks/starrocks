[sql]
with  revenue0 (supplier_no, total_revenue) as (
    select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
    from
        lineitem
    where
        l_shipdate >= date '1995-01-01'
        and l_shipdate < date '1995-01-01' + interval '3' month
    group by
        l_suppkey
)
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    revenue0
where
        s_suppkey = supplier_no
  and total_revenue = (
    select
        max(total_revenue)
    from
        revenue0
)
order by
    s_suppkey;
[result]
TOP-N (order by [[19: s_suppkey ASC NULLS FIRST]])
    TOP-N (order by [[19: s_suppkey ASC NULLS FIRST]])
        INNER JOIN (join-predicate [19: s_suppkey = 30: l_suppkey] post-join-predicate [null])
            SCAN (table[supplier] columns[19: s_suppkey, 20: s_name, 21: s_address, 23: s_phone] predicate[null])
            EXCHANGE SHUFFLE[30]
                INNER JOIN (join-predicate [43: sum = 62: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{167: sum=sum(167: sum)}] group by [[68: l_suppkey]] having [167: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[68]
                            AGGREGATE ([LOCAL] aggregate [{167: sum=sum(77: sum_disc_price)}] group by [[68: l_suppkey]] having [null]
                                SCAN (mv[lineitem_agg_mv2] columns[68: l_suppkey, 69: l_shipdate, 77: sum_disc_price] predicate[69: l_shipdate <= 1995-03-31 AND 69: l_shipdate >= 1995-01-01 AND 69: l_shipdate < 1996-01-01 AND 77: sum_disc_price IS NOT NULL])
                    EXCHANGE BROADCAST
                        PREDICATE 62: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{62: max=max(62: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{62: max=max(61: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{168: sum=sum(168: sum)}] group by [[68: l_suppkey]] having [null]
                                                EXCHANGE SHUFFLE[68]
                                                    AGGREGATE ([LOCAL] aggregate [{168: sum=sum(77: sum_disc_price)}] group by [[68: l_suppkey]] having [null]
                                                        SCAN (mv[lineitem_agg_mv2] columns[68: l_suppkey, 69: l_shipdate, 77: sum_disc_price] predicate[69: l_shipdate <= 1995-03-31 AND 69: l_shipdate >= 1995-01-01 AND 69: l_shipdate < 1996-01-01])
[end]

