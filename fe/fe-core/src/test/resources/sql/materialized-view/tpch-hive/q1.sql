[sql]
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    hive0.tpch.lineitem
where
        l_shipdate <= date '1998-12-01'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
[result]
TOP-N (order by [[9: l_returnflag ASC NULLS FIRST, 10: l_linestatus ASC NULLS FIRST]])
    TOP-N (order by [[9: l_returnflag ASC NULLS FIRST, 10: l_linestatus ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{113: sum=sum(113: sum), 114: count=sum(114: count), 115: count=sum(115: count), 107: sum=sum(107: sum), 108: sum=sum(108: sum), 109: sum=sum(109: sum), 110: sum=sum(110: sum), 111: count=sum(111: count), 112: count=sum(112: count)}] group by [[92: l_returnflag, 93: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[92, 93]
                AGGREGATE ([LOCAL] aggregate [{113: sum=sum(98: sum_discount), 114: count=sum(99: count_discount), 115: count=sum(102: count_order), 107: sum=sum(94: sum_qty), 108: sum=sum(96: sum_base_price), 109: sum=sum(100: sum_disc_price), 110: sum=sum(101: sum_charge), 111: count=sum(95: count_qty), 112: count=sum(97: count_base_price)}] group by [[92: l_returnflag, 93: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv1] columns[91: l_shipdate, 92: l_returnflag, 93: l_linestatus, 94: sum_qty, 95: count_qty, 96: sum_base_price, 97: count_base_price, 98: sum_discount, 99: count_discount, 100: sum_disc_price, 101: sum_charge, 102: count_order] predicate[91: l_shipdate <= 1998-12-01])
[end]

