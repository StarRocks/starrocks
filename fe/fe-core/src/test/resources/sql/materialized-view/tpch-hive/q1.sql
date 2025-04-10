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
        AGGREGATE ([GLOBAL] aggregate [{116: sum=sum(116: sum), 117: sum=sum(117: sum), 118: sum=sum(118: sum), 119: sum=sum(119: sum), 120: count=sum(120: count), 121: count=sum(121: count), 122: sum=sum(122: sum), 123: count=sum(123: count), 124: count=sum(124: count)}] group by [[40: l_returnflag, 41: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[40, 41]
                AGGREGATE ([LOCAL] aggregate [{116: sum=sum(42: sum_qty), 117: sum=sum(44: sum_base_price), 118: sum=sum(48: sum_disc_price), 119: sum=sum(49: sum_charge), 120: count=sum(43: count_qty), 121: count=sum(45: count_base_price), 122: sum=sum(46: sum_discount), 123: count=sum(47: count_discount), 124: count=sum(50: count_order)}] group by [[40: l_returnflag, 41: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv1] columns[39: l_shipdate, 40: l_returnflag, 41: l_linestatus, 42: sum_qty, 43: count_qty, 44: sum_base_price, 45: count_base_price, 46: sum_discount, 47: count_discount, 48: sum_disc_price, 49: sum_charge, 50: count_order] predicate[39: l_shipdate <= 1998-12-01])
[end]

