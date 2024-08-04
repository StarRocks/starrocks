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
        AGGREGATE ([GLOBAL] aggregate [{117: sum=sum(117: sum), 118: sum=sum(118: sum), 119: sum=sum(119: sum), 120: sum=sum(120: sum), 121: count=sum(121: count), 122: count=sum(122: count), 123: sum=sum(123: sum), 124: count=sum(124: count), 125: count=sum(125: count)}] group by [[38: l_returnflag, 39: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[38, 39]
                AGGREGATE ([LOCAL] aggregate [{117: sum=sum(40: sum_qty), 118: sum=sum(42: sum_base_price), 119: sum=sum(46: sum_disc_price), 120: sum=sum(47: sum_charge), 121: count=sum(41: count_qty), 122: count=sum(43: count_base_price), 123: sum=sum(44: sum_discount), 124: count=sum(48: count_order), 125: count=sum(45: count_discount)}] group by [[38: l_returnflag, 39: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv1] columns[37: l_shipdate, 38: l_returnflag, 39: l_linestatus, 40: sum_qty, 41: count_qty, 42: sum_base_price, 43: count_base_price, 44: sum_discount, 45: count_discount, 46: sum_disc_price, 47: sum_charge, 48: count_order] predicate[37: l_shipdate <= 1998-12-01])
[end]

