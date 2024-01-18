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
        AGGREGATE ([GLOBAL] aggregate [{115: sum=sum(115: sum), 116: count=sum(116: count), 117: sum=sum(117: sum), 118: sum=sum(118: sum), 119: sum=sum(119: sum), 120: sum=sum(120: sum), 121: count=sum(121: count), 122: count=sum(122: count), 123: count=sum(123: count)}] group by [[81: l_returnflag, 82: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[81, 82]
                AGGREGATE ([LOCAL] aggregate [{115: sum=sum(87: sum_discount), 116: count=sum(88: count_discount), 117: sum=sum(83: sum_qty), 118: sum=sum(85: sum_base_price), 119: sum=sum(89: sum_disc_price), 120: sum=sum(90: sum_charge), 121: count=sum(91: count_order), 122: count=sum(84: count_qty), 123: count=sum(86: count_base_price)}] group by [[81: l_returnflag, 82: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv1] columns[80: l_shipdate, 81: l_returnflag, 82: l_linestatus, 83: sum_qty, 84: count_qty, 85: sum_base_price, 86: count_base_price, 87: sum_discount, 88: count_discount, 89: sum_disc_price, 90: sum_charge, 91: count_order] predicate[80: l_shipdate <= 1998-12-01])
[end]

