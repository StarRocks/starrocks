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
        AGGREGATE ([GLOBAL] aggregate [{114: count=sum(114: count), 115: sum=sum(115: sum), 116: sum=sum(116: sum), 117: sum=sum(117: sum), 118: sum=sum(118: sum), 119: count=sum(119: count), 120: count=sum(120: count), 121: count=sum(121: count), 122: sum=sum(122: sum)}] group by [[29: l_returnflag, 30: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[29, 30]
                AGGREGATE ([LOCAL] aggregate [{114: count=sum(36: count_discount), 115: sum=sum(31: sum_qty), 116: sum=sum(33: sum_base_price), 117: sum=sum(37: sum_disc_price), 118: sum=sum(38: sum_charge), 119: count=sum(39: count_order), 120: count=sum(32: count_qty), 121: count=sum(34: count_base_price), 122: sum=sum(35: sum_discount)}] group by [[29: l_returnflag, 30: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv1] columns[28: l_shipdate, 29: l_returnflag, 30: l_linestatus, 31: sum_qty, 32: count_qty, 33: sum_base_price, 34: count_base_price, 35: sum_discount, 36: count_discount, 37: sum_disc_price, 38: sum_charge, 39: count_order] predicate[28: l_shipdate <= 1998-12-01])
[end]

