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
    lineitem
where
        l_shipdate <= date '1998-12-01'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
[result]
TOP-N (order by [[10: l_returnflag ASC NULLS FIRST, 11: l_linestatus ASC NULLS FIRST]])
    TOP-N (order by [[10: l_returnflag ASC NULLS FIRST, 11: l_linestatus ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{118: count=sum(79: count_qty), 119: sum=sum(78: sum_qty), 120: count=sum(81: count_base_price), 121: sum=sum(80: sum_base_price), 122: sum=sum(82: sum_discount), 123: sum=sum(84: sum_disc_price), 124: count=sum(83: count_discount), 125: sum=sum(85: sum_charge), 126: count=sum(86: count_order)}] group by [[76: l_returnflag, 77: l_linestatus]] having [null]
            SCAN (mv[lineitem_agg_mv1] columns[75: l_shipdate, 76: l_returnflag, 77: l_linestatus, 78: sum_qty, 79: count_qty, 80: sum_base_price, 81: count_base_price, 82: sum_discount, 83: count_discount, 84: sum_disc_price, 85: sum_charge, 86: count_order] predicate[75: l_shipdate <= 1998-12-01])
[end]

