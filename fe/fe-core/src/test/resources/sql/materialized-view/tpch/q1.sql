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
        AGGREGATE ([GLOBAL] aggregate [{118: count=sum(106: count_qty), 119: sum=sum(105: sum_qty), 120: count=sum(108: count_base_price), 121: sum=sum(107: sum_base_price), 122: sum=sum(109: sum_discount), 123: sum=sum(111: sum_disc_price), 124: count=sum(110: count_discount), 125: sum=sum(112: sum_charge), 126: count=sum(113: count_order)}] group by [[103: l_returnflag, 104: l_linestatus]] having [null]
            SCAN (mv[lineitem_agg_mv1] columns[102: l_shipdate, 103: l_returnflag, 104: l_linestatus, 105: sum_qty, 106: count_qty, 107: sum_base_price, 108: count_base_price, 109: sum_discount, 110: count_discount, 111: sum_disc_price, 112: sum_charge, 113: count_order] predicate[102: l_shipdate <= 1998-12-01])
[end]

