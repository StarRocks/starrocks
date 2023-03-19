[sql]
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    sum(l_quantity) / count(l_quantity) as avg_qty,
    sum(l_extendedprice) / count(l_extendedprice) as avg_price,
    sum(l_discount) / count(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
group by
    l_returnflag,
    l_linestatus
[result]
AGGREGATE ([GLOBAL] aggregate [{118: sum=sum(109: sum_qty), 119: sum=sum(111: sum_base_price), 120: sum=sum(115: sum_disc_price), 121: sum=sum(116: sum_charge), 122: count=sum(110: count_qty), 123: count=sum(112: count_base_price), 124: sum=sum(113: sum_discount), 125: count=sum(114: count_discount), 126: count=sum(117: count_order)}] group by [[107: l_returnflag, 108: l_linestatus]] having [null]
    SCAN (mv[lineitem_agg_mv1] columns[107: l_returnflag, 108: l_linestatus, 109: sum_qty, 110: count_qty, 111: sum_base_price, 112: count_base_price, 113: sum_discount, 114: count_discount, 115: sum_disc_price, 116: sum_charge, 117: count_order] predicate[null])
[end]

