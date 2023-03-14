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
AGGREGATE ([GLOBAL] aggregate [{129: sum=sum(129: sum), 130: sum=sum(130: sum), 131: sum=sum(131: sum), 132: count=sum(132: count), 133: count=sum(133: count), 134: sum=sum(134: sum), 135: count=sum(135: count), 136: count=sum(136: count), 128: sum=sum(128: sum)}] group by [[80: l_returnflag, 81: l_linestatus]] having [null]
    EXCHANGE SHUFFLE[80, 81]
        AGGREGATE ([LOCAL] aggregate [{129: sum=sum(84: sum_base_price), 130: sum=sum(88: sum_disc_price), 131: sum=sum(89: sum_charge), 132: count=sum(83: count_qty), 133: count=sum(85: count_base_price), 134: sum=sum(86: sum_discount), 135: count=sum(87: count_discount), 136: count=sum(90: count_order), 128: sum=sum(82: sum_qty)}] group by [[80: l_returnflag, 81: l_linestatus]] having [null]
            SCAN (mv[lineitem_agg_mv1] columns[80: l_returnflag, 81: l_linestatus, 82: sum_qty, 83: count_qty, 84: sum_base_price, 85: count_base_price, 86: sum_discount, 87: count_discount, 88: sum_disc_price, 89: sum_charge, 90: count_order] predicate[null])
[end]

