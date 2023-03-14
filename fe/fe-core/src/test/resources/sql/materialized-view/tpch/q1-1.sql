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
AGGREGATE ([GLOBAL] aggregate [{129: sum=sum(129: sum), 130: sum=sum(130: sum), 131: sum=sum(131: sum), 132: count=sum(132: count), 133: count=sum(133: count), 134: sum=sum(134: sum), 135: count=sum(135: count), 136: count=sum(136: count), 128: sum=sum(128: sum)}] group by [[107: l_returnflag, 108: l_linestatus]] having [null]
    EXCHANGE SHUFFLE[107, 108]
        AGGREGATE ([LOCAL] aggregate [{129: sum=sum(111: sum_base_price), 130: sum=sum(115: sum_disc_price), 131: sum=sum(116: sum_charge), 132: count=sum(110: count_qty), 133: count=sum(112: count_base_price), 134: sum=sum(113: sum_discount), 135: count=sum(114: count_discount), 136: count=sum(117: count_order), 128: sum=sum(109: sum_qty)}] group by [[107: l_returnflag, 108: l_linestatus]] having [null]
            SCAN (mv[lineitem_agg_mv1] columns[107: l_returnflag, 108: l_linestatus, 109: sum_qty, 110: count_qty, 111: sum_base_price, 112: count_base_price, 113: sum_discount, 114: count_discount, 115: sum_disc_price, 116: sum_charge, 117: count_order] predicate[null])
[end]

