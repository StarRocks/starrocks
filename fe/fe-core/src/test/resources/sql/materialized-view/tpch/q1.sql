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
where
    l_shipdate <= date '1998-12-01'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus ;
[result]
TOP-N (order by [[10: l_returnflag ASC NULLS FIRST, 11: l_linestatus ASC NULLS FIRST]])
    TOP-N (order by [[10: l_returnflag ASC NULLS FIRST, 11: l_linestatus ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{129: sum=sum(129: sum), 130: sum=sum(130: sum), 131: sum=sum(131: sum), 132: count=sum(132: count), 133: count=sum(133: count), 134: sum=sum(134: sum), 135: count=sum(135: count), 136: count=sum(136: count), 128: sum=sum(128: sum)}] group by [[104: l_returnflag, 105: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[104, 105]
                AGGREGATE ([LOCAL] aggregate [{129: sum=sum(108: sum_base_price), 130: sum=sum(112: sum_disc_price), 131: sum=sum(113: sum_charge), 132: count=sum(107: count_qty), 133: count=sum(109: count_base_price), 134: sum=sum(110: sum_discount), 135: count=sum(111: count_discount), 136: count=sum(114: count_order), 128: sum=sum(106: sum_qty)}] group by [[104: l_returnflag, 105: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv1] columns[103: l_shipdate, 104: l_returnflag, 105: l_linestatus, 106: sum_qty, 107: count_qty, 108: sum_base_price, 109: count_base_price, 110: sum_discount, 111: count_discount, 112: sum_disc_price, 113: sum_charge, 114: count_order] predicate[103: l_shipdate <= 1998-12-01])
[end]

