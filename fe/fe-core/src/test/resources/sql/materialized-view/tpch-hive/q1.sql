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
        AGGREGATE ([GLOBAL] aggregate [{113: count=sum(113: count), 103: sum=sum(103: sum), 104: sum=sum(104: sum), 105: sum=sum(105: sum), 106: sum=sum(106: sum), 107: sum=sum(107: sum), 108: count=sum(108: count), 109: sum=sum(109: sum), 110: count=sum(110: count), 111: sum=sum(111: sum), 112: count=sum(112: count)}] group by [[83: l_returnflag, 84: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[83, 84]
                AGGREGATE ([LOCAL] aggregate [{113: count=sum(93: count_order), 103: sum=sum(85: sum_qty), 104: sum=sum(87: sum_base_price), 105: sum=sum(91: sum_disc_price), 106: sum=sum(92: sum_charge), 107: sum=sum(85: sum_qty), 108: count=sum(86: count_qty), 109: sum=sum(87: sum_base_price), 110: count=sum(88: count_base_price), 111: sum=sum(89: sum_discount), 112: count=sum(90: count_discount)}] group by [[83: l_returnflag, 84: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv1] columns[82: l_shipdate, 83: l_returnflag, 84: l_linestatus, 85: sum_qty, 86: count_qty, 87: sum_base_price, 88: count_base_price, 89: sum_discount, 90: count_discount, 91: sum_disc_price, 92: sum_charge, 93: count_order] predicate[82: l_shipdate <= 1998-12-01])
[end]

