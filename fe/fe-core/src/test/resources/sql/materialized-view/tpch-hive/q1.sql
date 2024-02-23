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
        AGGREGATE ([GLOBAL] aggregate [{113: count=sum(113: count), 114: sum=sum(114: sum), 115: count=sum(115: count), 116: count=sum(116: count), 108: sum=sum(108: sum), 109: sum=sum(109: sum), 110: sum=sum(110: sum), 111: sum=sum(111: sum), 112: count=sum(112: count)}] group by [[34: l_returnflag, 35: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[34, 35]
                AGGREGATE ([LOCAL] aggregate [{113: count=sum(39: count_base_price), 114: sum=sum(40: sum_discount), 115: count=sum(44: count_order), 116: count=sum(41: count_discount), 108: sum=sum(36: sum_qty), 109: sum=sum(38: sum_base_price), 110: sum=sum(42: sum_disc_price), 111: sum=sum(43: sum_charge), 112: count=sum(37: count_qty)}] group by [[34: l_returnflag, 35: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv1] columns[33: l_shipdate, 34: l_returnflag, 35: l_linestatus, 36: sum_qty, 37: count_qty, 38: sum_base_price, 39: count_base_price, 40: sum_discount, 41: count_discount, 42: sum_disc_price, 43: sum_charge, 44: count_order] predicate[33: l_shipdate <= 1998-12-01])
[end]

