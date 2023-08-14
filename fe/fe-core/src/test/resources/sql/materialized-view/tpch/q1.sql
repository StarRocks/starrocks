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
        AGGREGATE ([GLOBAL] aggregate [{113: sum=sum(33: sum_base_price), 114: sum=sum(35: sum_disc_price), 115: sum=sum(36: sum_charge), 116: count=sum(31: total_cnt), 117: count=sum(31: total_cnt), 118: count=sum(31: total_cnt), 119: sum=sum(34: sum_discount), 120: count=sum(31: total_cnt), 112: sum=sum(32: sum_qty)}] group by [[29: l_returnflag, 30: l_linestatus]] having [null]
            SCAN (mv[lineitem_agg_mv1] columns[28: l_shipdate, 29: l_returnflag, 30: l_linestatus, 31: total_cnt, 32: sum_qty, 33: sum_base_price, 34: sum_discount, 35: sum_disc_price, 36: sum_charge] predicate[28: l_shipdate <= 1998-12-01])
[end]

