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
<<<<<<< HEAD
        AGGREGATE ([GLOBAL] aggregate [{129: count=sum(46: total_cnt), 121: sum=sum(47: sum_qty), 122: sum=sum(48: sum_base_price), 123: sum=sum(50: sum_disc_price), 124: sum=sum(51: sum_charge), 125: count=sum(46: total_cnt), 126: count=sum(46: total_cnt), 127: count=sum(46: total_cnt), 128: sum=sum(49: sum_discount)}] group by [[44: l_returnflag, 45: l_linestatus]] having [null]
            SCAN (mv[lineitem_agg_mv1] columns[43: l_shipdate, 44: l_returnflag, 45: l_linestatus, 46: total_cnt, 47: sum_qty, 48: sum_base_price, 49: sum_discount, 50: sum_disc_price, 51: sum_charge] predicate[43: l_shipdate <= 1998-12-01])
=======
        AGGREGATE ([GLOBAL] aggregate [{113: count=sum(91: total_cnt), 114: sum=sum(93: sum_base_price), 115: count=sum(91: total_cnt), 116: sum=sum(94: sum_discount), 117: count=sum(91: total_cnt), 118: count=sum(91: total_cnt), 108: sum=sum(92: sum_qty), 109: sum=sum(93: sum_base_price), 110: sum=sum(95: sum_disc_price), 111: sum=sum(96: sum_charge), 112: sum=sum(92: sum_qty)}] group by [[89: l_returnflag, 90: l_linestatus]] having [null]
            SCAN (mv[lineitem_agg_mv1] columns[88: l_shipdate, 89: l_returnflag, 90: l_linestatus, 91: total_cnt, 92: sum_qty, 93: sum_base_price, 94: sum_discount, 95: sum_disc_price, 96: sum_charge] predicate[88: l_shipdate <= 1998-12-01])
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
[end]

