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
        AGGREGATE ([GLOBAL] aggregate [{19: sum=sum(19: sum), 20: sum=sum(20: sum), 21: sum=sum(21: sum), 22: sum=sum(22: sum), 23: avg=avg(23: avg), 24: avg=avg(24: avg), 25: avg=avg(25: avg), 26: count=count(26: count)}] group by [[9: l_returnflag, 10: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[9, 10]
                AGGREGATE ([LOCAL] aggregate [{19: sum=sum(5: l_quantity), 20: sum=sum(6: l_extendedprice), 21: sum=sum(17: expr), 22: sum=sum(18: expr), 23: avg=avg(5: l_quantity), 24: avg=avg(6: l_extendedprice), 25: avg=avg(7: l_discount), 26: count=count()}] group by [[9: l_returnflag, 10: l_linestatus]] having [null]
                    SCAN (columns{5,6,7,8,9,10,11,110} predicate[11: l_shipdate <= 1998-12-01])
[end]

