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
    l_linestatus ;
[plan-1]
TOP-N (order by [[9: L_RETURNFLAG ASC NULLS FIRST, 10: L_LINESTATUS ASC NULLS FIRST]])
    TOP-N (order by [[9: L_RETURNFLAG ASC NULLS FIRST, 10: L_LINESTATUS ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{20: sum(5: L_QUANTITY)=sum(5: L_QUANTITY), 21: sum(6: L_EXTENDEDPRICE)=sum(6: L_EXTENDEDPRICE), 22: sum(18: expr)=sum(18: expr), 23: sum(19: expr)=sum(19: expr), 24: avg(5: L_QUANTITY)=avg(5: L_QUANTITY), 25: avg(6: L_EXTENDEDPRICE)=avg(6: L_EXTENDEDPRICE), 26: avg(7: L_DISCOUNT)=avg(7: L_DISCOUNT), 27: count()=count()}] group by [[9: L_RETURNFLAG, 10: L_LINESTATUS]] having [null]
            EXCHANGE SHUFFLE[9, 10]
                SCAN (columns[5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 8: L_TAX, 9: L_RETURNFLAG, 10: L_LINESTATUS, 11: L_SHIPDATE] predicate[11: L_SHIPDATE <= 1998-12-01])
[end]
[plan-2]
TOP-N (order by [[9: L_RETURNFLAG ASC NULLS FIRST, 10: L_LINESTATUS ASC NULLS FIRST]])
    TOP-N (order by [[9: L_RETURNFLAG ASC NULLS FIRST, 10: L_LINESTATUS ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{20: sum(5: L_QUANTITY)=sum(20: sum(5: L_QUANTITY)), 21: sum(6: L_EXTENDEDPRICE)=sum(21: sum(6: L_EXTENDEDPRICE)), 22: sum(18: expr)=sum(22: sum(18: expr)), 23: sum(19: expr)=sum(23: sum(19: expr)), 24: avg(5: L_QUANTITY)=avg(24: avg(5: L_QUANTITY)), 25: avg(6: L_EXTENDEDPRICE)=avg(25: avg(6: L_EXTENDEDPRICE)), 26: avg(7: L_DISCOUNT)=avg(26: avg(7: L_DISCOUNT)), 27: count()=count(27: count())}] group by [[9: L_RETURNFLAG, 10: L_LINESTATUS]] having [null]
            EXCHANGE SHUFFLE[9, 10]
                AGGREGATE ([LOCAL] aggregate [{20: sum(5: L_QUANTITY)=sum(5: L_QUANTITY), 21: sum(6: L_EXTENDEDPRICE)=sum(6: L_EXTENDEDPRICE), 22: sum(18: expr)=sum(18: expr), 23: sum(19: expr)=sum(19: expr), 24: avg(5: L_QUANTITY)=avg(5: L_QUANTITY), 25: avg(6: L_EXTENDEDPRICE)=avg(6: L_EXTENDEDPRICE), 26: avg(7: L_DISCOUNT)=avg(7: L_DISCOUNT), 27: count()=count()}] group by [[9: L_RETURNFLAG, 10: L_LINESTATUS]] having [null]
                    SCAN (columns[5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 8: L_TAX, 9: L_RETURNFLAG, 10: L_LINESTATUS, 11: L_SHIPDATE] predicate[11: L_SHIPDATE <= 1998-12-01])
[end]