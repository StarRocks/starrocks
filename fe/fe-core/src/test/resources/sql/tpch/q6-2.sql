[sql]
select
  sum(l_extendedprice * l_discount) as revenue
from
  lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;
[result]
AGGREGATE ([GLOBAL] aggregate [{19: sum=sum(19: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{19: sum=sum(multiply(6: L_EXTENDEDPRICE, 7: L_DISCOUNT))}] group by [[]] having [null]
            SCAN (columns[5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 11: L_SHIPDATE] predicate[11: L_SHIPDATE >= 1994-01-01 AND 11: L_SHIPDATE < 1995-01-01 AND 7: L_DISCOUNT >= 0.05 AND 7: L_DISCOUNT <= 0.07 AND 5: L_QUANTITY < 24])
[end]
