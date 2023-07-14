[sql]
select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1996-01-01'
  and l_discount between 0.02 and 0.04
  and l_quantity < 24 ;
[result]
AGGREGATE ([GLOBAL] aggregate [{19: sum=sum(19: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
<<<<<<< HEAD
        AGGREGATE ([LOCAL] aggregate [{19: sum=sum(18: expr)}] group by [[]] having [null]
            SCAN (columns[5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 11: L_SHIPDATE] predicate[11: L_SHIPDATE >= 1995-01-01 AND 11: L_SHIPDATE < 1996-01-01 AND 7: L_DISCOUNT >= 0.02 AND 7: L_DISCOUNT <= 0.04 AND 5: L_QUANTITY < 24.0])
=======
        AGGREGATE ([LOCAL] aggregate [{19: sum=sum(multiply(6: L_EXTENDEDPRICE, 7: L_DISCOUNT))}] group by [[]] having [null]
            SCAN (columns[5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 11: L_SHIPDATE] predicate[11: L_SHIPDATE >= 1995-01-01 AND 11: L_SHIPDATE < 1996-01-01 AND 7: L_DISCOUNT >= 0.02 AND 7: L_DISCOUNT <= 0.04 AND 5: L_QUANTITY < 24])
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
[end]

