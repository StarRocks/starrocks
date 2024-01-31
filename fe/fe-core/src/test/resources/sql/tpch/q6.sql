[result]
AGGREGATE ([GLOBAL] aggregate [{19: sum=sum(19: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{19: sum=sum(multiply(6: L_EXTENDEDPRICE, 7: L_DISCOUNT))}] group by [[]] having [null]
            SCAN (columns[5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 11: L_SHIPDATE] predicate[11: L_SHIPDATE >= 1995-01-01 AND 11: L_SHIPDATE < 1996-01-01 AND 7: L_DISCOUNT >= 0.02 AND 7: L_DISCOUNT <= 0.04 AND 5: L_QUANTITY < 24])
[end]

