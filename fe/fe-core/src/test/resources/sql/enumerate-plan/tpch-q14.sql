[planCount]
3
[plan-1]
AGGREGATE ([GLOBAL] aggregate [{30: sum=sum(28: case), 31: sum=sum(29: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [2: L_PARTKEY = 18: P_PARTKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[2]
                SCAN (columns[2: L_PARTKEY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 11: L_SHIPDATE] predicate[11: L_SHIPDATE >= 1997-02-01 AND 11: L_SHIPDATE < 1997-03-01])
            EXCHANGE SHUFFLE[18]
                SCAN (columns[18: P_PARTKEY, 22: P_TYPE] predicate[null])
[end]
[plan-2]
AGGREGATE ([GLOBAL] aggregate [{30: sum=sum(30: sum), 31: sum=sum(31: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{30: sum=sum(if(22: P_TYPE LIKE PROMO%, 34: multiply, 0)), 31: sum=sum(29: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [18: P_PARTKEY = 2: L_PARTKEY] post-join-predicate [null])
                SCAN (columns[18: P_PARTKEY, 22: P_TYPE] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[2: L_PARTKEY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 11: L_SHIPDATE] predicate[11: L_SHIPDATE >= 1997-02-01 AND 11: L_SHIPDATE < 1997-03-01])
[end]
[plan-3]
AGGREGATE ([GLOBAL] aggregate [{30: sum=sum(30: sum), 31: sum=sum(31: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{30: sum=sum(if(22: P_TYPE LIKE PROMO%, 34: multiply, 0)), 31: sum=sum(29: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [18: P_PARTKEY = 2: L_PARTKEY] post-join-predicate [null])
                SCAN (columns[18: P_PARTKEY, 22: P_TYPE] predicate[null])
                EXCHANGE SHUFFLE[2]
                    EXCHANGE SHUFFLE[2]
                        SCAN (columns[2: L_PARTKEY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 11: L_SHIPDATE] predicate[11: L_SHIPDATE >= 1997-02-01 AND 11: L_SHIPDATE < 1997-03-01])
[end]