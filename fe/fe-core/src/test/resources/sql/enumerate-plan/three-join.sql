[sql]
select SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as amount
from
    part,
    partsupp,
    lineitem
where
    p_partkey = cast(l_partkey as bigint)
    and ps_suppkey = l_suppkey
    and ps_partkey = l_partkey
    and p_name like '%peru%';
[plan-1]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            CROSS JOIN (join-predicate [null] post-join-predicate [null])
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE BROADCAST
                SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-2]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            CROSS JOIN (join-predicate [null] post-join-predicate [null])
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE BROADCAST
                SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-3]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            CROSS JOIN (join-predicate [null] post-join-predicate [null])
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE BROADCAST
                SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-4]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            CROSS JOIN (join-predicate [null] post-join-predicate [null])
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE BROADCAST
                SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-5]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            CROSS JOIN (join-predicate [null] post-join-predicate [null])
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[18]
                SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-6]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            CROSS JOIN (join-predicate [null] post-join-predicate [null])
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[18]
                SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-7]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast] post-join-predicate [null])
            EXCHANGE SHUFFLE[36]
                SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[37]
                INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE SHUFFLE[18]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-8]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast] post-join-predicate [null])
            EXCHANGE SHUFFLE[36]
                SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[37]
                INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[18]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
                    EXCHANGE SHUFFLE[11]
                        SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
[plan-9]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
            INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE SHUFFLE[18]
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
            EXCHANGE BROADCAST
                SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[plan-10]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
            INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
                EXCHANGE SHUFFLE[18]
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
                EXCHANGE SHUFFLE[11]
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
            EXCHANGE BROADCAST
                SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[plan-11]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
            EXCHANGE SHUFFLE[18]
                INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[plan-12]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
            EXCHANGE SHUFFLE[18]
                INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
                    EXCHANGE SHUFFLE[36]
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[plan-13]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[18]
                INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[11]
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
[plan-14]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[18]
                INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
                    EXCHANGE SHUFFLE[36]
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[11]
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
[plan-15]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(35: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
<<<<<<< HEAD
        AGGREGATE ([LOCAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
=======
        AGGREGATE ([LOCAL] aggregate [{35: sum=sum(subtract(multiply(22: L_EXTENDEDPRICE, subtract(1, 23: L_DISCOUNT)), multiply(14: PS_SUPPLYCOST, 21: L_QUANTITY)))}] group by [[]] having [null]
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
            INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                CROSS JOIN (join-predicate [null] post-join-predicate [null])
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                EXCHANGE BROADCAST
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-16]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(35: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
<<<<<<< HEAD
        AGGREGATE ([LOCAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                CROSS JOIN (join-predicate [null] post-join-predicate [null])
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                EXCHANGE BROADCAST
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-17]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(35: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                CROSS JOIN (join-predicate [null] post-join-predicate [null])
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                EXCHANGE BROADCAST
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-18]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(35: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                CROSS JOIN (join-predicate [null] post-join-predicate [null])
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                EXCHANGE BROADCAST
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-19]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(35: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
=======
        AGGREGATE ([LOCAL] aggregate [{35: sum=sum(subtract(multiply(22: L_EXTENDEDPRICE, subtract(1, 23: L_DISCOUNT)), multiply(14: PS_SUPPLYCOST, 21: L_QUANTITY)))}] group by [[]] having [null]
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
            INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                CROSS JOIN (join-predicate [null] post-join-predicate [null])
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                EXCHANGE SHUFFLE[18]
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])
[plan-20]
AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(35: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
<<<<<<< HEAD
        AGGREGATE ([LOCAL] aggregate [{35: sum=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                CROSS JOIN (join-predicate [null] post-join-predicate [null])
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
=======
        AGGREGATE ([LOCAL] aggregate [{35: sum=sum(subtract(multiply(22: L_EXTENDEDPRICE, subtract(1, 23: L_DISCOUNT)), multiply(14: PS_SUPPLYCOST, 21: L_QUANTITY)))}] group by [[]] having [null]
            INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
                EXCHANGE SHUFFLE[18]
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[cast(18: L_PARTKEY as bigint(20)) IS NOT NULL])