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
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[36, 12, 11]
                CROSS JOIN (join-predicate [null] post-join-predicate [null])
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                    EXCHANGE BROADCAST
                        SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
            EXCHANGE SHUFFLE[37, 19, 18]
                SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
[end]
[plan-2]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[36, 12, 11]
                CROSS JOIN (join-predicate [null] post-join-predicate [null])
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[37, 19, 18]
                SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
[end]
[plan-3]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast] post-join-predicate [null])
            EXCHANGE SHUFFLE[36]
                SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[37]
                INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[12, 11]
                        SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE SHUFFLE[19, 18]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
[end]
[plan-4]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast] post-join-predicate [null])
            EXCHANGE SHUFFLE[36]
                SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[37]
                INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE SHUFFLE[18]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
[end]
[plan-5]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [36: cast = 37: cast] post-join-predicate [null])
            EXCHANGE SHUFFLE[36]
                SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[37]
                INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[19, 18]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                    EXCHANGE SHUFFLE[12, 11]
                        SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
[end]
[plan-6]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
            INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                EXCHANGE SHUFFLE[12, 11]
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE SHUFFLE[19, 18]
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
            EXCHANGE BROADCAST
                SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-7]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
            INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE SHUFFLE[18]
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
            EXCHANGE BROADCAST
                SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-8]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
            INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
                EXCHANGE SHUFFLE[19, 18]
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                EXCHANGE SHUFFLE[12, 11]
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
            EXCHANGE BROADCAST
                SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-9]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[12, 11]
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
            EXCHANGE SHUFFLE[19, 18]
                INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-10]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[12, 11]
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
            EXCHANGE SHUFFLE[19, 18]
                INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                    EXCHANGE SHUFFLE[36]
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-11]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
            EXCHANGE SHUFFLE[18]
                INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-12]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
            SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
            EXCHANGE SHUFFLE[18]
                INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                    EXCHANGE SHUFFLE[36]
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-13]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[19, 18]
                INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[12, 11]
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
[end]
[plan-14]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[19, 18]
                INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                    EXCHANGE SHUFFLE[36]
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
            EXCHANGE SHUFFLE[12, 11]
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
[end]
[plan-15]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                EXCHANGE SHUFFLE[36, 12, 11]
                    CROSS JOIN (join-predicate [null] post-join-predicate [null])
                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                        EXCHANGE BROADCAST
                            SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE SHUFFLE[37, 19, 18]
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
[end]
[plan-16]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [36: cast = 37: cast AND 12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                EXCHANGE SHUFFLE[36, 12, 11]
                    CROSS JOIN (join-predicate [null] post-join-predicate [null])
                        SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                EXCHANGE SHUFFLE[37, 19, 18]
                    SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
[end]
[plan-17]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [36: cast = 37: cast] post-join-predicate [null])
                EXCHANGE SHUFFLE[36]
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                EXCHANGE SHUFFLE[37]
                    INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[12, 11]
                            SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                        EXCHANGE SHUFFLE[19, 18]
                            SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
[end]
[plan-18]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [36: cast = 37: cast] post-join-predicate [null])
                EXCHANGE SHUFFLE[36]
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                EXCHANGE SHUFFLE[37]
                    INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                        SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                        EXCHANGE SHUFFLE[18]
                            SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
[end]
[plan-19]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [36: cast = 37: cast] post-join-predicate [null])
                EXCHANGE SHUFFLE[36]
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                EXCHANGE SHUFFLE[37]
                    INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[19, 18]
                            SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                        EXCHANGE SHUFFLE[12, 11]
                            SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
[end]
[plan-20]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[12, 11]
                        SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE SHUFFLE[19, 18]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-21]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE SHUFFLE[18]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-22]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[19, 18]
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                    EXCHANGE SHUFFLE[12, 11]
                        SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-23]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                EXCHANGE SHUFFLE[12, 11]
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE SHUFFLE[19, 18]
                    INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-24]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                EXCHANGE SHUFFLE[12, 11]
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE SHUFFLE[19, 18]
                    INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                        EXCHANGE SHUFFLE[37]
                            SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                        EXCHANGE SHUFFLE[36]
                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-25]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE SHUFFLE[18]
                    INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-26]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [12: PS_SUPPKEY = 19: L_SUPPKEY AND 11: PS_PARTKEY = 18: L_PARTKEY] post-join-predicate [null])
                SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
                EXCHANGE SHUFFLE[18]
                    INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                        EXCHANGE SHUFFLE[37]
                            SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                        EXCHANGE SHUFFLE[36]
                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
[end]
[plan-27]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
                EXCHANGE SHUFFLE[19, 18]
                    INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                        SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                EXCHANGE SHUFFLE[12, 11]
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
[end]
[plan-28]
AGGREGATE ([GLOBAL] aggregate [{35: sum(34: expr)=sum(35: sum(34: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{35: sum(34: expr)=sum(34: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [19: L_SUPPKEY = 12: PS_SUPPKEY AND 18: L_PARTKEY = 11: PS_PARTKEY] post-join-predicate [null])
                EXCHANGE SHUFFLE[19, 18]
                    INNER JOIN (join-predicate [37: cast = 36: cast] post-join-predicate [null])
                        EXCHANGE SHUFFLE[37]
                            SCAN (columns[18: L_PARTKEY, 19: L_SUPPKEY, 21: L_QUANTITY, 22: L_EXTENDEDPRICE, 23: L_DISCOUNT] predicate[null])
                        EXCHANGE SHUFFLE[36]
                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                EXCHANGE SHUFFLE[12, 11]
                    SCAN (columns[11: PS_PARTKEY, 12: PS_SUPPKEY, 14: PS_SUPPLYCOST] predicate[null])
[end]
