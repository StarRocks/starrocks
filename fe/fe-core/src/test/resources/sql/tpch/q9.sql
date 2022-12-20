[sql]
select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        where
                s_suppkey = l_suppkey
          and ps_suppkey = l_suppkey
          and ps_partkey = l_partkey
          and p_partkey = l_partkey
          and o_orderkey = l_orderkey
          and s_nationkey = n_nationkey
          and p_name like '%peru%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc ;
[result]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [21: L_SUPPKEY = 11: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [21: L_SUPPKEY = 37: PS_SUPPKEY AND 20: L_PARTKEY = 36: PS_PARTKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [19: L_ORDERKEY = 42: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[42]
                                        SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                            EXCHANGE BROADCAST
                                SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [52: N_NATIONKEY = 14: S_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                                EXCHANGE SHUFFLE[14]
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
[end]

