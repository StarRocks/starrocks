[sql]
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
                s_suppkey = l_suppkey
          and o_orderkey = l_orderkey
          and c_custkey = o_custkey
          and s_nationkey = n1.n_nationkey
          and c_nationkey = n2.n_nationkey
          and (
                (n1.n_name = 'CANADA' and n2.n_name = 'IRAN')
                or (n1.n_name = 'IRAN' and n2.n_name = 'CANADA')
            )
          and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year ;
[planCount]
4
[plan-1]
TOP-N (order by [[46: N_NAME ASC NULLS FIRST, 51: N_NAME ASC NULLS FIRST, 55: year ASC NULLS FIRST]])
    TOP-N (order by [[46: N_NAME ASC NULLS FIRST, 51: N_NAME ASC NULLS FIRST, 55: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{57: sum=sum(57: sum)}] group by [[46: N_NAME, 51: N_NAME, 55: year]] having [null]
            EXCHANGE SHUFFLE[46, 51, 55]
                AGGREGATE ([LOCAL] aggregate [{57: sum=sum(56: expr)}] group by [[46: N_NAME, 51: N_NAME, 55: year]] having [null]
                    INNER JOIN (join-predicate [36: C_CUSTKEY = 27: O_CUSTKEY AND 39: C_NATIONKEY = 50: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[36: C_CUSTKEY, 39: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [26: O_ORDERKEY = 9: L_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[26: O_ORDERKEY, 27: O_CUSTKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: L_SUPPKEY = 1: S_SUPPKEY] post-join-predicate [null])
                                        SCAN (columns[19: L_SHIPDATE, 9: L_ORDERKEY, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-01-01 AND 19: L_SHIPDATE <= 1996-12-31])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [4: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                                SCAN (columns[1: S_SUPPKEY, 4: S_NATIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    INNER JOIN (join-predicate [46: N_NAME = CANADA AND 51: N_NAME = IRAN OR 46: N_NAME = IRAN AND 51: N_NAME = CANADA] post-join-predicate [null])
                                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME] predicate[46: N_NAME IN (CANADA, IRAN)])
                                                        EXCHANGE BROADCAST
                                                            SCAN (columns[50: N_NATIONKEY, 51: N_NAME] predicate[51: N_NAME IN (IRAN, CANADA)])
[end]
[plan-2]
TOP-N (order by [[46: N_NAME ASC NULLS FIRST, 51: N_NAME ASC NULLS FIRST, 55: year ASC NULLS FIRST]])
    TOP-N (order by [[46: N_NAME ASC NULLS FIRST, 51: N_NAME ASC NULLS FIRST, 55: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{57: sum=sum(57: sum)}] group by [[46: N_NAME, 51: N_NAME, 55: year]] having [null]
            EXCHANGE SHUFFLE[46, 51, 55]
                AGGREGATE ([LOCAL] aggregate [{57: sum=sum(56: expr)}] group by [[46: N_NAME, 51: N_NAME, 55: year]] having [null]
                    INNER JOIN (join-predicate [36: C_CUSTKEY = 27: O_CUSTKEY AND 39: C_NATIONKEY = 50: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[36: C_CUSTKEY, 39: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [26: O_ORDERKEY = 9: L_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[26: O_ORDERKEY, 27: O_CUSTKEY] predicate[null])
                                EXCHANGE SHUFFLE[9]
                                    INNER JOIN (join-predicate [11: L_SUPPKEY = 1: S_SUPPKEY] post-join-predicate [null])
                                        SCAN (columns[19: L_SHIPDATE, 9: L_ORDERKEY, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-01-01 AND 19: L_SHIPDATE <= 1996-12-31])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [4: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                                SCAN (columns[1: S_SUPPKEY, 4: S_NATIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    INNER JOIN (join-predicate [46: N_NAME = CANADA AND 51: N_NAME = IRAN OR 46: N_NAME = IRAN AND 51: N_NAME = CANADA] post-join-predicate [null])
                                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME] predicate[46: N_NAME IN (CANADA, IRAN)])
                                                        EXCHANGE BROADCAST
                                                            SCAN (columns[50: N_NATIONKEY, 51: N_NAME] predicate[51: N_NAME IN (IRAN, CANADA)])
[end]
[plan-3]
TOP-N (order by [[46: N_NAME ASC NULLS FIRST, 51: N_NAME ASC NULLS FIRST, 55: year ASC NULLS FIRST]])
    TOP-N (order by [[46: N_NAME ASC NULLS FIRST, 51: N_NAME ASC NULLS FIRST, 55: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{57: sum=sum(57: sum)}] group by [[46: N_NAME, 51: N_NAME, 55: year]] having [null]
            EXCHANGE SHUFFLE[46, 51, 55]
                AGGREGATE ([LOCAL] aggregate [{57: sum=sum(56: expr)}] group by [[46: N_NAME, 51: N_NAME, 55: year]] having [null]
                    INNER JOIN (join-predicate [36: C_CUSTKEY = 27: O_CUSTKEY AND 39: C_NATIONKEY = 50: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[36: C_CUSTKEY, 39: C_NATIONKEY] predicate[null])
                        EXCHANGE SHUFFLE[27]
                            EXCHANGE SHUFFLE[27, 50]
                                INNER JOIN (join-predicate [26: O_ORDERKEY = 9: L_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[26: O_ORDERKEY, 27: O_CUSTKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [11: L_SUPPKEY = 1: S_SUPPKEY] post-join-predicate [null])
                                            SCAN (columns[19: L_SHIPDATE, 9: L_ORDERKEY, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-01-01 AND 19: L_SHIPDATE <= 1996-12-31])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [4: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                                    SCAN (columns[1: S_SUPPKEY, 4: S_NATIONKEY] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        INNER JOIN (join-predicate [46: N_NAME = CANADA AND 51: N_NAME = IRAN OR 46: N_NAME = IRAN AND 51: N_NAME = CANADA] post-join-predicate [null])
                                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME] predicate[46: N_NAME IN (CANADA, IRAN)])
                                                            EXCHANGE BROADCAST
                                                                SCAN (columns[50: N_NATIONKEY, 51: N_NAME] predicate[51: N_NAME IN (IRAN, CANADA)])
[end]
[plan-4]
TOP-N (order by [[46: N_NAME ASC NULLS FIRST, 51: N_NAME ASC NULLS FIRST, 55: year ASC NULLS FIRST]])
    TOP-N (order by [[46: N_NAME ASC NULLS FIRST, 51: N_NAME ASC NULLS FIRST, 55: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{57: sum=sum(57: sum)}] group by [[46: N_NAME, 51: N_NAME, 55: year]] having [null]
            EXCHANGE SHUFFLE[46, 51, 55]
                AGGREGATE ([LOCAL] aggregate [{57: sum=sum(56: expr)}] group by [[46: N_NAME, 51: N_NAME, 55: year]] having [null]
                    INNER JOIN (join-predicate [36: C_CUSTKEY = 27: O_CUSTKEY AND 39: C_NATIONKEY = 50: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[36: C_CUSTKEY, 39: C_NATIONKEY] predicate[null])
                        EXCHANGE SHUFFLE[27]
                            EXCHANGE SHUFFLE[27, 50]
                                INNER JOIN (join-predicate [26: O_ORDERKEY = 9: L_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[26: O_ORDERKEY, 27: O_CUSTKEY] predicate[null])
                                    EXCHANGE SHUFFLE[9]
                                        INNER JOIN (join-predicate [11: L_SUPPKEY = 1: S_SUPPKEY] post-join-predicate [null])
                                            SCAN (columns[19: L_SHIPDATE, 9: L_ORDERKEY, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-01-01 AND 19: L_SHIPDATE <= 1996-12-31])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [4: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                                    SCAN (columns[1: S_SUPPKEY, 4: S_NATIONKEY] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        INNER JOIN (join-predicate [46: N_NAME = CANADA AND 51: N_NAME = IRAN OR 46: N_NAME = IRAN AND 51: N_NAME = CANADA] post-join-predicate [null])
                                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME] predicate[46: N_NAME IN (CANADA, IRAN)])
                                                            EXCHANGE BROADCAST
                                                                SCAN (columns[50: N_NATIONKEY, 51: N_NAME] predicate[51: N_NAME IN (IRAN, CANADA)])
[end]