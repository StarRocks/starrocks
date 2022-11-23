[sql]
select
    o_year,
    sum(case
            when nation = 'IRAN' then volume
            else 0
        end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
                p_partkey = l_partkey
          and s_suppkey = l_suppkey
          and l_orderkey = o_orderkey
          and o_custkey = c_custkey
          and c_nationkey = n1.n_nationkey
          and n1.n_regionkey = r_regionkey
          and r_name = 'MIDDLE EAST'
          and s_nationkey = n2.n_nationkey
          and o_orderdate between date '1995-01-01' and date '1996-12-31'
          and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year ;
[result]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: sum=sum(73: sum)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                AGGREGATE ([LOCAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
                    INNER JOIN (join-predicate [60: N_NATIONKEY = 14: S_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[60: N_NATIONKEY, 61: N_NAME] predicate[null])
                        EXCHANGE SHUFFLE[14]
                            INNER JOIN (join-predicate [21: L_SUPPKEY = 11: S_SUPPKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [19: L_ORDERKEY = 36: O_ORDERKEY] post-join-predicate [null])
                                    INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                        SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                    EXCHANGE SHUFFLE[36]
                                        INNER JOIN (join-predicate [37: O_CUSTKEY = 46: C_CUSTKEY] post-join-predicate [null])
                                            SCAN (columns[36: O_ORDERKEY, 37: O_CUSTKEY, 40: O_ORDERDATE] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                                                    SCAN (columns[49: C_NATIONKEY, 46: C_CUSTKEY] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                                                            SCAN (columns[55: N_NATIONKEY, 57: N_REGIONKEY] predicate[null])
                                                            EXCHANGE BROADCAST
                                                                SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
                                EXCHANGE BROADCAST
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
[end]

