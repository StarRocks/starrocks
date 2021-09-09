[sql]
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    (	select
             l_suppkey as supplier_no,
             sum(l_extendedprice * (1 - l_discount)) as total_revenue
         from
             lineitem
         where
                 l_shipdate >= date '1995-07-01'
           and l_shipdate < date '1995-10-01'
         group by
             l_suppkey) a
where
        s_suppkey = supplier_no
  and total_revenue = (
    select
        max(total_revenue)
    from
        (	select
                 l_suppkey as supplier_no,
                 sum(l_extendedprice * (1 - l_discount)) as total_revenue
             from
                 lineitem
             where
                     l_shipdate >= date '1995-07-01'
               and l_shipdate < date '1995-10-01'
             group by
                 l_suppkey) b
)
order by
    s_suppkey;
[result]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE SHUFFLE[11]
                INNER JOIN (join-predicate [27: sum(26: expr) = 47: max(46: sum(45: expr))] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum(26: expr)=sum(27: sum(26: expr))}] group by [[11: L_SUPPKEY]] having [null]
                        EXCHANGE SHUFFLE[11]
                            AGGREGATE ([LOCAL] aggregate [{27: sum(26: expr)=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [null]
                                SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        AGGREGATE ([GLOBAL] aggregate [{47: max(46: sum(45: expr))=max(47: max(46: sum(45: expr)))}] group by [[]] having [null]
                            EXCHANGE GATHER
                                AGGREGATE ([LOCAL] aggregate [{47: max(46: sum(45: expr))=max(46: sum(45: expr))}] group by [[]] having [null]
                                    AGGREGATE ([GLOBAL] aggregate [{46: sum(45: expr)=sum(46: sum(45: expr))}] group by [[30: L_SUPPKEY]] having [null]
                                        EXCHANGE SHUFFLE[30]
                                            AGGREGATE ([LOCAL] aggregate [{46: sum(45: expr)=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]

