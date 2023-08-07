[sql]
select * from (select * from region) t0 full join (select * from nation) t1 on t0.r_name = t1.n_name
join (select * from supplier limit 10) t2 on 1 = 1;
[planCount]
1
[plan-1]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    FULL OUTER JOIN (join-predicate [2: R_NAME = 6: N_NAME] post-join-predicate [null])
        EXCHANGE SHUFFLE[2]
            SCAN (columns[1: R_REGIONKEY, 2: R_NAME, 3: R_COMMENT, 4: PAD] predicate[null])
        EXCHANGE SHUFFLE[6]
            SCAN (columns[5: N_NATIONKEY, 6: N_NAME, 7: N_REGIONKEY, 8: N_COMMENT, 9: PAD] predicate[null])
    EXCHANGE BROADCAST
        PREDICATE true
            EXCHANGE GATHER
                SCAN (columns[10: S_SUPPKEY, 11: S_NAME, 12: S_ADDRESS, 13: S_NATIONKEY, 14: S_PHONE, 15: S_ACCTBAL, 16: S_COMMENT, 17: PAD] predicate[null]) Limit 10