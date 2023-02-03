[sql]
select * from (select * from region) t0 full join (select * from nation) t1 on t0.r_name = t1.n_name
join (select * from supplier limit 10) t2 on if(r_name is null, 1, r_name) = t2.S_ADDRESS;
[planCount]
3
[plan-1]
INNER JOIN (join-predicate [18: if = 12: S_ADDRESS] post-join-predicate [null])
    FULL OUTER JOIN (join-predicate [2: R_NAME = 6: N_NAME] post-join-predicate [if(2: R_NAME IS NULL, 1, 2: R_NAME) IS NOT NULL])
        EXCHANGE SHUFFLE[2]
            SCAN (columns[1: R_REGIONKEY, 2: R_NAME, 3: R_COMMENT, 4: PAD] predicate[null])
        EXCHANGE SHUFFLE[6]
            SCAN (columns[5: N_NATIONKEY, 6: N_NAME, 7: N_REGIONKEY, 8: N_COMMENT, 9: PAD] predicate[null])
    EXCHANGE BROADCAST
        PREDICATE 12: S_ADDRESS IS NOT NULL
            EXCHANGE GATHER
                SCAN (columns[10: S_SUPPKEY, 11: S_NAME, 12: S_ADDRESS, 13: S_NATIONKEY, 14: S_PHONE, 15: S_ACCTBAL, 16: S_COMMENT, 17: PAD] predicate[null]) Limit 10
[end]
[plan-2]
INNER JOIN (join-predicate [18: if = 12: S_ADDRESS] post-join-predicate [null])
    FULL OUTER JOIN (join-predicate [6: N_NAME = 2: R_NAME] post-join-predicate [if(2: R_NAME IS NULL, 1, 2: R_NAME) IS NOT NULL])
        EXCHANGE SHUFFLE[6]
            SCAN (columns[5: N_NATIONKEY, 6: N_NAME, 7: N_REGIONKEY, 8: N_COMMENT, 9: PAD] predicate[null])
        EXCHANGE SHUFFLE[2]
            SCAN (columns[1: R_REGIONKEY, 2: R_NAME, 3: R_COMMENT, 4: PAD] predicate[null])
    EXCHANGE BROADCAST
        PREDICATE 12: S_ADDRESS IS NOT NULL
            EXCHANGE GATHER
                SCAN (columns[10: S_SUPPKEY, 11: S_NAME, 12: S_ADDRESS, 13: S_NATIONKEY, 14: S_PHONE, 15: S_ACCTBAL, 16: S_COMMENT, 17: PAD] predicate[null]) Limit 10
[end]
[plan-3]
INNER JOIN (join-predicate [18: if = 12: S_ADDRESS] post-join-predicate [null])
    EXCHANGE SHUFFLE[18]
        FULL OUTER JOIN (join-predicate [2: R_NAME = 6: N_NAME] post-join-predicate [if(2: R_NAME IS NULL, 1, 2: R_NAME) IS NOT NULL])
            EXCHANGE SHUFFLE[2]
                SCAN (columns[1: R_REGIONKEY, 2: R_NAME, 3: R_COMMENT, 4: PAD] predicate[null])
            EXCHANGE SHUFFLE[6]
                SCAN (columns[5: N_NATIONKEY, 6: N_NAME, 7: N_REGIONKEY, 8: N_COMMENT, 9: PAD] predicate[null])
    EXCHANGE SHUFFLE[12]
        PREDICATE 12: S_ADDRESS IS NOT NULL
            EXCHANGE GATHER
                SCAN (columns[10: S_SUPPKEY, 11: S_NAME, 12: S_ADDRESS, 13: S_NATIONKEY, 14: S_PHONE, 15: S_ACCTBAL, 16: S_COMMENT, 17: PAD] predicate[null]) Limit 10
[end]