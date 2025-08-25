[scheduler]
PLAN FRAGMENT 0(F07)
  DOP: 16
  INSTANCES
    INSTANCE(0-F07#0)
      BE: 10002

PLAN FRAGMENT 1(F06)
  DOP: 16
  INSTANCES
    INSTANCE(1-F06#0)
      DESTINATIONS: 0-F07#0
      BE: 10003
    INSTANCE(2-F06#1)
      DESTINATIONS: 0-F07#0
      BE: 10002
    INSTANCE(3-F06#2)
      DESTINATIONS: 0-F07#0
      BE: 10001

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS: 1-F06#0,2-F06#1,3-F06#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 19, 4, 22, 7, 10, 13]
        0:OlapScanNode
          1. partitionID=2390,tabletID=2393
          2. partitionID=2390,tabletID=2399
          3. partitionID=2390,tabletID=2405
          4. partitionID=2390,tabletID=2411
          5. partitionID=2390,tabletID=2417
          6. partitionID=2390,tabletID=2423
          7. partitionID=2390,tabletID=2429
          8. partitionID=2390,tabletID=2435
    INSTANCE(5-F00#1)
      DESTINATIONS: 1-F06#0,2-F06#1,3-F06#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 20, 5, 23, 8, 11, 14]
        0:OlapScanNode
          1. partitionID=2390,tabletID=2395
          2. partitionID=2390,tabletID=2401
          3. partitionID=2390,tabletID=2407
          4. partitionID=2390,tabletID=2413
          5. partitionID=2390,tabletID=2419
          6. partitionID=2390,tabletID=2425
          7. partitionID=2390,tabletID=2431
          8. partitionID=2390,tabletID=2437
    INSTANCE(6-F00#2)
      DESTINATIONS: 1-F06#0,2-F06#1,3-F06#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 18, 3, 21, 6, 9, 12, 15]
        0:OlapScanNode
          1. partitionID=2390,tabletID=2391
          2. partitionID=2390,tabletID=2397
          3. partitionID=2390,tabletID=2403
          4. partitionID=2390,tabletID=2409
          5. partitionID=2390,tabletID=2415
          6. partitionID=2390,tabletID=2421
          7. partitionID=2390,tabletID=2427
          8. partitionID=2390,tabletID=2433

PLAN FRAGMENT 3(F04)
  DOP: 16
  INSTANCES
    INSTANCE(7-F04#0)
      DESTINATIONS: 6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
      BE: 10001
      SCAN RANGES
        9:OlapScanNode
          1. partitionID=2448,tabletID=2451
          2. partitionID=2448,tabletID=2457
          3. partitionID=2448,tabletID=2463
          4. partitionID=2448,tabletID=2469
          5. partitionID=2448,tabletID=2475
          6. partitionID=2448,tabletID=2481
    INSTANCE(8-F04#1)
      DESTINATIONS: 6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
      BE: 10002
      SCAN RANGES
        9:OlapScanNode
          1. partitionID=2448,tabletID=2453
          2. partitionID=2448,tabletID=2459
          3. partitionID=2448,tabletID=2465
          4. partitionID=2448,tabletID=2471
          5. partitionID=2448,tabletID=2477
          6. partitionID=2448,tabletID=2483
    INSTANCE(9-F04#2)
      DESTINATIONS: 6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
      BE: 10003
      SCAN RANGES
        9:OlapScanNode
          1. partitionID=2448,tabletID=2449
          2. partitionID=2448,tabletID=2455
          3. partitionID=2448,tabletID=2461
          4. partitionID=2448,tabletID=2467
          5. partitionID=2448,tabletID=2473
          6. partitionID=2448,tabletID=2479

PLAN FRAGMENT 4(F02)
  DOP: 16
  INSTANCES
    INSTANCE(10-F02#0)
      DESTINATIONS: 4-F00#0,5-F00#1,6-F00#2
      BE: 10001

PLAN FRAGMENT 5(F01)
  DOP: 16
  INSTANCES
    INSTANCE(11-F01#0)
      DESTINATIONS: 10-F02#0
      BE: 10001
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=2390,tabletID=2393
          2. partitionID=2390,tabletID=2399
          3. partitionID=2390,tabletID=2405
          4. partitionID=2390,tabletID=2411
          5. partitionID=2390,tabletID=2417
          6. partitionID=2390,tabletID=2423
          7. partitionID=2390,tabletID=2429
          8. partitionID=2390,tabletID=2435
    INSTANCE(12-F01#1)
      DESTINATIONS: 10-F02#0
      BE: 10002
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=2390,tabletID=2395
          2. partitionID=2390,tabletID=2401
          3. partitionID=2390,tabletID=2407
          4. partitionID=2390,tabletID=2413
          5. partitionID=2390,tabletID=2419
          6. partitionID=2390,tabletID=2425
          7. partitionID=2390,tabletID=2431
          8. partitionID=2390,tabletID=2437
    INSTANCE(13-F01#2)
      DESTINATIONS: 10-F02#0
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=2390,tabletID=2391
          2. partitionID=2390,tabletID=2397
          3. partitionID=2390,tabletID=2403
          4. partitionID=2390,tabletID=2409
          5. partitionID=2390,tabletID=2415
          6. partitionID=2390,tabletID=2421
          7. partitionID=2390,tabletID=2427
          8. partitionID=2390,tabletID=2433

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:29: substring | 30: count | 31: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  17:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 29: substring

  STREAM DATA SINK
    EXCHANGE ID: 17
    UNPARTITIONED

  16:SORT
  |  order by: <slot 29> 29: substring ASC
  |  offset: 0
  |
  15:AGGREGATE (merge finalize)
  |  output: count(30: count), sum(31: sum)
  |  group by: 29: substring
  |
  14:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 14
    HASH_PARTITIONED: 29: substring

  13:AGGREGATE (update serialize)
  |  STREAMING
  |  output: count(*), sum(6: c_acctbal)
  |  group by: 29: substring
  |
  12:Project
  |  <slot 6> : 6: c_acctbal
  |  <slot 29> : substring(5: c_phone, 1, 2)
  |
  11:HASH JOIN
  |  join op: LEFT ANTI JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason:
  |  equal join conjunct: 1: c_custkey = 21: o_custkey
  |
  |----10:EXCHANGE
  |
  8:Project
  |  <slot 1> : 1: c_custkey
  |  <slot 5> : 5: c_phone
  |  <slot 6> : 6: c_acctbal
  |
  7:NESTLOOP JOIN
  |  join op: INNER JOIN
  |  colocate: false, reason:
  |  other join predicates: CAST(6: c_acctbal AS DECIMAL128(38,8)) > 17: avg
  |
  |----6:EXCHANGE
  |
  0:OlapScanNode
     TABLE: customer
     PREAGGREGATION: ON
     PREDICATES: substring(5: c_phone, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
     partitions=1/1
     rollup: customer
     tabletRatio=24/24
     tabletList=2391,2393,2395,2397,2399,2401,2403,2405,2407,2409 ...
     cardinality=1
     avgRowSize=31.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 10
    BUCKET_SHUFFLE_HASH_PARTITIONED: 21: o_custkey

  9:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=8.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: UNPARTITIONED

  STREAM DATA SINK
    EXCHANGE ID: 06
    UNPARTITIONED

  5:AGGREGATE (merge finalize)
  |  output: avg(17: avg)
  |  group by:
  |
  4:EXCHANGE

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    UNPARTITIONED

  3:AGGREGATE (update serialize)
  |  output: avg(14: c_acctbal)
  |  group by:
  |
  2:Project
  |  <slot 14> : 14: c_acctbal
  |
  1:OlapScanNode
     TABLE: customer
     PREAGGREGATION: ON
     PREDICATES: 14: c_acctbal > 0.00, substring(13: c_phone, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
     partitions=1/1
     rollup: customer
     tabletRatio=24/24
     tabletList=1309,1311,1313,1315,1317,1319,1321,1323,1325,1327 ...
     cardinality=1
     avgRowSize=23.0
[end]