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
[scheduler]
PLAN FRAGMENT 0(F22)
  DOP: 16
  INSTANCES
    INSTANCE(0-F22#0)
      BE: 10001

PLAN FRAGMENT 1(F21)
  DOP: 16
  INSTANCES
    INSTANCE(1-F21#0)
      DESTINATIONS
        0-F22#0
      BE: 10001
    INSTANCE(2-F21#1)
      DESTINATIONS
        0-F22#0
      BE: 10003
    INSTANCE(3-F21#2)
      DESTINATIONS
        0-F22#0
      BE: 10002

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS
        1-F21#0,2-F21#1,3-F21#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES
          16,1,4,7,10,13
        0:OlapScanNode
          1. partitionID=1401,tabletID=1406
          2. partitionID=1401,tabletID=1412
          3. partitionID=1401,tabletID=1418
          4. partitionID=1401,tabletID=1424
          5. partitionID=1401,tabletID=1430
          6. partitionID=1401,tabletID=1436
    INSTANCE(5-F00#1)
      DESTINATIONS
        1-F21#0,2-F21#1,3-F21#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES
          17,2,5,8,11,14
        0:OlapScanNode
          1. partitionID=1401,tabletID=1408
          2. partitionID=1401,tabletID=1414
          3. partitionID=1401,tabletID=1420
          4. partitionID=1401,tabletID=1426
          5. partitionID=1401,tabletID=1432
          6. partitionID=1401,tabletID=1438
    INSTANCE(6-F00#2)
      DESTINATIONS
        1-F21#0,2-F21#1,3-F21#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          0,3,6,9,12,15
        0:OlapScanNode
          1. partitionID=1401,tabletID=1404
          2. partitionID=1401,tabletID=1410
          3. partitionID=1401,tabletID=1416
          4. partitionID=1401,tabletID=1422
          5. partitionID=1401,tabletID=1428
          6. partitionID=1401,tabletID=1434

PLAN FRAGMENT 3(F01)
  DOP: 16
  INSTANCES
    INSTANCE(7-F01#0)
      DESTINATIONS
        6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2
        4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          0
        2:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 4(F02)
  DOP: 16
  INSTANCES
    INSTANCE(8-F02#0)
      DESTINATIONS
        7-F01#0
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES
          2,5,8,11
        3:OlapScanNode
          1. partitionID=1484,tabletID=1491
          2. partitionID=1484,tabletID=1497
          3. partitionID=1484,tabletID=1503
          4. partitionID=1484,tabletID=1509
    INSTANCE(9-F02#1)
      DESTINATIONS
        7-F01#0
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES
          0,3,6,9
        3:OlapScanNode
          1. partitionID=1484,tabletID=1487
          2. partitionID=1484,tabletID=1493
          3. partitionID=1484,tabletID=1499
          4. partitionID=1484,tabletID=1505
    INSTANCE(10-F02#2)
      DESTINATIONS
        7-F01#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          1,4,7,10
        3:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

PLAN FRAGMENT 5(F03)
  DOP: 16
  INSTANCES
    INSTANCE(11-F03#0)
      DESTINATIONS
        9-F02#1,10-F02#2,8-F02#0,9-F02#1,10-F02#2,8-F02#0,9-F02#1,10-F02#2,8-F02#0,9-F02#1
        10-F02#2,8-F02#0
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES
          0,18,3,6,9,12,15
        4:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(12-F03#1)
      DESTINATIONS
        9-F02#1,10-F02#2,8-F02#0,9-F02#1,10-F02#2,8-F02#0,9-F02#1,10-F02#2,8-F02#0,9-F02#1
        10-F02#2,8-F02#0
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES
          16,1,19,4,7,10,13
        4:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(13-F03#2)
      DESTINATIONS
        9-F02#1,10-F02#2,8-F02#0,9-F02#1,10-F02#2,8-F02#0,9-F02#1,10-F02#2,8-F02#0,9-F02#1
        10-F02#2,8-F02#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          17,2,5,8,11,14
        4:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

PLAN FRAGMENT 6(F16)
  DOP: 16
  INSTANCES
    INSTANCE(14-F16#0)
      DESTINATIONS
        11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0
        12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1
      BE: 10001
    INSTANCE(15-F16#1)
      DESTINATIONS
        11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0
        12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1
      BE: 10002
    INSTANCE(16-F16#2)
      DESTINATIONS
        11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0
        12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1,13-F03#2,11-F03#0,12-F03#1
      BE: 10003

PLAN FRAGMENT 7(F14)
  DOP: 16
  INSTANCES
    INSTANCE(17-F14#0)
      DESTINATIONS
        14-F16#0,15-F16#1,16-F16#2
      BE: 10003
    INSTANCE(18-F14#1)
      DESTINATIONS
        14-F16#0,15-F16#1,16-F16#2
      BE: 10002
    INSTANCE(19-F14#2)
      DESTINATIONS
        14-F16#0,15-F16#1,16-F16#2
      BE: 10001

PLAN FRAGMENT 8(F12)
  DOP: 16
  INSTANCES
    INSTANCE(20-F12#0)
      DESTINATIONS
        17-F14#0,18-F14#1,19-F14#2
      BE: 10003

PLAN FRAGMENT 9(F10)
  DOP: 16
  INSTANCES
    INSTANCE(21-F10#0)
      DESTINATIONS
        20-F12#0
      BE: 10001
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1479,tabletID=1482

PLAN FRAGMENT 10(F08)
  DOP: 16
  INSTANCES
    INSTANCE(22-F08#0)
      DESTINATIONS
        20-F12#0
      BE: 10003
      SCAN RANGES
        9:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 11(F06)
  DOP: 16
  INSTANCES
    INSTANCE(23-F06#0)
      DESTINATIONS
        17-F14#0,18-F14#1,19-F14#2
      BE: 10001
      SCAN RANGES
        7:OlapScanNode
          1. partitionID=1306,tabletID=1311
          2. partitionID=1306,tabletID=1317
          3. partitionID=1306,tabletID=1323
          4. partitionID=1306,tabletID=1329
          5. partitionID=1306,tabletID=1335
          6. partitionID=1306,tabletID=1341
          7. partitionID=1306,tabletID=1347
          8. partitionID=1306,tabletID=1353
    INSTANCE(24-F06#1)
      DESTINATIONS
        17-F14#0,18-F14#1,19-F14#2
      BE: 10002
      SCAN RANGES
        7:OlapScanNode
          1. partitionID=1306,tabletID=1313
          2. partitionID=1306,tabletID=1319
          3. partitionID=1306,tabletID=1325
          4. partitionID=1306,tabletID=1331
          5. partitionID=1306,tabletID=1337
          6. partitionID=1306,tabletID=1343
          7. partitionID=1306,tabletID=1349
          8. partitionID=1306,tabletID=1355
    INSTANCE(25-F06#2)
      DESTINATIONS
        17-F14#0,18-F14#1,19-F14#2
      BE: 10003
      SCAN RANGES
        7:OlapScanNode
          1. partitionID=1306,tabletID=1309
          2. partitionID=1306,tabletID=1315
          3. partitionID=1306,tabletID=1321
          4. partitionID=1306,tabletID=1327
          5. partitionID=1306,tabletID=1333
          6. partitionID=1306,tabletID=1339
          7. partitionID=1306,tabletID=1345
          8. partitionID=1306,tabletID=1351

PLAN FRAGMENT 12(F04)
  DOP: 16
  INSTANCES
    INSTANCE(26-F04#0)
      DESTINATIONS
        14-F16#0,15-F16#1,16-F16#2
      BE: 10001
      SCAN RANGES
        5:OlapScanNode
          1. partitionID=1362,tabletID=1367
          2. partitionID=1362,tabletID=1373
          3. partitionID=1362,tabletID=1379
          4. partitionID=1362,tabletID=1385
          5. partitionID=1362,tabletID=1391
          6. partitionID=1362,tabletID=1397
    INSTANCE(27-F04#1)
      DESTINATIONS
        14-F16#0,15-F16#1,16-F16#2
      BE: 10002
      SCAN RANGES
        5:OlapScanNode
          1. partitionID=1362,tabletID=1369
          2. partitionID=1362,tabletID=1375
          3. partitionID=1362,tabletID=1381
          4. partitionID=1362,tabletID=1387
          5. partitionID=1362,tabletID=1393
          6. partitionID=1362,tabletID=1399
    INSTANCE(28-F04#2)
      DESTINATIONS
        14-F16#0,15-F16#1,16-F16#2
      BE: 10003
      SCAN RANGES
        5:OlapScanNode
          1. partitionID=1362,tabletID=1365
          2. partitionID=1362,tabletID=1371
          3. partitionID=1362,tabletID=1377
          4. partitionID=1362,tabletID=1383
          5. partitionID=1362,tabletID=1389
          6. partitionID=1362,tabletID=1395

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:62: year | 67: expr
  PARTITION: UNPARTITIONED

  RESULT SINK

  39:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 62: year

  STREAM DATA SINK
    EXCHANGE ID: 39
    UNPARTITIONED

  38:SORT
  |  order by: <slot 62> 62: year ASC
  |  offset: 0
  |  
  37:Project
  |  <slot 62> : 62: year
  |  <slot 67> : 65: sum / 66: sum
  |  
  36:AGGREGATE (merge finalize)
  |  output: sum(65: sum), sum(66: sum)
  |  group by: 62: year
  |  
  35:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 35
    HASH_PARTITIONED: 62: year

  34:AGGREGATE (update serialize)
  |  STREAMING
  |  output: sum(64: case), sum(63: expr)
  |  group by: 62: year
  |  
  33:Project
  |  <slot 62> : year(35: o_orderdate)
  |  <slot 63> : 69: multiply
  |  <slot 64> : if(56: n_name = 'IRAN', 69: multiply, 0.0)
  |  common expressions:
  |  <slot 68> : 1.0 - 23: L_DISCOUNT
  |  <slot 69> : 22: L_EXTENDEDPRICE * 68: subtract
  |  
  32:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 1: p_partkey = 18: L_PARTKEY
  |  
  |----31:EXCHANGE
  |    
  1:Project
  |  <slot 1> : 1: p_partkey
  |  
  0:OlapScanNode
     TABLE: part
     PREAGGREGATION: ON
     PREDICATES: 5: p_type = 'ECONOMY ANODIZED STEEL'
     partitions=1/1
     rollup: part
     tabletRatio=18/18
     tabletList=1404,1406,1408,1410,1412,1414,1416,1418,1420,1422 ...
     cardinality=1
     avgRowSize=33.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 31
    BUCKET_SHUFFLE_HASH_PARTITIONED: 18: L_PARTKEY

  30:Project
  |  <slot 18> : 18: L_PARTKEY
  |  <slot 22> : 22: L_EXTENDEDPRICE
  |  <slot 23> : 23: L_DISCOUNT
  |  <slot 35> : 35: o_orderdate
  |  <slot 56> : 56: n_name
  |  
  29:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 55: n_nationkey = 13: s_nationkey
  |  
  |----28:EXCHANGE
  |    
  2:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=29.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 28
    BUCKET_SHUFFLE_HASH_PARTITIONED: 13: s_nationkey

  27:Project
  |  <slot 13> : 13: s_nationkey
  |  <slot 18> : 18: L_PARTKEY
  |  <slot 22> : 22: L_EXTENDEDPRICE
  |  <slot 23> : 23: L_DISCOUNT
  |  <slot 35> : 35: o_orderdate
  |  
  26:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 10: s_suppkey = 19: L_SUPPKEY
  |  
  |----25:EXCHANGE
  |    
  3:OlapScanNode
     TABLE: supplier
     PREAGGREGATION: ON
     partitions=1/1
     rollup: supplier
     tabletRatio=12/12
     tabletList=1487,1489,1491,1493,1495,1497,1499,1501,1503,1505 ...
     cardinality=1
     avgRowSize=8.0

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 25
    BUCKET_SHUFFLE_HASH_PARTITIONED: 19: L_SUPPKEY

  24:Project
  |  <slot 18> : 18: L_PARTKEY
  |  <slot 19> : 19: L_SUPPKEY
  |  <slot 22> : 22: L_EXTENDEDPRICE
  |  <slot 23> : 23: L_DISCOUNT
  |  <slot 35> : 35: o_orderdate
  |  
  23:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 17: L_ORDERKEY = 34: o_orderkey
  |  
  |----22:EXCHANGE
  |    
  4:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=36.0

PLAN FRAGMENT 6
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 36: o_custkey

  STREAM DATA SINK
    EXCHANGE ID: 22
    BUCKET_SHUFFLE_HASH_PARTITIONED: 34: o_orderkey

  21:Project
  |  <slot 34> : 34: o_orderkey
  |  <slot 35> : 35: o_orderdate
  |  
  20:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 36: o_custkey = 43: c_custkey
  |  
  |----19:EXCHANGE
  |    
  6:EXCHANGE

PLAN FRAGMENT 7
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 46: c_nationkey

  STREAM DATA SINK
    EXCHANGE ID: 19
    HASH_PARTITIONED: 43: c_custkey

  18:Project
  |  <slot 43> : 43: c_custkey
  |  
  17:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 46: c_nationkey = 51: n_nationkey
  |  
  |----16:EXCHANGE
  |    
  8:EXCHANGE

PLAN FRAGMENT 8
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 53: n_regionkey

  STREAM DATA SINK
    EXCHANGE ID: 16
    HASH_PARTITIONED: 51: n_nationkey

  15:Project
  |  <slot 51> : 51: n_nationkey
  |  
  14:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 53: n_regionkey = 59: r_regionkey
  |  
  |----13:EXCHANGE
  |    
  10:EXCHANGE

PLAN FRAGMENT 9
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 13
    HASH_PARTITIONED: 59: r_regionkey

  12:Project
  |  <slot 59> : 59: r_regionkey
  |  
  11:OlapScanNode
     TABLE: region
     PREAGGREGATION: ON
     PREDICATES: 60: r_name = 'MIDDLE EAST'
     partitions=1/1
     rollup: region
     tabletRatio=1/1
     tabletList=1482
     cardinality=1
     avgRowSize=29.0

PLAN FRAGMENT 10
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 10
    HASH_PARTITIONED: 53: n_regionkey

  9:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=8.0

PLAN FRAGMENT 11
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 08
    HASH_PARTITIONED: 46: c_nationkey

  7:OlapScanNode
     TABLE: customer
     PREAGGREGATION: ON
     partitions=1/1
     rollup: customer
     tabletRatio=24/24
     tabletList=1309,1311,1313,1315,1317,1319,1321,1323,1325,1327 ...
     cardinality=1
     avgRowSize=12.0

PLAN FRAGMENT 12
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 06
    HASH_PARTITIONED: 36: o_custkey

  5:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     PREDICATES: 35: o_orderdate >= '1995-01-01', 35: o_orderdate <= '1996-12-31'
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=20.0
[end]

