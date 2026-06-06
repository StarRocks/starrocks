[scheduler]
PLAN FRAGMENT 0(F30)
  DOP: 16
  INSTANCES
    INSTANCE(0-F30#0)
      BE: 10001

PLAN FRAGMENT 1(F29)
  DOP: 16
  INSTANCES
    INSTANCE(1-F29#0)
      DESTINATIONS: 0-F30#0
      BE: 10001
    INSTANCE(2-F29#1)
      DESTINATIONS: 0-F30#0
      BE: 10002
    INSTANCE(3-F29#2)
      DESTINATIONS: 0-F30#0
      BE: 10003

PLAN FRAGMENT 2(F28)
  DOP: 16
  INSTANCES
    INSTANCE(4-F28#0)
      DESTINATIONS: 1-F29#0,2-F29#1,3-F29#2
      BE: 10003
    INSTANCE(5-F28#1)
      DESTINATIONS: 1-F29#0,2-F29#1,3-F29#2
      BE: 10002
    INSTANCE(6-F28#2)
      DESTINATIONS: 1-F29#0,2-F29#1,3-F29#2
      BE: 10001

PLAN FRAGMENT 3(F26)
  DOP: 16
  INSTANCES
    INSTANCE(7-F26#0)
      DESTINATIONS: 4-F28#0,5-F28#1,6-F28#2
      BE: 10001
    INSTANCE(8-F26#1)
      DESTINATIONS: 4-F28#0,5-F28#1,6-F28#2
      BE: 10003
    INSTANCE(9-F26#2)
      DESTINATIONS: 4-F28#0,5-F28#1,6-F28#2
      BE: 10002

PLAN FRAGMENT 4(F24)
  DOP: 16
  INSTANCES
    INSTANCE(10-F24#0)
      DESTINATIONS: 7-F26#0,8-F26#1,9-F26#2
      BE: 10003
      SCAN RANGES
        31:OlapScanNode
          1. partitionID=2442,tabletID=2443

PLAN FRAGMENT 5(F22)
  DOP: 16
  INSTANCES
    INSTANCE(11-F22#0)
      DESTINATIONS: 7-F26#0,8-F26#1,9-F26#2
      BE: 10001
      SCAN RANGES
        29:OlapScanNode
          1. partitionID=2574,tabletID=2579
          2. partitionID=2574,tabletID=2585
          3. partitionID=2574,tabletID=2591
          4. partitionID=2574,tabletID=2597
    INSTANCE(12-F22#1)
      DESTINATIONS: 7-F26#0,8-F26#1,9-F26#2
      BE: 10002
      SCAN RANGES
        29:OlapScanNode
          1. partitionID=2574,tabletID=2575
          2. partitionID=2574,tabletID=2581
          3. partitionID=2574,tabletID=2587
          4. partitionID=2574,tabletID=2593
    INSTANCE(13-F22#2)
      DESTINATIONS: 7-F26#0,8-F26#1,9-F26#2
      BE: 10003
      SCAN RANGES
        29:OlapScanNode
          1. partitionID=2574,tabletID=2577
          2. partitionID=2574,tabletID=2583
          3. partitionID=2574,tabletID=2589
          4. partitionID=2574,tabletID=2595

PLAN FRAGMENT 6(F20)
  DOP: 16
  INSTANCES
    INSTANCE(14-F20#0)
      DESTINATIONS: 4-F28#0,5-F28#1,6-F28#2
      BE: 10001
    INSTANCE(15-F20#1)
      DESTINATIONS: 4-F28#0,5-F28#1,6-F28#2
      BE: 10002
    INSTANCE(16-F20#2)
      DESTINATIONS: 4-F28#0,5-F28#1,6-F28#2
      BE: 10003

PLAN FRAGMENT 7(F18)
  DOP: 16
  INSTANCES
    INSTANCE(17-F18#0)
      DESTINATIONS: 14-F20#0,15-F20#1,16-F20#2
      BE: 10001
      SCAN RANGES
        23:OlapScanNode
          1. partitionID=2488,tabletID=2491
          2. partitionID=2488,tabletID=2497
          3. partitionID=2488,tabletID=2503
          4. partitionID=2488,tabletID=2509
          5. partitionID=2488,tabletID=2515
          6. partitionID=2488,tabletID=2521
    INSTANCE(18-F18#1)
      DESTINATIONS: 14-F20#0,15-F20#1,16-F20#2
      BE: 10002
      SCAN RANGES
        23:OlapScanNode
          1. partitionID=2488,tabletID=2493
          2. partitionID=2488,tabletID=2499
          3. partitionID=2488,tabletID=2505
          4. partitionID=2488,tabletID=2511
          5. partitionID=2488,tabletID=2517
          6. partitionID=2488,tabletID=2523
    INSTANCE(19-F18#2)
      DESTINATIONS: 14-F20#0,15-F20#1,16-F20#2
      BE: 10003
      SCAN RANGES
        23:OlapScanNode
          1. partitionID=2488,tabletID=2489
          2. partitionID=2488,tabletID=2495
          3. partitionID=2488,tabletID=2501
          4. partitionID=2488,tabletID=2507
          5. partitionID=2488,tabletID=2513
          6. partitionID=2488,tabletID=2519

PLAN FRAGMENT 8(F16)
  DOP: 16
  INSTANCES
    INSTANCE(20-F16#0)
      DESTINATIONS: 14-F20#0,15-F20#1,16-F20#2
      BE: 10003
    INSTANCE(21-F16#1)
      DESTINATIONS: 14-F20#0,15-F20#1,16-F20#2
      BE: 10002
    INSTANCE(22-F16#2)
      DESTINATIONS: 14-F20#0,15-F20#1,16-F20#2
      BE: 10001

PLAN FRAGMENT 9(F14)
  DOP: 16
  INSTANCES
    INSTANCE(23-F14#0)
      DESTINATIONS: 20-F16#0,21-F16#1,22-F16#2
      BE: 10001
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=1004,tabletID=1005
          2. partitionID=1004,tabletID=1011
          3. partitionID=1004,tabletID=1017
          4. partitionID=1004,tabletID=1023
          5. partitionID=1004,tabletID=1029
          6. partitionID=1004,tabletID=1035
          7. partitionID=1004,tabletID=1041
    INSTANCE(24-F14#1)
      DESTINATIONS: 20-F16#0,21-F16#1,22-F16#2
      BE: 10002
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=1004,tabletID=1007
          2. partitionID=1004,tabletID=1013
          3. partitionID=1004,tabletID=1019
          4. partitionID=1004,tabletID=1025
          5. partitionID=1004,tabletID=1031
          6. partitionID=1004,tabletID=1037
          7. partitionID=1004,tabletID=1043
    INSTANCE(25-F14#2)
      DESTINATIONS: 20-F16#0,21-F16#1,22-F16#2
      BE: 10003
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=1004,tabletID=1009
          2. partitionID=1004,tabletID=1015
          3. partitionID=1004,tabletID=1021
          4. partitionID=1004,tabletID=1027
          5. partitionID=1004,tabletID=1033
          6. partitionID=1004,tabletID=1039

PLAN FRAGMENT 10(F12)
  DOP: 16
  INSTANCES
    INSTANCE(26-F12#0)
      DESTINATIONS: 20-F16#0,21-F16#1,22-F16#2
      BE: 10001
    INSTANCE(27-F12#1)
      DESTINATIONS: 20-F16#0,21-F16#1,22-F16#2
      BE: 10003
    INSTANCE(28-F12#2)
      DESTINATIONS: 20-F16#0,21-F16#1,22-F16#2
      BE: 10002

PLAN FRAGMENT 11(F10)
  DOP: 16
  INSTANCES
    INSTANCE(29-F10#0)
      DESTINATIONS: 26-F12#0,27-F12#1,28-F12#2
      BE: 10001
      SCAN RANGES
        13:OlapScanNode
          1. partitionID=2448,tabletID=2451
          2. partitionID=2448,tabletID=2457
          3. partitionID=2448,tabletID=2463
          4. partitionID=2448,tabletID=2469
          5. partitionID=2448,tabletID=2475
          6. partitionID=2448,tabletID=2481
    INSTANCE(30-F10#1)
      DESTINATIONS: 26-F12#0,27-F12#1,28-F12#2
      BE: 10002
      SCAN RANGES
        13:OlapScanNode
          1. partitionID=2448,tabletID=2453
          2. partitionID=2448,tabletID=2459
          3. partitionID=2448,tabletID=2465
          4. partitionID=2448,tabletID=2471
          5. partitionID=2448,tabletID=2477
          6. partitionID=2448,tabletID=2483
    INSTANCE(31-F10#2)
      DESTINATIONS: 26-F12#0,27-F12#1,28-F12#2
      BE: 10003
      SCAN RANGES
        13:OlapScanNode
          1. partitionID=2448,tabletID=2449
          2. partitionID=2448,tabletID=2455
          3. partitionID=2448,tabletID=2461
          4. partitionID=2448,tabletID=2467
          5. partitionID=2448,tabletID=2473
          6. partitionID=2448,tabletID=2479

PLAN FRAGMENT 12(F08)
  DOP: 16
  INSTANCES
    INSTANCE(32-F08#0)
      DESTINATIONS: 26-F12#0,27-F12#1,28-F12#2
      BE: 10001
    INSTANCE(33-F08#1)
      DESTINATIONS: 26-F12#0,27-F12#1,28-F12#2
      BE: 10002
    INSTANCE(34-F08#2)
      DESTINATIONS: 26-F12#0,27-F12#1,28-F12#2
      BE: 10003

PLAN FRAGMENT 13(F06)
  DOP: 16
  INSTANCES
    INSTANCE(35-F06#0)
      DESTINATIONS: 32-F08#0,33-F08#1,34-F08#2
      BE: 10001
      SCAN RANGES
        7:OlapScanNode
          1. partitionID=2568,tabletID=2569

PLAN FRAGMENT 14(F04)
  DOP: 16
  INSTANCES
    INSTANCE(36-F04#0)
      DESTINATIONS: 32-F08#0,33-F08#1,34-F08#2
      BE: 10003
    INSTANCE(37-F04#1)
      DESTINATIONS: 32-F08#0,33-F08#1,34-F08#2
      BE: 10002
    INSTANCE(38-F04#2)
      DESTINATIONS: 32-F08#0,33-F08#1,34-F08#2
      BE: 10001

PLAN FRAGMENT 15(F02)
  DOP: 16
  INSTANCES
    INSTANCE(39-F02#0)
      DESTINATIONS: 36-F04#0,37-F04#1,38-F04#2
      BE: 10003
      SCAN RANGES
        2:OlapScanNode
          1. partitionID=2442,tabletID=2443

PLAN FRAGMENT 16(F00)
  DOP: 16
  INSTANCES
    INSTANCE(40-F00#0)
      DESTINATIONS: 36-F04#0,37-F04#1,38-F04#2
      BE: 10001
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=2390,tabletID=2393
          2. partitionID=2390,tabletID=2399
          3. partitionID=2390,tabletID=2405
          4. partitionID=2390,tabletID=2411
          5. partitionID=2390,tabletID=2417
          6. partitionID=2390,tabletID=2423
          7. partitionID=2390,tabletID=2429
          8. partitionID=2390,tabletID=2435
    INSTANCE(41-F00#1)
      DESTINATIONS: 36-F04#0,37-F04#1,38-F04#2
      BE: 10002
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=2390,tabletID=2395
          2. partitionID=2390,tabletID=2401
          3. partitionID=2390,tabletID=2407
          4. partitionID=2390,tabletID=2413
          5. partitionID=2390,tabletID=2419
          6. partitionID=2390,tabletID=2425
          7. partitionID=2390,tabletID=2431
          8. partitionID=2390,tabletID=2437
    INSTANCE(42-F00#2)
      DESTINATIONS: 36-F04#0,37-F04#1,38-F04#2
      BE: 10003
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=2390,tabletID=2391
          2. partitionID=2390,tabletID=2397
          3. partitionID=2390,tabletID=2403
          4. partitionID=2390,tabletID=2409
          5. partitionID=2390,tabletID=2415
          6. partitionID=2390,tabletID=2421
          7. partitionID=2390,tabletID=2427
          8. partitionID=2390,tabletID=2433
[end]