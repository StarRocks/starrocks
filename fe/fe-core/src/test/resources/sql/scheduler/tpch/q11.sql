[sql]
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'PERU'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
    select
    sum(ps_supplycost * ps_availqty) * 0.0001000000
    from
    partsupp,
    supplier,
    nation
    where
    ps_suppkey = s_suppkey
                  and s_nationkey = n_nationkey
                  and n_name = 'PERU'
    )
order by
    value desc ;
[scheduler]
PLAN FRAGMENT 0(F17)
  DOP: 16
  INSTANCES
    INSTANCE(0-F17#0)
      BE: 10002

PLAN FRAGMENT 1(F07)
  DOP: 16
  INSTANCES
    INSTANCE(1-F07#0)
      DESTINATIONS: 0-F17#0
      BE: 10001
    INSTANCE(2-F07#1)
      DESTINATIONS: 0-F17#0
      BE: 10003
    INSTANCE(3-F07#2)
      DESTINATIONS: 0-F17#0
      BE: 10002

PLAN FRAGMENT 2(F15)
  DOP: 16
  INSTANCES
    INSTANCE(4-F15#0)
      DESTINATIONS: 1-F07#0,2-F07#1,3-F07#2
      BE: 10001

PLAN FRAGMENT 3(F14)
  DOP: 16
  INSTANCES
    INSTANCE(5-F14#0)
      DESTINATIONS: 4-F15#0
      BE: 10001
    INSTANCE(6-F14#1)
      DESTINATIONS: 4-F15#0
      BE: 10002
    INSTANCE(7-F14#2)
      DESTINATIONS: 4-F15#0
      BE: 10003

PLAN FRAGMENT 4(F10)
  DOP: 16
  INSTANCES
    INSTANCE(8-F10#0)
      DESTINATIONS: 5-F14#0,6-F14#1,7-F14#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0]
        16:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 5(F11)
  DOP: 16
  INSTANCES
    INSTANCE(9-F11#0)
      DESTINATIONS: 8-F10#0
      BE: 10001
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=1484,tabletID=1491
          2. partitionID=1484,tabletID=1497
          3. partitionID=1484,tabletID=1503
          4. partitionID=1484,tabletID=1509
    INSTANCE(10-F11#1)
      DESTINATIONS: 8-F10#0
      BE: 10002
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=1484,tabletID=1487
          2. partitionID=1484,tabletID=1493
          3. partitionID=1484,tabletID=1499
          4. partitionID=1484,tabletID=1505
    INSTANCE(11-F11#2)
      DESTINATIONS: 8-F10#0
      BE: 10003
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

PLAN FRAGMENT 6(F08)
  DOP: 16
  INSTANCES
    INSTANCE(12-F08#0)
      DESTINATIONS: 5-F14#0,6-F14#1,7-F14#2
      BE: 10001
      SCAN RANGES
        14:OlapScanNode
          1. partitionID=1440,tabletID=1445
          2. partitionID=1440,tabletID=1451
          3. partitionID=1440,tabletID=1457
          4. partitionID=1440,tabletID=1463
          5. partitionID=1440,tabletID=1469
          6. partitionID=1440,tabletID=1475
    INSTANCE(13-F08#1)
      DESTINATIONS: 5-F14#0,6-F14#1,7-F14#2
      BE: 10002
      SCAN RANGES
        14:OlapScanNode
          1. partitionID=1440,tabletID=1447
          2. partitionID=1440,tabletID=1453
          3. partitionID=1440,tabletID=1459
          4. partitionID=1440,tabletID=1465
          5. partitionID=1440,tabletID=1471
          6. partitionID=1440,tabletID=1477
    INSTANCE(14-F08#2)
      DESTINATIONS: 5-F14#0,6-F14#1,7-F14#2
      BE: 10003
      SCAN RANGES
        14:OlapScanNode
          1. partitionID=1440,tabletID=1443
          2. partitionID=1440,tabletID=1449
          3. partitionID=1440,tabletID=1455
          4. partitionID=1440,tabletID=1461
          5. partitionID=1440,tabletID=1467
          6. partitionID=1440,tabletID=1473

PLAN FRAGMENT 7(F06)
  DOP: 16
  INSTANCES
    INSTANCE(15-F06#0)
      DESTINATIONS: 1-F07#0,2-F07#1,3-F07#2
      BE: 10003
    INSTANCE(16-F06#1)
      DESTINATIONS: 1-F07#0,2-F07#1,3-F07#2
      BE: 10002
    INSTANCE(17-F06#2)
      DESTINATIONS: 1-F07#0,2-F07#1,3-F07#2
      BE: 10001

PLAN FRAGMENT 8(F02)
  DOP: 16
  INSTANCES
    INSTANCE(18-F02#0)
      DESTINATIONS: 15-F06#0,16-F06#1,17-F06#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0]
        2:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 9(F03)
  DOP: 16
  INSTANCES
    INSTANCE(19-F03#0)
      DESTINATIONS: 18-F02#0
      BE: 10001
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=1484,tabletID=1491
          2. partitionID=1484,tabletID=1497
          3. partitionID=1484,tabletID=1503
          4. partitionID=1484,tabletID=1509
    INSTANCE(20-F03#1)
      DESTINATIONS: 18-F02#0
      BE: 10002
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=1484,tabletID=1487
          2. partitionID=1484,tabletID=1493
          3. partitionID=1484,tabletID=1499
          4. partitionID=1484,tabletID=1505
    INSTANCE(21-F03#2)
      DESTINATIONS: 18-F02#0
      BE: 10003
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

PLAN FRAGMENT 10(F00)
  DOP: 16
  INSTANCES
    INSTANCE(22-F00#0)
      DESTINATIONS: 15-F06#0,16-F06#1,17-F06#2
      BE: 10001
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1440,tabletID=1445
          2. partitionID=1440,tabletID=1451
          3. partitionID=1440,tabletID=1457
          4. partitionID=1440,tabletID=1463
          5. partitionID=1440,tabletID=1469
          6. partitionID=1440,tabletID=1475
    INSTANCE(23-F00#1)
      DESTINATIONS: 15-F06#0,16-F06#1,17-F06#2
      BE: 10002
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1440,tabletID=1447
          2. partitionID=1440,tabletID=1453
          3. partitionID=1440,tabletID=1459
          4. partitionID=1440,tabletID=1465
          5. partitionID=1440,tabletID=1471
          6. partitionID=1440,tabletID=1477
    INSTANCE(24-F00#2)
      DESTINATIONS: 15-F06#0,16-F06#1,17-F06#2
      BE: 10003
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1440,tabletID=1443
          2. partitionID=1440,tabletID=1449
          3. partitionID=1440,tabletID=1455
          4. partitionID=1440,tabletID=1461
          5. partitionID=1440,tabletID=1467
          6. partitionID=1440,tabletID=1473
[end]

