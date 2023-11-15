# Colocate Join

本小节介绍如何使用 Colocate Join。

Colocate Join 功能是分布式系统实现 Join 数据分布的策略之一，能够减少数据多节点分布时 Join 操作引起的数据移动和网络传输，从而提高查询性能。

在 StarRocks 中使用 Colocate Join 功能，您需要在建表时为其指定一个 Colocation Group（CG），同一 CG 内的表需遵循相同的 Colocation Group Schema（CGS），即表对应的分桶副本具有一致的分桶键、副本数量和副本放置方式。如此可以保证同一 CG 内，所有表的数据分布在相同一组 BE 节点上。当 Join 列为分桶键时，计算节点只需做本地 Join，从而减少数据在节点间的传输耗时，提高查询性能。因此，Colocate Join，相对于其他 Join，例如 Shuffle Join 和 Broadcast Join，可以有效避免数据网络传输开销，提高查询性能。

Colocate Join 支持等值 Join。

## 使用 Colocate Join 功能

### 创建 Colocation 表

在建表时，您需要在 PROPERTIES 中指定属性 `"colocate_with" = "group_name"` 以创建一个 Colocate Join 表，并且指定其归属于特定的 Colocation Group。

> 说明
>
> 自 2.5.4 起，支持了对不同 Database 中的表执行 Colocate Join，您只需要在建表时指定相同的 `colocate_with` 属性即可。

示例：

~~~SQL
CREATE TABLE tbl (k1 int, v1 int sum)
DISTRIBUTED BY HASH(k1)
PROPERTIES(
    "colocate_with" = "group1"
);
~~~

如果指定的 CG 不存在，StarRocks 会自动创建一个只包含当前表的 CG，并指定当前表为该 CG 的 Parent Table。如果 CG 已存在，StarRocks 会检查当前表是否满足 CGS。如果满足，StarRocks 会创建该表，并将该表加入 Group。同时，StarRocks 会根据已存在的 Group 中的数据分布规则为当前表创建分片和副本。

一个 Group 归属于一个 Database。Group 名在一个 Database 内唯一，在内部存储中，Group 的全名为 `dbId_groupName`，但您只感知 `groupName`。
> 说明
>
> 如果您为不同 Database 的表指定了相同的 Colocation Group，以保证这些表互相 colocate，则该 Colocation Group 都会存在每个 Database ，您可以通过执行 `show proc "/colocation_group"` 查看每个 Database 包含的 Colocation Group 信息。

分桶键哈希值，对分桶数取模得到桶的序号（Bucket Seq）。假设一个表的分桶数为 8，则共有 \[0, 1, 2, 3, 4, 5, 6, 7\] 8 个分桶（Bucket)，每个分桶内会有一个或多个子表（Tablet)，子表数量取决于表的分区（Partition）数量：为单分区表时，一个分桶内仅有一个子表。如果是多分区表，则会有多个子表。

为了使得表能够有相同的数据分布，同一 CG 内的表必须满足下列约束：

* 同一 CG 内的表的分桶键的类型、数量和顺序完全一致，并且桶数一致，从而保证多张表的数据分片能够一一对应地进行分布控制。分桶键，即在建表语句中 `DISTRIBUTED BY HASH(col1, col2, ...)` 中指定一组列。分桶键决定了一张表的数据通过哪些列的值进行 Hash 划分到不同的 Bucket Seq 下。同 CG 的表的分桶键的名字可以不相同，分桶列的定义在建表语句中的出现次序可以不一致，但是在 `DISTRIBUTED BY HASH(col1, col2, ...)` 的对应数据类型的顺序要完全一致。
* 同一个 CG 内所有表的所有分区的副本数必须一致。如果不一致，可能出现某一个子表的某一个副本，在同一个 BE 上没有其他的表分片的副本对应。
* 同一个 CG 内所有表的分区键，分区数量可以不同。

同一个 CG 中的所有表的副本放置必须满足下列约束：

* CG 中所有表的 Bucket Seq 和 BE 节点的映射关系和 Parent Table 一致。
* Parent Table 中所有分区的 Bucket Seq 和 BE 节点的映射关系和第一个分区一致。
* Parent Table 第一个分区的 Bucket Seq 和 BE 节点的映射关系利用原生的 Round Robin 算法决定。

CG 内表的一致的数据分布定义和子表副本映射，能够保证分桶键取值相同的数据行一定在相同 BE 节点上，因此当分桶键做 Join 列时，只需本地 Join 即可。

### 删除 Colocation Group

当 Group 中最后一张表彻底删除后（彻底删除是指从回收站中删除。通常，一张表通过 `DROP TABLE` 命令被删除后，会在回收站默认停留一天的时间后，再被彻底删除），该 Group 也会被自动删除。

### 查看 Group 信息

您可以通过以下命令查看集群内已存在的 Group 信息。只有拥有 `root` 角色的用户才可以查看，不支持普通用户查看。

~~~sql
SHOW PROC '/colocation_group';
~~~

示例：

~~~Plain Text
mysql> SHOW PROC '/colocation_group';
+-------------+--------------+----------+------------+----------------+----------+----------+
| GroupId     | GroupName    | TableIds | BucketsNum | ReplicationNum | DistCols | IsStable |
+-------------+--------------+----------+------------+----------------+----------+----------+
| 11912.11916 | 11912_group1 | 11914    | 8          | 3              | int(11)  | true     |
+-------------+--------------+----------+------------+----------------+----------+----------+
~~~

|列名|描述|
|----|----|
|GroupId|一个 Group 的全集群唯一标识。前半部分为 DB ID，后半部分为 **Group ID**。|
|GroupName|Group 的全名。|
|TabletIds|该 Group 包含的表的 ID 列表。|
|BucketsNum|分桶数。|
|ReplicationNum|副本数。|
|DistCols|Distribution columns，即分桶列类型。|
|IsStable|该 Group 是否[稳定](#colocation-副本均衡和修复)。|

<br/>

您可以通过以下命令进一步查看特定 Group 的数据分布情况。

~~~sql
SHOW PROC '/colocation_group/GroupId';
~~~

示例：

~~~Plain Text
mysql> SHOW PROC '/colocation_group/11912.11916';
+-------------+---------------------+
| BucketIndex | BackendIds          |
+-------------+---------------------+
| 0           | 10002, 10004, 10003 |
| 1           | 10002, 10004, 10003 |
| 2           | 10002, 10004, 10003 |
| 3           | 10002, 10004, 10003 |
| 4           | 10002, 10004, 10003 |
| 5           | 10002, 10004, 10003 |
| 6           | 10002, 10004, 10003 |
| 7           | 10002, 10004, 10003 |
+-------------+---------------------+
8 rows in set (0.00 sec)
~~~

| 类名 | 描述 |
|------|------|
| BucketIndex |分桶序列的下标。|
| BackendIds |分桶中数据分片所在的 BE 节点 ID 列表。|

### 修改表 Group 属性

您可以通过以下命令修改表的 Colocation Group 属性。

~~~SQL
ALTER TABLE tbl SET ("colocate_with" = "group_name");
~~~

如果该表之前没有指定过 Group，则该命令会检查 Schema，并将该表加入到该 Group（如 Group 不存在则会创建）。如果该表之前有指定其他 Group，则该命令会先将该表从原有 Group 中移除，并将其加入新 Group（如 Group 不存在则会创建）。

您也可以通过以下命令，删除一个表的 Colocation 属性：

~~~SQL
ALTER TABLE tbl SET ("colocate_with" = "");
~~~

<br/>

### 其他相关操作

当对一个具有 Colocation 属性的表进行增加分区 `ADD PARTITION` 或修改副本数操作时，StarRocks 会检查该操作是否会违反 CGS，如果违反则会拒绝该操作。

## Colocation 副本均衡和修复

Colocation 表的副本分布需遵循 Group 中指定的分布，所以其副本修复和均衡相较于普通分片有所区别。

Group 具有 **IsStable** 属性，当 **IsStable** 为 **true** 时（即 Stable 状态），表示当前 Group 内的表的所有分片没有正在进行的变动，Colocation 特性可以正常使用。当 **IsStable** 为 **false** 时（即 Unstable 状态），表示当前 Group 内有部分表的分片正在做修复或迁移，此时，相关表的 Colocate Join 将退化为普通 Join。

### 副本修复

因副本只能存储在指定的 BE 节点上，所以当某个 BE 不可用时（例如宕机或 Decommission 等），StarRocks 需要寻找一个新的 BE 节点进行替换。StarRocks 会优先寻找负载最低的 BE 节点进行替换。替换后，该分桶内所有在旧 BE 节点上的数据分片都需要修复。迁移过程中，Group 被标记为 Unstable。

### 副本均衡

StarRocks 会将 Colocation 表的分片尽可能均匀地分布在所有 BE 节点上。对于普通表的副本均衡，是以单副本为粒度的，即单独为每一个副本寻找负载较低的 BE 节点。而 Colocation 表的均衡是分桶级别的，即一个分桶内的所有副本都会一起迁移。StarRocks 采用一种简单的均衡算法，即在不考虑副本实际大小，而只根据副本数量，将 Bucket Seq 均匀的分布在所有 BE 节点上。具体算法可以参阅 ColocateTableBalancer.java 中的代码注释。

> 注意
>
> * 当前的 Colocation 副本均衡和修复算法，对于异构部署的 StarRocks 集群效果并不理想。所谓异构部署，即 BE 节点的磁盘容量、数量、磁盘类型（SSD 和 HDD）不一致。在异构部署情况下，可能出现小容量的 BE 节点和大容量的 BE 节点存储了相同的副本数量的情况。
> * 当一个 Group 处于 Unstable 状态时，其中的表的 Colocate Join 将退化为普通 Join。此时集群的查询性能可能会极大降低。如果不希望系统自动均衡，您可以设置 FE 的配置项 `tablet_sched_disable_colocate_balance` 来禁止自动均衡。然后在合适的时间打开即可。具体参阅 [高级操作](#高级操作) 小节。

## 查询

对 Colocation 表的查询方式和普通表一样，用户无需感知 Colocation 属性。如果 Colocation 表所在的 Group 处于 Unstable 状态，将自动退化为普通 Join。

以下示例基于同一 CG 中的表 1 和表 2。

表 1：

~~~SQL
CREATE TABLE `tbl1` (
    `k1` date NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` int(11) SUM NOT NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`k1`, `k2`)
PARTITION BY RANGE(`k1`)
(
    PARTITION p1 VALUES LESS THAN ('2019-05-31'),
    PARTITION p2 VALUES LESS THAN ('2019-06-30')
)
DISTRIBUTED BY HASH(`k2`)
PROPERTIES (
    "colocate_with" = "group1"
);
~~~

表 2：

~~~SQL
CREATE TABLE `tbl2` (
    `k1` datetime NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` double SUM NOT NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k2`)
PROPERTIES (
    "colocate_with" = "group1"
);
~~~

查看 Join 查询计划：

~~~Plain Text
EXPLAIN SELECT * FROM tbl1 INNER JOIN tbl2 ON (tbl1.k2 = tbl2.k2);

+----------------------------------------------------+
| Explain String                                     |
+----------------------------------------------------+
| PLAN FRAGMENT 0                                    |
|  OUTPUT EXPRS:`tbl1`.`k1` |                        |
|   PARTITION: RANDOM                                |
|                                                    |
|   RESULT SINK                                      |
|                                                    |
|   2:HASH JOIN                                      |
|   |  join op: INNER JOIN                           |
|   |  hash predicates:                              |
|   |  colocate: true                                |
|   |    `tbl1`.`k2` = `tbl2`.`k2`                   |
|   |  tuple ids: 0 1                                |
|   |                                                |
|   |----1:OlapScanNode                              |
|   |       TABLE: tbl2                              |
|   |       PREAGGREGATION: OFF. Reason: null        |
|   |       partitions=0/1                           |
|   |       rollup: null                             |
|   |       buckets=0/0                              |
|   |       cardinality=-1                           |
|   |       avgRowSize=0.0                           |
|   |       numNodes=0                               |
|   |       tuple ids: 1                             |
|   |                                                |
|   0:OlapScanNode                                   |
|      TABLE: tbl1                                   |
|      PREAGGREGATION: OFF. Reason: No AggregateInfo |
|      partitions=0/2                                |
|      rollup: null                                  |
|      buckets=0/0                                   |
|      cardinality=-1                                |
|      avgRowSize=0.0                                |
|      numNodes=0                                    |
|      tuple ids: 0                                  |
+----------------------------------------------------+
~~~

以上示例中 Hash Join 节点显示 `colocate: true`，表示 Colocate Join 生效。

以下示例中 Colocate Join 没有生效：

~~~Plain Text
+----------------------------------------------------+
| Explain String                                     |
+----------------------------------------------------+
| PLAN FRAGMENT 0                                    |
|  OUTPUT EXPRS:`tbl1`.`k1` |                        |
|   PARTITION: RANDOM                                |
|                                                    |
|   RESULT SINK                                      |
|                                                    |
|   2:HASH JOIN                                      |
|   |  join op: INNER JOIN (BROADCAST)               |
|   |  hash predicates:                              |
|   |  colocate: false, reason: group is not stable  |
|   |    `tbl1`.`k2` = `tbl2`.`k2`                   |
|   |  tuple ids: 0 1                                |
|   |                                                |
|   |----3:EXCHANGE                                  |
|   |       tuple ids: 1                             |
|   |                                                |
|   0:OlapScanNode                                   |
|      TABLE: tbl1                                   |
|      PREAGGREGATION: OFF. Reason: No AggregateInfo |
|      partitions=0/2                                |
|      rollup: null                                  |
|      buckets=0/0                                   |
|      cardinality=-1                                |
|      avgRowSize=0.0                                |
|      numNodes=0                                    |
|      tuple ids: 0                                  |
|                                                    |
| PLAN FRAGMENT 1                                    |
|  OUTPUT EXPRS:                                     |
|   PARTITION: RANDOM                                |
|                                                    |
|   STREAM DATA SINK                                 |
|     EXCHANGE ID: 03                                |
|     UNPARTITIONED                                  |
|                                                    |
|   1:OlapScanNode                                   |
|      TABLE: tbl2                                   |
|      PREAGGREGATION: OFF. Reason: null             |
|      partitions=0/1                                |
|      rollup: null                                  |
|      buckets=0/0                                   |
|      cardinality=-1                                |
|      avgRowSize=0.0                                |
|      numNodes=0                                    |
|      tuple ids: 1                                  |
+----------------------------------------------------+
~~~

以上示例中，HASH JOIN 节点显示了 Colocate Join 没有生效以及对应原因：`colocate: false, reason: group is not stable`。同时 StarRocks 生成一个 EXCHANGE 节点。

## 高级操作

### FE 配置项

`tablet_sched_disable_colocate_balance`：是否关闭自动 Colocation 副本均衡功能。默认为 `false`，即不关闭。该参数只影响 Colocation 表的副本均衡，不影响普通表。

以上参数支持动态修改，您可以通过以下命令关闭。

~~~sql
ADMIN SET FRONTEND CONFIG ("tablet_sched_disable_colocate_balance" = "TRUE");
~~~

### Session 变量

`disable_colocate_join`：是否在 session 粒度关闭 Colocate Join 功能。默认为 `false`，即不关闭。

以上参数可以动态修改，您可以通过以下命令关闭。

~~~sql
SET disable_colocate_join = TRUE;
~~~

### HTTP Restful API

StarRocks 提供了多个与 Colocate Join 有关的 HTTP Restful API，用于查看和修改 Colocation Group。

该 API 在 FE 端实现，您可以使用 `fe_host:fe_http_port` 进行访问。访问需要 `cluster_admin` 角色对应的权限。

1. 查看集群的全部 Colocation 信息。

    ~~~bash
    curl --location-trusted -u<username>:<password> 'http://<fe_host>:<fe_http_port>/api/colocate'  
    ~~~

    示例：

    ~~~JSON
    // 返回以 Json 格式表示内部 Colocation 信息。
    {
        "colocate_meta": {
            "groupName2Id": {
                "g1": {
                    "dbId": 10005,
                    "grpId": 10008
                }
            },
            "group2Tables": {},
            "table2Group": {
                "10007": {
                    "dbId": 10005,
                    "grpId": 10008
                },
                "10040": {
                    "dbId": 10005,
                    "grpId": 10008
                }
            },
            "group2Schema": {
                "10005.10008": {
                    "groupId": {
                        "dbId": 10005,
                        "grpId": 10008
                    },
                    "distributionColTypes": [{
                        "type": "INT",
                        "len": -1,
                        "isAssignedStrLenInColDefinition": false,
                        "precision": 0,
                        "scale": 0
                    }],
                    "bucketsNum": 10,
                    "replicationNum": 2
                }
            },
            "group2BackendsPerBucketSeq": {
                "10005.10008": [
                    [10004, 10002],
                    [10003, 10002],
                    [10002, 10004],
                    [10003, 10002],
                    [10002, 10004],
                    [10003, 10002],
                    [10003, 10004],
                    [10003, 10004],
                    [10003, 10004],
                    [10002, 10004]
                ]
            },
            "unstableGroups": []
        },
        "status": "OK"
    }
    ~~~

2. 将 Group 标记为 Stable 或 Unstable。

    ~~~shell
    # 标记为 Stable。
    curl -XPOST --location-trusted -u<user>:<password> ​'http://<fe_host>:<fe_http_port>/api/colocate/group_stable?db_id=<dbId>&group_id=<grpId>​'

    # 标记为 Unstable。
   curl -XPOST --location-trusted -u<user>:<password> ​'http://<fe_host>:<fe_http_port>/api/colocate/group_unstable?db_id=<dbId>&group_id=<grpId>​'
    ~~~

    若返回为 `200`， 则表示标记修改成功。

3. 设置 Group 的数据分布。

    该接口可以强制设置某一 Group 的数据分布。

    > 注意
    > 使用该命令，需要将 FE 的配置 `tablet_sched_disable_colocate_balance` 设为 `true`，即关闭系统自动 Colocation 副本修复和均衡。否则在修改数据分布设置后可能会被系统自动重置。

    ~~~shell
    curl -u<user>:<password> -X POST "http://<fe_host>:<fe_http_port>/api/colocate/bucketseq?db_id=10005&group_id=10008"
    ~~~

    `Body:`

    `[[10004,10002],[10003,10002],[10002,10004],[10003,10002],[10002,10004],[10003,10002],[10003,10004],[10003,10004],[10003,10004],[10002,10004]]`

    `返回：200`

    其中 Body 是以嵌套数组表示的 Bucket Seq 以及每个分桶中分片所在 BE 的 ID。
