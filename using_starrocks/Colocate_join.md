# Colocate Join

本小节介绍如何使用 Colocate join。

Colocate Join 功能，属于分布式系统实现 Join 数据分布的策略之一。 能够减少数据分布在多个节点引起的 Join 时的数据移动和网络传输，从而提高查询性能。
Colocate Join 使用 Colocation Group（CG）管理一组表 ，同一 CG 内的表 Colocation Group Schema（CGS）相同，即表对应的分桶副本具有一致的分桶键、副本数量和副本放置方式 。这样可以保证同一 CG 内，表的数据分布在相同一组 BE 节点上。当 Join 列为分桶键时，计算节点只需做本地 Join，因而可以减少数据在节点间的传输耗时，提高查询性能。
因此，Colocation Join，相对于其他 Join，例如 Shuffle Join 和 Broadcast Join，可以避免数据网络传输开销，提高查询性能。

**名词解释：**

* **Colocation Group（CG）**：一个 CG 中会包含一张及以上的 Table。一个CG内的 Table 有相同的分桶方式和副本放置方式，使用 Colocation Group Schema 描述。
* **Colocation Group Schema（CGS）**： 包含 CG 的分桶键，分桶数以及副本数等信息。

## 使用 Colocate Join 功能

### 创建 Colocation 表

建表时，可以在 PROPERTIES 中指定属性 `"colocate_with" = "group_name"`，表示这个表是一个 Colocate Join 表，并且归属于一个指定的 Colocation Group。

> 注意：StarRocks 仅支持对**同一 Database** 中的表进行 Colocate Join 操作。

~~~SQL
CREATE TABLE tbl (k1 int, v1 int sum)
DISTRIBUTED BY HASH(k1)
BUCKETS 8
PROPERTIES(
    "colocate_with" = "group1"
);
~~~

如果指定的 CG 不存在，StarRocks 会自动创建一个只包含当前表的 CG，并指定当前表为该 CG 的 Parent Table。如果 CG 已存在，StarRocks 会检查当前表是否满足 CGS。如果满足，StarRocks 会创建该表，并将该表加入 Group。同时，StarRocks 会根据已存在的 Group 中的数据分布规则为当前表创建分片和副本。

Group 归属于一个 Database，Group 名在一个 Database 内唯一。在内部存储中，Group 的全名为 dbId_groupName，但用户只感知 groupName。

分桶键 hash 值，对分桶数取模得到桶的序号(Bucket Seq)， 假设一个 Table 的分桶数为 8，则共有 \[0, 1, 2, 3, 4, 5, 6, 7\] 8 个分桶（Bucket)，每个 Bucket 内会有一个或多个子表（Tablet)，子表数量取决于表的分区数(Partition)：为单分区表时，一个 Bucket 内仅有一个 Tablet。如果是多分区表，则会有多个Tablet。

为了使得 Table 能够有相同的数据分布，同一 CG 内的 Table 必须保证下列约束：

1. 同一 CG 内的 Table 的分桶键的类型、数量和顺序完全一致，并且桶数一致，这样才能保证多张表的数据分片能够一一对应地进行分布控制。分桶键，即在建表语句中 `DISTRIBUTED BY HASH(col1, col2, ...)` 中指定一组列。分桶键决定了一张表的数据通过哪些列的值进行 Hash 划分到不同的 Bucket Seq 中。同 CG 的 table 的分桶键的名字可以不相同，分桶列的定义在建表语句中的出现次序可以不一致，但是在 `DISTRIBUTED BY HASH(col1, col2, ...)` 的对应数据类型的顺序要完全一致。
2. 同一个 CG 内所有表的所有分区（Partition）的副本数必须一致。如果不一致，可能出现某一个 Tablet 的某一个副本，在同一个 BE 上没有其他的表分片的副本对应。
3. 同一个 CG 内所有表的分区键，分区数量可以不同。

同一个CG中的所有表的副本放置满足:

1. CG中所有 Table 的 Bucket Seq 和 BE 节点的映射关系和 Parent Table 一致。
2. Parent Table 中所有 Partition 的 Bucket Seq 和 BE 节点的映射关系和第一个 Partition 一致。
3. Parent Table 第一个 Partition 的 Bucket Seq 和 BE 节点的映射关系利用原生的 Round Robin 算法决定。

CG内表的一致的数据分布定义和子表副本映射，能够保证分桶键取值相同的数据行一定在相同BE上，因此当分桶键做join列时，只需本地join即可。

### 删除 Colocation 表

当 Group 中最后一张表彻底删除后（彻底删除是指从回收站中删除。通常，一张表通过 `DROP TABLE` 命令删除后，会在回收站默认停留一天的时间后，再删除），该 Group 也会被自动删除。

### 查看 Group 信息

通过以下命令查看集群内已存在的 Group 信息。

~~~sql
SHOW PROC '/colocation_group';
~~~

~~~Plain Text
+-------------+--------------+--------------+------------+----------------+----------+----------+
| GroupId     | GroupName    | TableIds     | BucketsNum | ReplicationNum | DistCols | IsStable |
+-------------+--------------+--------------+------------+----------------+----------+----------+
| 10005.10008 | 10005_group1 | 10007, 10040 | 10         | 3              | int(11)  | true     |
+-------------+--------------+--------------+------------+----------------+----------+----------+
~~~

|列明|描述|
|----|----|
|GroupId|一个 Group 的全集群唯一标识。前半部分为 db id，后半部分为 **group id**。|
|GroupName|Group 的全名|
|TabletIds|该 Group 包含的 Table 的 id 列表|
|BucketsNum|分桶数|
|ReplicationNum|副本数|
|DistCols|Distribution columns，即分桶列类型。|
|IsStable|该 Group 是否[稳定](https://docs.starrocks.com/zh-cn/main/administration/Replica)|

<br/>

通过以下命令进一步查看一个 Group 的数据分布情况。

~~~sql
SHOW PROC '/colocation_group/10005.10008';
~~~

~~~Plain Text
+-------------+---------------------+
| BucketIndex | BackendIds          |
+-------------+---------------------+
| 0           | 10004, 10002, 10001 |
| 1           | 10003, 10002, 10004 |
| 2           | 10002, 10004, 10001 |
| 3           | 10003, 10002, 10004 |
| 4           | 10002, 10004, 10003 |
| 5           | 10003, 10002, 10001 |
| 6           | 10003, 10004, 10001 |
| 7           | 10003, 10004, 10002 |
+-------------+---------------------+
~~~

| 类名 | 描述 |
|------|------|
| BucketIndex |分桶序列的下标。|
| BackendIds |分桶中数据分片所在的 BE 节点 id 列表。|

> 注意：以上命令需要 ADMIN 权限。暂不支持普通用户查看。

### 修改表 Group 属性

通过以下命令修改表的 Colocation Group 属性。

~~~SQL
ALTER TABLE tbl SET ("colocate_with" = "group2");
~~~

如果该表之前没有指定过 Group，则该命令检查 Schema，并将该表加入到该 Group（Group 不存在则会创建）。如果该表之前有指定其他 Group，则该命令会先将该表从原有 Group 中移除，并加入新 Group（Group 不存在则会创建）。

也可以通过以下命令，删除一个表的 Colocation 属性：

~~~SQL
ALTER TABLE tbl SET ("colocate_with" = "");
~~~

<br/>

### 其他相关操作

当对一个具有 Colocation 属性的表进行增加分区（ADD PARTITION）、修改副本数时，StarRocks 会检查修改是否会违反 Colocation Group Schema，如果违反则会拒绝。

## Colocation 副本均衡和修复

Colocation 表的副本分布需遵循 Group 中指定的分布，所以在其副本修复和均衡相较于普通分片有所区别。

Group 具有 **Stable** 属性，当 **Stable** 为 **true** 时（Stable），表示当前 Group 内的表的所有分片没有正在进行变动，Colocation 特性可以正常使用。当 **Stable** 为 **false** 时（Unstable），表示当前 Group 内有部分表的分片正在做修复或迁移，此时，相关表的 Colocate Join 将退化为普通 Join。

### 副本修复

因副本只能存储在指定的 BE 节点上，所以当某个 BE 不可用时（例如宕机或 Decommission 等），StarRocks 需要寻找一个新的 BE 进行替换。StarRocks 会优先寻找负载最低的 BE 进行替换。替换后，该 Bucket 内所有在旧 BE 上的数据分片都要做修复。迁移过程中，Group 被标记为 Unstable。

### 副本均衡

StarRocks 会尽力将 Colocation 表的分片均匀分布在所有 BE 节点上。对于普通表的副本均衡，是以单副本为粒度的，即单独为每一个副本寻找负载较低的 BE 节点即可。而 Colocation 表的均衡是 Bucket 级别的，即一个 Bucket 内的所有副本都会一起迁移。我们采用一个简单的均衡算法，即在不考虑副本实际大小，而只根据副本数量，将 BucketsSequnce 均匀的分布在所有 BE 上。具体算法可以参阅 ColocateTableBalancer.java 中的代码注释。

> 注1：当前的 Colocation 副本均衡和修复算法，对于异构部署的 StarRocks 集群效果可能不佳。所谓异构部署，即 BE 节点的磁盘容量、数量、磁盘类型（SSD 和 HDD）不一致。在异构部署情况下，可能出现小容量的 BE 节点和大容量的 BE 节点存储了相同的副本数量。
>
> 注2：当一个 Group 处于 Unstable 状态时，其中的表的 Join 将退化为普通 Join。此时可能会极大降低集群的查询性能。如果不希望系统自动均衡，可以设置 FE 的配置项 disable_colocate_balance 来禁止自动均衡。然后在合适的时间打开即可。（具体参阅 [高级操作](#高级操作) 一节）

## 查询

对 Colocation 表的查询方式和普通表一样，用户无需感知 Colocation 属性。如果 Colocation 表所在的 Group 处于 Unstable 状态，将自动退化为普通 Join。

表1：

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
DISTRIBUTED BY HASH(`k2`) BUCKETS 8
PROPERTIES (
    "colocate_with" = "group1"
);
~~~

表2：

~~~SQL
CREATE TABLE `tbl2` (
    `k1` datetime NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` double SUM NOT NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k2`) BUCKETS 8
PROPERTIES (
    "colocate_with" = "group1"
);
~~~

查看查询计划：

~~~Plain Text
DESC SELECT * FROM tbl1 INNER JOIN tbl2 ON (tbl1.k2 = tbl2.k2);

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

如果 Colocation Join 生效，则 Hash Join 节点会显示 `colocate: true`。

如果没有生效，则查询计划如下：

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

HASH JOIN 节点会显示对应原因：`colocate: false, reason: group is not stable`。同时会有一个 EXCHANGE 节点生成。

## 高级操作

### FE 配置项

* **disable_colocate_balance**

    是否关闭 StarRocks 的自动 Colocation 副本均衡。默认为 false，即不关闭。该参数只影响 Colocation 表的副本均衡，不影响普通表。

以上参数可以动态修改，设置方式请参阅 `HELP ADMIN SHOW CONFIG;` 和 `HELP ADMIN SET CONFIG;`。

### Session 变量

* **disable_colocate_join**

    可以通过设置该变量在 session 粒度关闭 colocate join 功能。

以上参数可以动态修改，设置方式请参阅《[系统变量](../reference/System_variable.md)》章节。

### HTTP Restful API

StarRocks 提供了几个和 Colocation Join 有关的 HTTP Restful API，用于查看和修改 Colocation Group。

该 API 实现在 FE 端，使用 fe_host:fe_http_port 进行访问，需要 ADMIN 权限。

1. 查看集群的全部 Colocation 信息

    `GET /api/colocate`

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

2. 将 Group 标记为 Stable 或 Unstable

    * 标记为 Stable

        `POST /api/colocate/group_stable?db_id=10005&group_id=10008`

        `返回：200`

    * 标记为 Unstable

        `POST /api/colocate/group_unstable?db_id=10005&group_id=10008`

        `返回：200`

3. 设置 Group 的数据分布

    该接口可以强制设置某一 Group 的数据分布。

    `POST /api/colocate/bucketseq?db_id=10005&group_id= 10008`

    `Body:`

    `[[10004,10002],[10003,10002],[10002,10004],[10003,10002],[10002,10004],[10003,10002],[10003,10004],[10003,10004],[10003,10004],[10002,10004]]`

    `返回：200`

    其中 Body 是以嵌套数组表示的 BucketsSequence 以及每个 Bucket 中分片所在 BE 的 id。

    > 注意，使用该命令，可能需要将 FE 的配置 disable_colocate_balance 设为 true。即关闭系统自动的 Colocation 副本修复和均衡。否则可能在修改后，会被系统自动重置。
