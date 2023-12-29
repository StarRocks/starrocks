---
displayed_sidebar: "English"
---

# Colocate Join

For shuffle join and broadcast join, if the join condition is met, the data rows of the two joining tables are merged into a single node to complete the join. Neither of these two join methods can avoid latency or overhead caused by data network transmission between nodes.

The core idea is to keep bucketing key, number of copies, and copy placement consistent for tables in the same Colocation Group. If the join column is a bucketing key, the computing node only needs to do local join without getting data from other nodes. Colocate Join supports equi joins.

This document introduces the principle, implementation, usage, and considerations of Colocate Join.

## Terminology

* **Colocation Group (CG)**: A CG will contain one or more Tables. The Tables within a CG have the same bucketing and replica placement, and are described using the Colocation Group Schema.
* **Colocation Group Schema (CGS)**: A CGS contains the bucketing key, number of buckets, and number of replicas of a CG.

## Principle

Colocate Join is to form a CG with a set of Tables having the same CGS, and ensure that the corresponding bucket copies of these Tables will fall on the same set of BE nodes. When the tables in the CG perform Join operations on the bucketed columns, the local data can be joined directly, saving time from transferring data between nodes.

Bucket Seq is obtained by `hash(key) mod buckets`. Suppose a Table has 8 buckets, then there are \[0, 1, 2, 3, 4, 5, 6, 7\] 8 buckets, and each Bucket has one or more sub-tables, the number of sub-tables depends on the number of partitions. If it is a multi-partitioned table, there will be multiple tablets.

In order to have the same data distribution, tables within the same CG must comply with the following.

1. Tables within the same CG must have the identical  bucketing key (type, number, order) and the same number of buckets so that the data slices of multiple tables can be distributed and controlled one by one. The bucketing key is the columns specified in the table creation statement `DISTRIBUTED BY HASH(col1, col2, ...)`. The bucketing key determines which columns of data are Hashed into different Bucket Seqs. The name of the bucketing key can vary for tables within the same CG.The bucketing columns can be different in the creation statement, but the order of the corresponding data types in `DISTRIBUTED BY HASH(col1, col2, ...)` should be exactly the same .
2. Tables within the same CG must have the same number of partition copies. If not, it may happen that a tablet copy has no corresponding copy in the partition of  the same BE.
3. Tables within the same CG may have different numbers of partitions and different partition keys.

When creating a table, the CG is specified by the attribute `"colocate_with" = "group_name"` in the table PROPERTIES. If the CG does not exist, it means the table is the first table of the CG and called Parent Table. The data distribution of the Parent Table (type, number and order of split bucket keys, number of copies and number of split buckets) determines the CGS. If the CG exists, check whether the data distribution of the table is consistent with the CGS.

The copy placement of tables within the same CG satisfies:

1. The mapping between the Bucket Seq and BE nodes of all the Tables is the same as that of the Parent Table.
2. The mapping between the Bucket Seq and BE nodes of all the Partitions in the Parent Table is the same as that of the first Partition.
3. The mapping between the Bucket Seq and BE nodes of the first Partition of the Parent Table is determined using the native Round Robin algorithm.

The consistent data distribution and mapping guarantee that the data rows with the same value taken by bucketing key fall on the same BE. Therefore, when using the bucketing key to  join columns, only local joins are required.

## Usage

### Table creation

When creating a table, you can specify the attribute `"colocate_with" = "group_name"` in PROPERTIES to indicate that the table is a Colocate Join table and belongs to a specified Colocation Group.
> **NOTE**
>
> From version 2.5.4, Colocate Join can be performed on tables from different databases. You only need to specify the same `colocate_with` property when you create tables.

For example:

~~~SQL
CREATE TABLE tbl (k1 int, v1 int sum)
DISTRIBUTED BY HASH(k1)
BUCKETS 8
PROPERTIES(
    "colocate_with" = "group1"
);
~~~

If the specified Group does not exist, StarRocks automatically creates a Group that only contains the current table. If the Group exists, StarRocks checks to see if the current table meets the Colocation Group Schema. If so, it creates the table and adds it to the Group. At the same time, the table creates a partition and a tablet based on the data distribution rules of the existing Group.

A Colocation Group belongs to a database. The name of a Colocation Group is unique within a database. In the internal storage, the full name of the Colocation Group is `dbId_groupName`, but you only perceive `groupName`.
> **NOTE**
>
> If you specify the same Colocation Group to associate tables from different databases for Colocate Join, the Colocation Group exists in each of these databases. You can run `show proc "/colocation_group"` to check the Colocation Groups in different databases.

### Delete

 A complete deletion is a deletion from the Recycle Bin. Normally, after a table is deleted with the `DROP TABLE` command, by default it will stay in the recycle bin for a day before being deleted). When the last table in a Group is completely deleted, the Group will also be deleted automatically.

### View group information

The following command allows you to view the Group information that already exists in the cluster.

~~~Plain Text
SHOW PROC '/colocation_group';

+-------------+--------------+--------------+------------+----------------+----------+----------+
| GroupId     | GroupName    | TableIds     | BucketsNum | ReplicationNum | DistCols | IsStable |
+-------------+--------------+--------------+------------+----------------+----------+----------+
| 10005.10008 | 10005_group1 | 10007, 10040 | 10         | 3              | int(11)  | true     |
+-------------+--------------+--------------+------------+----------------+----------+----------+
~~~

* **GroupId**: The cluster-wide unique identifier of a Group, with the first half being the db id and the second half being the group id.
* **GroupName**: The full name of the Group.
* **TabletIds**: The list of ids of the Tables  in the Group.
* **BucketsNum**: The number of buckets.
* **ReplicationNum**: The number of replicas.
* **DistCols**: Distribution columns, i.e. bucketing column type.
* **IsStable**: Whether the Group is stable (for the definition of stability, see the section of Colocation Replica Balancing and Repair).

You can further view the data distribution of a Group with the following command.

~~~Plain Text
SHOW PROC '/colocation_group/10005.10008';

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

* **BucketIndex**: Subscript of the sequence of buckets.
* **BackendIds**: The ids of BE nodes where the bucketing data slices t are located.

> Note: The above command requires AMDIN privilege. Regular users cannot access it.

### Modifying Table Group Properties

You can modify the Colocation Group properties of a table. For example:

~~~SQL
ALTER TABLE tbl SET ("colocate_with" = "group2");
~~~

If the table has not been assigned to a Group before, the command will check the Schema and add the table to the Group (the Group will be created first if it does not exist). If the table has been previously assigned to another Group, the command will remove the table from the original Group and add it to the new Group (the Group will be created first if it does not exist).

You can also remove the Colocation properties of a table with the following command.

~~~SQL
ALTER TABLE tbl SET ("colocate_with" = "");
~~~

### Other related operations

When adding a partition using `ADD PARTITION` or modifying the number of copies to a table with the Colocation attribute, StarRocks checks if the operation will violate the Colocation Group Schema and rejects it if it does.

## Colocation replica balancing and repair

The replicas distribution of a Colocation table needs to follow the distribution rules specified in the Group schema, so it differs from normal sharding in terms of replica repair and balancing.

The Group itself has a `stable` property. When `stable` is `true`, it means that no changes are being made to table slices in the Group and the Colocation feature is working properly. When `stable` is `false`, it means that some table slices in the current Group are being repaired or migrated, and the Colocate Join of the affected tables will degrade to a normal Join.

### Replica repair

Replicas can only be stored on the specified BE node. StarRocks will look for the least loaded BE to replace an unavailable BE (e.g.,down, decommission),. After the replacement, all the bucketing data slices on the old BE are repaired. During migration, the Group is marked as **Unstable**.

### Replica balancing

StarRocks tries to distribute the Colocation table slices evenly across all BE nodes. Balancing for normal tables is at the  replica level, that is, each replica individually finds a lower-load BE node. Balancing for Colocation tables is at the Bucket level, that is, all replicas within a Bucket are migrated together. We use a simple balancing algorithm that distributes `BucketsSequnce` evenly across all BE nodes without considering the actual size of the replicas but only the number of replicas. The exact algorithm can be found in the code comments in `ColocateTableBalancer.java`.

> Note 1: The current Colocation replica balancing and repair algorithm may not work well for StarRocks clusters with heterogeneous deployment. The so-called heterogeneous deployment means that the disk capacity, number of disk, and disk type (SSD and HDD) of BE nodes are not consistent. In the case of heterogeneous deployment, it may happen that a small-capacity BE node stores the same number of replicas as a large-capacity BE node.
>
> Note 2: When a Group is in the Unstable state, the Join of its tables will degrade to a normal Join, which may significantly degrade the query performance of the cluster. If you do not want the system to be automatically balanced, set the FE configuration `disable_colocate_balance` to disable automatic balancing and  enable it back at the appropriate time. (See the section Advanced Operations (#Advanced Operations) for details)

## Query

The Colocation table is queried in the same way as a normal table. If the Group where the Colocation table is located is in Unstable state, it will automatically degrade to a normal Join, as illustrated by the following example.

Table 1:

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
DISTRIBUTED BY HASH(`k2`)  BUCKETS 6
PROPERTIES (
    "colocate_with" = "group1"
);
INSERT INTO tbl1
VALUES
    ("2015-09-12",1000,1),
    ("2015-09-13",2000,2);
~~~

Table 2:

~~~SQL
CREATE TABLE `tbl2` (
    `k1` datetime NOT NULL COMMENT "",
    `k2` int(11) NOT NULL COMMENT "",
    `v1` double SUM NOT NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k2`)  BUCKETS 6
PROPERTIES (
    "colocate_with" = "group1"
);
INSERT INTO tbl2
VALUES
    ("2015-09-12 00:00:00",3000,3),
    ("2015-09-12 00:00:00",4000,4);
~~~

View query plan:

~~~Plain Text
EXPLAIN SELECT * FROM tbl1 INNER JOIN tbl2 ON (tbl1.k2 = tbl2.k2);
+-------------------------------------------------------------------------+
| Explain String                                                          |
+-------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                         |
|  OUTPUT EXPRS:1: k1 | 2: k2 | 3: v1 | 4: k1 | 5: k2 | 6: v1             |
|   PARTITION: UNPARTITIONED                                              |
|                                                                         |
|   RESULT SINK                                                           |
|                                                                         |
|   3:EXCHANGE                                                            |
|                                                                         |
| PLAN FRAGMENT 1                                                         |
|  OUTPUT EXPRS:                                                          |
|   PARTITION: RANDOM                                                     |
|                                                                         |
|   STREAM DATA SINK                                                      |
|     EXCHANGE ID: 03                                                     |
|     UNPARTITIONED                                                       |
|                                                                         |
|   2:HASH JOIN                                                           |
|   |  join op: INNER JOIN (COLOCATE)                                     |
|   |  colocate: true                                                     |
|   |  equal join conjunct: 5: k2 = 2: k2                                 |
|   |                                                                     |
|   |----1:OlapScanNode                                                   |
|   |       TABLE: tbl1                                                   |
|   |       PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join |
|   |       partitions=1/2                                                |
|   |       rollup: tbl1                                                  |
|   |       tabletRatio=6/6                                               |
|   |       tabletList=15344,15346,15348,15350,15352,15354                |
|   |       cardinality=1                                                 |
|   |       avgRowSize=3.0                                                |
|   |                                                                     |
|   0:OlapScanNode                                                        |
|      TABLE: tbl2                                                        |
|      PREAGGREGATION: OFF. Reason: None aggregate function               |
|      partitions=1/1                                                     |
|      rollup: tbl2                                                       |
|      tabletRatio=6/6                                                    |
|      tabletList=15373,15375,15377,15379,15381,15383                     |
|      cardinality=1                                                      |
|      avgRowSize=3.0                                                     |
+-------------------------------------------------------------------------+
40 rows in set (0.03 sec)
~~~

If a Colocate Join takes effect, the Hash Join node displays `colocate: true`.

If it doesn’t take effect, the query plan is as follows:

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

The HASH JOIN node will show the corresponding reason: `colocate: false, reason: group is not stable`. An EXCHANGE node will be generated at the same time.

## Advanced operations

### FE configuration items

* **disable_colocate_relocate**

Whether to disable automatic Colocation replica repair for StarRocks. The default is false, which means it is turned on. This parameter only affects replica repair for Colocation tables, not for normal tables.

* **disable_colocate_balance**

Whether to disable automatic Colocation replica balancing for StarRocks. The default is false, which means it is turned on. This parameter only affects the replica balancing of Colocation tables, not for normal tables.

* **disable_colocate_join**

    You can disable Colocate join at session granularity by changing this variable.

* **disable_colocate_join**

    The Colocate join function can be disabled by changing this variable.

### HTTP Restful API

StarRocks provides several HTTP Restful APIs related to Colocate Join for viewing and modifying Colocation Groups.

This API is implemented on the FE and can be accessed using `fe_host:fe_http_port` with ADMIN permissions.

1. View all Colocation information of a cluster

    ~~~bash
    curl --location-trusted -u<username>:<password> 'http://<fe_host>:<fe_http_port>/api/colocate'  
    ~~~

    ~~~JSON
    // Returns the internal Colocation information in Json format.
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

2. Mark the Group as Stable or Unstable

    ~~~bash
    # Mark as Stable
    curl -XPOST --location-trusted -u<username>:<password> ​'http://<fe_host>:<fe_http_port>/api/colocate/group_stable?db_id=<dbId>&group_id=<grpId>​'
    # Mark as Unstable
    curl -XPOST --location-trusted -u<username>:<password> ​'http://<fe_host>:<fe_http_port>/api/colocate/group_unstable?db_id=<dbId>&group_id=<grpId>​'
    ~~~

    If the returned result is `200`, the Group is successfully marked as Stable or Unstable.

3. Set the data distribution of a Group

    This interface allows you to force the number distribution of a Group.

    `POST /api/colocate/bucketseq?db_id=10005&group_id= 10008`

    `Body:`

    `[[10004,10002],[10003,10002],[10002,10004],[10003,10002],[10002,10004],[10003,10002],[10003,10004],[10003,10004],[10003,10004],[10002,10004]]`

    `returns: 200`

    Where `Body` is `BucketsSequence` represented as a nested array and the ids of the BEs in which bucketing slices are located.

    > Note that to use this command, you may need to set the FE configuration `disable_colocate_relocate` and `disable_colocate_balance` to true, that is, to disable the system to perform automatic Colocation replica repair and balancing. Otherwise, it may be automatically reset by the system after modification.
