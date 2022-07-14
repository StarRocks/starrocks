# Replica management

## Terminology

* Tablet: Logical slice of a table, a table has multiple tablets.
* Replica: Copy of a tablet, 3 replicas per tablet by default.
* Healthy Replica: A replica whose backend is alive and version is complete.
* TabletChecker (TC): A resident background thread that periodically scans all Tablets and decides whether to send tablets to TabletScheduler based on their status.
* TabletScheduler (TS): A resident background thread that processes tablets sent by TabletChecker, and also performs cluster replica balancing.
* TabletSchedCtx (TSC): A wrapper for a tablet. When a tablet is selected by TC, it is encapsulated as a TSC and sent to TS.
* Storage Medium：StarRocks supports different storage media for partition granularity, including SSD and HDD. The scheduling of replicas varies for different storage media.

## Status

### Replica Status

The health states of a Replica are as follows.

* **BAD**

The replica is corrupted. This includes, but is not limited to, unrecoverable damage to the replica caused by disk failures, bugs, etc.

* **VERSION_MISSING**

Each batch import corresponds to one version of data. You should see several consecutive versions in one replica. Due to import errors, some replicas may have incomplete versions of data.

* **HEALTHY**

The replica has correct data and the BE node where it is located is in normal state (with a normal heartbeat and is not in the process of being taken offline).

### Tablet Status

The health state of a Tablet is determined by the status of all its replicas:

* **REPLICA_MISSING**

Replica missing. That is, the number of surviving replicas is less than the expected number of replicas.

* **VERSION_INCOMPLETE**

The number of surviving replicas is greater than or equal to the expected number, but the number of healthy replicas is less than the expected number.

* **REPLICA_RELOCATING**

The number of surviving replicas that have full version is equal to `replication num`. But the BE node where some of the replicas are located is unavailable (e.g., in decommission status).

* **REPLICA_MISSING_IN_CLUSTER**

When using multiple clusters, the number of healthy replicas is greater than or equal to the expected number, but the number of replicas within the corresponding cluster is less than the expected number.

* **REDUNDANT**

Replica redundancy. All healthy replicas are in the corresponding cluster, but the total number of replicas is greater than the expected numbers due to redundant unavailable replicas.

* **FORCE_REDUNDANT**

This is a special status. It only occurs when the expected number of replicas is greater than or equal to the number of available nodes, and the Tablet is in the replica missing state. In this case, one replica needs to be deleted first to ensure that there are available nodes for creating new replicas.

* **COLOCATE_MISMATCH**

Tablet status for the Colocation attribute, indicating that the sharded replica does not match the distribution specified by the Colocation Group.

* **COLOCATE_REDUNDANT**

Tablet status for the Colocation attribute, indicating that the tablet replica of the Colocation table is redundant.

* **HEALTHY**

Healthy tablets.

## Replica Repair

TabletChecker periodically checks the status of all tablets. Unhealthy tablets are handed over to TabletScheduler for scheduling and repairing. The repair is done on the BE as a clone task. The FE is responsible for generating the clone task.

> Note 1: The main idea of replica repair is to first create or patch new replicas to reach the desired value, and then delete any redundant replicas.
> Note 2: A clone task is to copy target data from a remote BE to a destination BE.

For different states, we use different methods to repair.

1. REPLICA_MISSING/REPLICA_RELOCATING
   Select an available BE node as the destination BE. Select a healthy replica as the source. The clone task will copy a full replica from the source to the destination BE. For replica replenishment, an available BE node will be selected regardless of the storage medium.
2. VERSION_INCOMPLETE
   Select a relatively complete replica as the destination. Select a healthy replica as the source. The clone task will copy the missing version from the source to the destination.
3. REPLICA_MISSING_IN_CLUSTER
   This status is handled in the same way as REPLICA_MISSING.
4. REDUNDANT
  Usually after a replica repair, the tablet will have redundant replicas. Choose a redundant replica and delete it. The selection of redundant replicas follows this order:

   * The BE where the replica is located is already offline
   * The replica is corrupted
   * The BE where the replica is located is out of connection or in the process of being taken offline
   * The replica is in CLONE state, which is an intermediate state during clone task execution)
   * The replica has a missing version
   * The cluster where the replica is located is incorrect
   * The BE node where the replica is located is heavily loaded

5. FORCE_REDUNDANT
   Although the Tablet has a missing replica, there are no available nodes to create a new one. In this case, a replica must be deleted first to free up a node for creating a new replica. The order of deleting replicas is the same as REDUNDANT.
6. COLOCATE_MISMATCH
   Select one of the BE nodes specified in the Colocation Group as the destination node for replica replenishment.
7. COLOCATE_REDUNDANT
   Delete a replica on a BE node that is not specified in the Colocation Group.

StarRocks does not deploy replicas of the same Tablet on different BEs of the same host, which ensures that even if all BEs of the same host fail, some replicas are still alive.

## Replica Scheduling

### Scheduling priority

Tablets waiting to be scheduled in TabletScheduler are given different priorities depending on their status. Tablets with higher priority will be scheduled first. Priority levels are as follows:

* VERY_HIGH

* In `REDUNDANT` state. Tablets with redundant replicas are given very high priority. Although the replica redundancy is the least urgent, it is the fastest to process and free up resources (such as disk space).
* In `FORCE_REDUNDANT` state. Same as above.

* HIGH

* In `REPLICA_MISSING` state and most replicas are missing (e.g., 2 of 3 replicas are missing)
* In `VERSION_INCOMPLETE` state and most replicas have missing versions
* In `COLOCATE_MISMATCH` state. Tablets associated with the Colocation table will be fixed as soon as possible.
* In `COLOCATE_REDUNDANT` state.

* NORMAL

* In `REPLICA_MISSING` state but most replicas survive (e.g. 1 of 3 replicas is missing)
* In `VERSION_INCOMPLETE` state but most replicas have complete versions
* In `REPLICA_RELOCATING` state and most replicas need to be relocated (e.g. 3 copies with 2)

* LOW

* In `REPLICA_MISSING_IN_CLUSTER` state
* In `REPLICA_RELOCATING` state but most replicas are stable

### Manually determined priority

The system will automatically determine the scheduling priority. However, there are times when users want certain tablets to be repaired faster. Therefore, we provide a command where the user can specify tablets of a certain table or partition to be repaired first.

ADMIN REPAIR TABLE tbl [PARTITION (p1, p2, ...)] ;

This command tells TC to give `VERY_HIGH` priority to the problematic Tablet  so it can be repaired first.

> Note: This command does not guarantee a successful repair, and the priority will change with the scheduling of TS. This information will be lost when the leader FE is changed or restarted.

Prioritization can be canceled with the following command.

ADMIN CANCEL REPAIR TABLE tbl [PARTITION (p1, p2, ...)] ;

### Priority Scheduling

Prioritization ensures that severely damaged tablets are repaired first. However, if a high-priority repair task keeps failing, it will result in low-priority tasks remaining unscheduled. Therefore, we dynamically adjust the priority based on the status of each task to ensure that all tasks have a chance to be scheduled.

* If the scheduling fails for 5 consecutive times (e.g., unable to acquire resources, unable to find a suitable source or destination, etc.), the task will be deprioritized.
* A task will be prioritized if it has not been scheduled for 30 minutes.
* The priority of a task can’t be adjusted twice within five minutes.

To ensure that the initial priority level is weighted, `VERY_HIGH` tasks can only be deprioritized to `NORMAL` at most, and `LOW` tasks can only be prioritized to `HIGH` at most. The priority adjustment also applies to tasks whose priority is manually set by users.

## Replica Balancing

StarRocks automatically performs replica balancing within a cluster. The main idea of balancing is to create  replicas on a low-load node and delete replicas on the high-load node. More than one storage medium may exist on different BE nodes within the same cluster.   To keep tablets in their original storage medium after balancing, BE nodes are divided based on storage media  , allowing the load balancing to be wisely scheduled .

Similarly, replica balancing ensures that no replicas of the same Tablet are deployed on BEs of the same host.

### BE Node Load

ClusterLoadStatistics (CLS) shows the load balancing of each Backend in a cluster and triggersTabletScheduler to balance the cluster. Currently, we  `**Disk Utilization**` and `**Number of Replicas**` to calculate `loadScore` for each BE. The higher the score, the heavier the load on the BE.

`capacityCoefficient` and `replicaNumCoefficient` (sum to 1) are the weighting factors for `Disk Utilization` and `Number of Replicas` respectively. The `capacityCoefficient` is dynamically adjusted according to the actual disk usage. When the overall disk utilization of a BE is below 50%, the `capacityCoefficient` value is 0.5. When the disk utilization is above 75% (configurable via the FE configuration item `capacity_used_percent_high_water`), the value is 1. If the utilization is between 50% and 75%, the weight factor increases smoothly based on this formula:

`capacityCoefficient= 2 * disk utilization - 0.5`

This weighting factor ensures that when the disk usage is too high, the load score of this Backend will be higher, forcing to reduce the load on this BE as soon as possible.

TabletScheduler will update the CLS every 1 minute.

### Balancing Policy

TabletScheduler selects a certain number of healthy tablets as candidates for balancing through LoadBalancer in each scheduling round and adjust future scheduling based on these candidate slices.

## Resource Control

Both replica repair and balancing are done by copying across BEs. Too many tasks simultaneously performed on the same BE lead to an increase in IO pressure. Therefore, StarRocks controls the number of tasks that can be executed on each node during scheduling. The smallest unit of resource control is the disk (i.e., a data path specified in `be.conf`). By default, two slots for each disk are allocated for replica repair. A clone task occupies one slot on the source and one slot on the destination. If the disk has zero slots, no more tasks will be assigned to it. This number of slots can be configured with the `schedule_slot_num_per_path` parameter of FE.

In addition, by default, two slots for each disk are allocated for replica balancing. The purpose is to prevent heavily loaded nodes from being occupied by repair tasks and not being able to free up space through balancing.

## View replica status

The replica status is **only present in the leader FE node**. Therefore, the following commands need to be executed by connecting directly to the leader FE.

### View replica status

1. Global Status Check  
    The `SHOW PROC '/statistic';`command allows you to view the replica status of the entire cluster.

    ~~~plain text
        +----------+-----------------------------+----------+--------------+----------+-----------+------------+--------------------+-----------------------+
        | DbId     | DbName                      | TableNum | PartitionNum | IndexNum | TabletNum | ReplicaNum | UnhealthyTabletNum | InconsistentTabletNum |
        +----------+-----------------------------+----------+--------------+----------+-----------+------------+--------------------+-----------------------+
        | 35153636 | default_cluster:DF_Newrisk  | 3        | 3            | 3        | 96        | 288        | 0                  | 0                     |
        | 48297972 | default_cluster:PaperData   | 0        | 0            | 0        | 0         | 0          | 0                  | 0                     |
        | 5909381  | default_cluster:UM_TEST     | 7        | 7            | 10       | 320       | 960        | 1                  | 0                     |
        | Total    | 240                         | 10       | 10           | 13       | 416       | 1248       | 1                  | 0                     |
        +----------+-----------------------------+----------+--------------+----------+-----------+------------+--------------------+-----------------------+
    ~~~

    The `UnhealthyTabletNum` column shows how many unhealthy Tablets are in the corresponding Database. The `InconsistentTabletNum` column shows how many Tablets in the corresponding Database are in a replica inconsistent state. The last `Total` row gives the statistics of the entire cluster. Normally `UnhealthyTabletNum` and `InconsistentTabletNum` should be zero. If they are not zero, you can further check the tablet’s information. As shown above, there is one tablet in the `UM_TEST` database with unhealthy status, then you can use the following command to see which tablet it is.

    `SHOW PROC '/statistic/5909381';`  
    where `5909381` is the corresponding DbId.

    ~~~plain text
    +------------------+---------------------+
    | UnhealthyTablets | InconsistentTablets |
    +------------------+---------------------+
    | [40467980]       | []                  |
    +------------------+---------------------+
    ~~~

    The above result shows the specific unhealthy Tablet ID (40467980). Next, we will describe how to check the status of each replica of a specific Tablet.

2. Table (Partition) Level Status Check  
    Users can check the status of replicas of a specific table or partition with the following command and can filter the status by the WHERE statement. For example, to view the status of NORMAL replicas of `p1` and `p2` in `tbl1`.  
    `ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2) WHERE STATUS = "NORMAL";`

    ~~~plain text
    +----------+-----------+-----------+---------+-------------------+--------------------+------------------+------------+------------+-------+--------+--------+
    | TabletId | ReplicaId | BackendId | Version | LastFailedVersion | LastSuccessVersion | CommittedVersion | SchemaHash | VersionNum | IsBad | State  | Status |
    +----------+-----------+-----------+---------+-------------------+--------------------+------------------+------------+------------+-------+--------+--------+
    | 29502429 | 29502432  | 10006     | 2       | -1                | 2                  | 1                | -1         | 2          | false | NORMAL | OK     |
    | 29502429 | 36885996  | 10002     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
    | 29502429 | 48100551  | 10007     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
    | 29502433 | 29502434  | 10001     | 2       | -1                | 2                  | 1                | -1         | 2          | false | NORMAL | OK     |
    | 29502433 | 44900737  | 10004     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
    | 29502433 | 48369135  | 10006     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
    +----------+-----------+-----------+---------+-------------------+--------------------+------------------+------------+------------+-------+--------+--------+
    ~~~

    If `IsBad = true`, that means that the replica is corrupted. The `Status` column shows additional information.
    The `ADMIN SHOW REPLICA STATUS` command is mainly used to view the health status of a replica. Users can also view additional information with the following command.  

    `SHOW TABLET FROM tbl1;`

    ~~~plain text
    +----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+----------------------+--------------+----------------------+----------------------+----------------------+
    | TabletId | ReplicaId | BackendId | SchemaHash | Version | VersionHash | LstSuccessVersion | LstSuccessVersionHash | LstFailedVersion | LstFailedVersionHash | LstFailedTime | DataSize | RowCount | State  | LstConsistencyCheckTime | CheckVersion |     CheckVersionHash | VersionCount | PathHash             | MetaUrl              | CompactionStatus     |
    +----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+----------------------+--------------+----------------------+----------------------+----------------------+
    | 29502429 | 29502432  | 10006     | 1421156361 | 2       | 0           | 2                 | 0                     | -1               | 0                    | N/A           | 784      | 0        | NORMAL | N/A                     | -1           |     -1               | 2            | -5822326203532286804 | url                  | url                  |
    | 29502429 | 36885996  | 10002     | 1421156361 | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | 784      | 0        | NORMAL | N/A                     | -1           |     -1               | 2            | -1441285706148429853 | url                  | url                  |
    | 29502429 | 48100551  | 10007     | 1421156361 | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | 784      | 0        | NORMAL | N/A                     | -1           |     -1               | 2            | -4784691547051455525 | url                  | url                  |
    +----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+----------------------+--------------+----------------------+----------------------+----------------------+
    ~~~

Additional information includes replica size, number of rows, number of versions, data path where it is located, etc.

> Note: The `State` column does not represent the health status of the replica itself, but the status of the replica under certain tasks (e.g., `CLONE`, `SCHEMA_CHANGE`, `ROLLUP`, etc.).

In addition, users can check whether the replicas are evenly distributed with the following command  
`ADMIN SHOW REPLICA DISTRIBUTION FROM tbl1;`

~~~plain text
    +-----------+------------+-------+---------+
    | BackendId | ReplicaNum | Graph | Percent |
    +-----------+------------+-------+---------+
    | 10000     | 7          |       | 7.29 %  |
    | 10001     | 9          |       | 9.38 %  |
    | 10002     | 7          |       | 7.29 %  |
    | 10003     | 7          |       | 7.29 %  |
    | 10004     | 9          |       | 9.38 %  |
    | 10005     | 11         | >     | 11.46 % |
    | 10006     | 18         | >     | 18.75 % |
    | 10007     | 15         | >     | 15.62 % |
    | 10008     | 13         | >     | 13.54 % |
    +-----------+------------+-------+---------+
    ~~~

Information includes the number of replicas on each BE node, percentage, and simple graphical display is shown above.

Tablet Level Status Check  

Users can use the following command to check the status of a specific Tablet. For example, to check the status of the tablet with ID 29502553.  

`SHOW TABLET 29502553;`

~~~plain text
    +------------------------+-----------+---------------+-----------+----------+----------+-------------+----------+--------+---------------------------------------------------------------------------+
    | DbName                 | TableName | PartitionName | IndexName | DbId     | TableId  | PartitionId | IndexId  | IsSync | DetailCmd                                                                 |
    +------------------------+-----------+---------------+-----------+----------+----------+-------------+----------+--------+---------------------------------------------------------------------------+
    | default_cluster:test   | test      | test          | test      | 29502391 | 29502428 | 29502427    | 29502428 | true   | SHOW PROC '/dbs/29502391/29502428/partitions/29502427/29502428/29502553'; |
    +------------------------+-----------+---------------+-----------+----------+----------+-------------+----------+--------+---------------------------------------------------------------------------+
    ~~~

Above shows the database, table, partition, index and other information corresponding to the tablet. Users can copy the command in `DetailCmd` to check out the details 
    `SHOW PROC '/dbs/29502391/29502428/partitions/29502427/29502428/29502553';`

    ~~~plain text
    +-----------+-----------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+----------+------------------+
    | ReplicaId | BackendId | Version | VersionHash | LstSuccessVersion | LstSuccessVersionHash | LstFailedVersion | LstFailedVersionHash | LstFailedTime | SchemaHash | DataSize | RowCount | State  | IsBad | VersionCount | PathHash             | MetaUrl  | CompactionStatus |
    +-----------+-----------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+----------+------------------+
    | 43734060  | 10004     | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | -1         | 784      | 0        | NORMAL | false | 2            | -8566523878520798656 | url      | url              |
    | 29502555  | 10002     | 2       | 0           | 2                 | 0                     | -1               | 0                    | N/A           | -1         | 784      | 0        | NORMAL | false | 2            | 1885826196444191611  | url      | url              |
    | 39279319  | 10007     | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | -1         | 784      | 0        | NORMAL | false | 2            | 1656508631294397870  | url      | url              |
    +-----------+-----------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+----------+------------------+
~~~

Above shows all replicas of the corresponding Tablet. The content shown here is the same as `SHOW TABLET FROM tbl1;`. But here you can clearly see the status of all replicas of a specific Tablet.

### Scheduling tasks for replicas

1. View the tasks that are waiting to be scheduled by `SHOW PROC '/cluster_balance/pending_tablets';`

    ~~~plain text
    +----------+--------+-----------------+---------+----------+----------+-------+---------+--------+----------+---------+---------------------+---------------------+---------------------+----------+------+-------------+---------------+---------------------+------------+---------------------+--------+---------------------+-------------------------------+
    | TabletId | Type   | Status          | State   | OrigPrio | DynmPrio | SrcBe | SrcPath | DestBe | DestPath | Timeout | Create              | LstSched            | LstVisit            | Finished | Rate | FailedSched | FailedRunning | LstAdjPrio          | VisibleVer | VisibleVerHash      | CmtVer | CmtVerHash          | ErrMsg                        |
    +----------+--------+-----------------+---------+----------+----------+-------+---------+--------+----------+---------+---------------------+---------------------+---------------------+----------+------+-------------+---------------+---------------------+------------+---------------------+--------+---------------------+-------------------------------+
    | 4203036  | REPAIR | REPLICA_MISSING | PENDING | HIGH     | LOW      | -1    | -1      | -1     | -1       | 0       | 2019-02-21 15:00:20 | 2019-02-24 11:18:41 | 2019-02-24 11:18:41 | N/A      | N/A  | 2           | 0             | 2019-02-21 15:00:43 | 1          | 0                   | 2      | 0                   | unable to find source replica |
    +----------+--------+-----------------+---------+----------+----------+-------+---------+--------+----------+---------+---------------------+---------------------+---------------------+----------+------+-------------+---------------+---------------------+------------+---------------------+--------+---------------------+-------------------------------+
    ~~~

    A breakdown of all the properties is as follows.

    * TabletId: The ID of the Tablet that is waiting for scheduling. A task for each Tablet
    * Type: The type of task, either REPAIR or BALANCE
    * Status: The current status of the Tablet, such as `REPLICA_MISSING`
    * State: the state of this scheduling task, may be `PENDING`/`RUNNING`/`FINISHED`/`CANCELLED`/`TIMEOUT`/`UNEXPECTED`
    * OrigPrio: The initial priority
    * DynmPrio: The dynamic priority
    * SrcBe: The ID of the source BE node
    * SrcPath: The path of the source BE node
    * DestBe: The ID of the destination BE node
    * DestPath: The path of the destination BE node
    * Timeout: When the task is scheduled successfully, the timeout of the task is shown here in seconds
    * Create: The time when the task was created
    * LstSched: The time the task was last scheduled
    * LstVisit: The time the task was last accessed. Here " accessed " refers to scheduling, task execution reporting, etc.
    * Finished: The time when the task was finished.
    * Rate: The data copy rate of the clone task
    * FailedSched: The number of times the task scheduling failed
    * FailedRunning: The number of times the task failed to execute
    * LstAdjPrio: The time the task priority was last adjusted
    * CmtVer/CmtVerHash/VisibleVer/VisibleVerHash: Version information used to execute the clone task
    * ErrMsg: The error message when the task is scheduled and run

2. View the running tasks  
    `SHOW PROC '/cluster_balance/running_tablets';`  
    Same properties are used here. See `pending tablets` for details.
3. View the finished tasks  
    `SHOW PROC '/cluster_balance/history_tablets';`  
    By default, we only keep the last 1000 completed tasks. Same properties are used here. See `pending tablets` for details. If the `State` is `FINISHED`, that means the task is completed properly. Otherwise, check `ErrMsg` for the reason of a failed task.
