Runtime Disk Management

# Runtime Disk Management

This topic introduces the Runtime Disk Management feature of the StarRocks Enterprise Edition.

## Overview

From v3.2.0 onwards, the StarRocks Enterprise Edition supports local disk management in a BE node without stopping the node first. 

Runtime Disk Management can serve the following purposes:

- **Disable a bad disk**
  -  Bad disks or corrupted sectors may cause loading tasks to fail or even the BE node to crash. You can disable the bad disk to prevent the disk from being read or written. The lost replicas will be rebuilt with a full clone operation on another BE node.
- **Decommission a disk**
  -  If you want to replace a healthy disk to achieve a larger storage size or better performance, you can decommission the disk before disabling it. When you decommission a disk, the data on it will be migrated to other disks on the BE node first. Only after the data is successfully migrated can you safely disable and remove the disk, thus without losing a data replica.

## Disable a disk

Before you disable a disk, you must set the BE dynamic configuration `max_percentage_of_error_disk` to `100`.

```Bash
curl -XPOST http://<be_host>:<be_http_port>/api/update_config?max_percentage_of_error_disk=100
```

- `be_host`: The IP address or FQDN of the BE node on which you want to disable the disk.
- `be_http_port`: The HTTP port of the BE node on which you want to disable the disk.

Use the following syntax to disable a disk:

```SQL
ALTER SYSTEM DISABLE DISK '<disk_path>' [, ...]
ON BACKEND "<be_host>:<heartbeat_service_port>"
```

- `disk_path`: The path of the disk you want to disable. You need to wrap each path with double quotation marks. If you need to specify multiple paths, separate the paths with commas (,).
- `be_host`: The IP address or FQDN of the BE node on which you want to disable the disk.
- `heartbeat_service_port`: The heartbeat service port of the BE node on which you want to disable the disk.

> **NOTE**
>
> - Once a disk is disabled, the data replica on the disk is no longer available. StarRocks will repair the lost replica by performing a full clone operation on other available BE nodes. After the statement returns success, you can replace or fix the disabled disk.
> - Once a disk is disabled, the disk itself is also unavailable. If you want to add the disk back to the cluster after repairing it, you need to assign a new disk path to the disk in the item `storage_root_path` of the BE configuration file **be.conf**, and delete the old path.

Example:

```SQL
ALTER SYSTEM DISABLE DISK "/disk1"
ON BACKEND "xxx.xx.xx.xxx:9050";
```

## Decommission a disk

Use the following syntax to decommission a disk:

```SQL
ALTER SYSTEM DECOMMISSION DISK "<disk_path>" [, ...]
ON BACKEND "<be_host>:<heartbeat_service_port>"
```

- `disk_path`: The path of the disk you want to decommission. You need to wrap each path with double quotation marks. If you need to specify multiple paths, separate the paths with commas (,).
- `be_host`: The IP address or FQDN of the BE node on which you want to decommission the disk.
- `heartbeat_service_port`: The heartbeat service port of the BE node on which you want to decommission the disk.

Example:

```SQL
ALTER SYSTEM DECOMMISSION DISK "/disk1"
ON BACKEND "xxx.xx.xx.xxx:9050";
```

After you execute the statement to decommission a disk, StarRocks migrates the data replica on the disk to other available disks of the node. Data migration is an asynchronous operation. You can check the progress of the migration using the following syntax:

```SQL
SHOW PROC '/backends/<be_id>'
```

`be_id`: The ID of the BE node you decommissioned.

Output:

```Plain
| RootPath                       | DataUsedCapacity | OtherUsedCapacity | AvailCapacity | TotalCapacity | TotalUsedPct | State  | PathHash             | StorageMedium | TabletNum | DataTotalCapacity | DataUsedPct |
+--------------------------------+------------------+-------------------+---------------+---------------+--------------+--------+----------------------+---------------+-----------+-------------------+-------------+
| /home/disk1/gengjun/be/storage | 5.901 KB         | 1.442 TB          | 539.267 GB    | 1.968 TB      | 73.25 %      | ONLINE | -7433622749810768808 | HDD           | 219       | 539.267 GB        | 0.00 %      |
```

When the `TabletNum` field returns `0`, the migration succeeds.

As long as the migration task succeeds, you can then disable the disk and replace it safely.

## Cancel the decommissioned state of a disk

Use the following syntax to cancel the decommissioned state of a disk:

```SQL
ALTER SYSTEM CANCEL DECOMMISSION DISK "<disk_path>" [, ...]
ON BACKEND "<be_host>:<heartbeat_service_port>"
```

- `disk_path`: The path of the disk you want to cancel the decommissioned state. You need to wrap each path with double quotation marks. If you need to specify multiple paths, separate the paths with commas (,).
- `be_host`: The IP address or FQDN of the BE node on which you want to cancel the decommissioned state of the disk.
- `heartbeat_service_port`: The heartbeat service port of the BE node on which you want to cancel the decommissioned state of the disk.

> **NOTE**
>
> Once the decommissioned state is canceled, StarRocks automatically balances the data distribution across disks.

Example:

```SQL
ALTER SYSTEM CANCEL DECOMMISSION DISK "/disk1"
ON BACKEND "xxx.xx.xx.xxx:9050";
```

## Usage notes

- Currently, the Runtime Disk Management feature does not support adding new disks to a BE node at runtime. To add a new disk to a BE node, you must stop the node first, add the disk, specify the disk path in the BE configuration item `storage_root_path` in the BE configuration file **be.conf**, and restart the node.
- After you have disabled or decommissioned a disk in your StarRocks cluster, if you downgrade the cluster to v3.1 or earlier and then upgrade it back to v3.2, the disk will be set enabled again. 