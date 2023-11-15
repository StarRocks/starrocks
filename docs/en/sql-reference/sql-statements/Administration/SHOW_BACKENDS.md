# SHOW BACKENDS

## description

This statement is used to view BE node om tje cluster syntax:
Syntax:

```sql
SHOW BACKENDS;
```

Note:

1. LastStartTime indicates the latest BE start-up time.
2. LastHeartbeat indicates the latest heartbeat.
3. Alive indicates whether the node survives.
4. SystemDecommissioned being true means the node is safely offline.
5. ClusterDecommissioned being true means the node is rushing offline in the current cluster.  
6. TabletNum represents the number of shardings in the node.
7. DataUsedCapacity represents the space occupied by the actual user data.
8. AvailCapacity means the available space in the disk.
9. TotalCapacity means the total space in the disk. TotalCapacity = AvailCapacity + DataUsedCapacity + other space occupied by non-userdata files
10. UsedPct represents the percentage of space used in the disk.
11. ErrMsg is used to display errors when a heartbeat fails.
12. Status is used to display BE status in JSON format. It currently displays when it is that BE reports its tablet last time.

## keyword

SHOW, BACKENDS
