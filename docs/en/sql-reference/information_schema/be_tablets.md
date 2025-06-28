---
displayed_sidebar: docs
---

# be_tablets

`be_tablets` provides information about tablets on each BE node.

The following fields are provided in `be_tablets`:

| **Field**     | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| BE_ID         | ID of the BE node.                                           |
| TABLE_ID      | ID of the table to which the tablet belongs.                 |
| PARTITION_ID  | ID of the partition to which the tablet belongs.             |
| TABLET_ID     | ID of the tablet.                                            |
| NUM_VERSION   | Number of versions in the tablet.                            |
| MAX_VERSION   | Maximum version of the tablet.                               |
| MIN_VERSION   | Minimum version of the tablet.                               |
| NUM_ROWSET    | Number of rowsets in the tablet.                             |
| NUM_ROW       | Number of rows in the tablet.                                |
| DATA_SIZE     | Data size of the tablet (in bytes).                          |
| INDEX_MEM     | Index memory usage of the tablet (in bytes).                 |
| CREATE_TIME   | Creation time of the tablet (Unix timestamp in seconds).     |
| STATE         | State of the tablet (e.g., `NORMAL`, `REPLICA_MISSING`).     |
| TYPE          | Type of the tablet.                                          |
| DATA_DIR      | Data directory where the tablet is stored.                   |
| SHARD_ID      | Shard ID of the tablet.                                      |
| SCHEMA_HASH   | Schema hash of the tablet.                                   |
| INDEX_DISK    | Index disk usage of the tablet (in bytes).                   |
| MEDIUM_TYPE   | Medium type of the tablet (e.g., `HDD`, `SSD`).              |
| NUM_SEGMENT   | Number of segments in the tablet.                            |
