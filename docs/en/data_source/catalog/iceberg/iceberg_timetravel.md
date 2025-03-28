---
displayed_sidebar: docs
---

# Time Travel with Iceberg Catalog

This topic introduces StarRocks' Time Travel feature for Iceberg catalogs. This feature is supported from v3.4.0 onwards.

## Overview

Each Iceberg table maintains a metadata snapshot log, which represents the changes applied to it. Databases can perform Time Travel queries against Iceberg tables by accessing these historical snapshots. Iceberg supports branching and tagging snapshots for sophisticated snapshot lifecycle management, allowing each branch or tag to maintain its own lifecycle based on customized retention policies. For more information on Iceberg's branching and tagging feature, see [Official Documentation](https://iceberg.apache.org/docs/latest/branching/).

By integrating Iceberg's snapshot branching and tagging feature, StarRocks supports creating and managing branches and tags in Iceberg catalogs, and Time Travel queries against tables within.

## Manage branches, tags, and snapshots

### Create a branch

**Syntax**

```SQL
ALTER TABLE [catalog.][database.]table_name
CREATE [OR REPLACE] BRANCH [IF NOT EXISTS] <branch_name>
[AS OF VERSION <snapshot_id>]
[RETAIN <int> { DAYS | HOURS | MINUTES }]
[WITH SNAPSHOT RETENTION 
    { minSnapshotsToKeep | maxSnapshotAge | minSnapshotsToKeep maxSnapshotAge }]

minSnapshotsToKeep ::= <int> SNAPSHOTS

maxSnapshotAge ::= <int> { DAYS | HOURS | MINUTES }
```

**Parameters**

- `branch_name`: Name of the branch to create.
- `AS OF VERSION`: ID of the snapshot (version) on which to create the branch. 
- `RETAIN`: Time to retain the branch. Format: `<int> <unit>`. Supported units: `DAYS`, `HOURS`, and `MINUTES`. Example: `7 DAYS`, `12 HOURS`, or `30 MINUTES`.
- `WITH SNAPSHOT RETENTION`: The minimum number of snapshots to keep and/or the maximum time to keep the snapshots.

**Example**

Create a branch `test-branch` based on version (snapshot ID) `12345` of the table `iceberg.sales.order`, retain the branch for `7` days, and keep at least `2` snapshots on the branch.

```SQL
ALTER TABLE iceberg.sales.order CREATE BRANCH `test-branch` 
AS OF VERSION 12345
RETAIN 7 DAYS
WITH SNAPSHOT RETENTION 2 SNAPSHOTS;
```

### Load data into a specific branch of a table

**Syntax**

```SQL
INSERT INTO [catalog.][database.]table_name
[FOR] VERSION AS OF <branch_name>
<query_statement>
```

**Parameters**

- `branch_name`: Name of the table branch into which the data is loaded.
- `query_statement`: Query statement whose result will be loaded into the destination table. It can be any SQL statement supported by StarRocks.

**Example**

Load the result of a query into the branch `test-branch` of the table `iceberg.sales.order`.

```SQL
INSERT INTO iceberg.sales.order
FOR VERSION AS OF `test-branch`
SELECT c1, k1 FROM tbl;
```

### Create a tag

**Syntax**

```SQL
ALTER TABLE [catalog.][database.]table_name
CREATE [OR REPLACE] TAG [IF NOT EXISTS] <tag_name>
[AS OF VERSION <snapshot_id>]
[RETAIN <int> { DAYS | HOURS | MINUTES }]
```

**Parameters**

- `tag_name`: Name of the tag to create.
- `AS OF VERSION`: ID of the snapshot (version) on which to create the tag. 
- `RETAIN`: Time to retain the tag. Format: `<int> <unit>`. Supported units: `DAYS`, `HOURS`, and `MINUTES`. Example: `7 DAYS`, `12 HOURS`, or `30 MINUTES`.

**Example**

Create a tag `test-tag` based on version (snapshot ID) `12345` of the table `iceberg.sales.order`, and retain the tag for `7` days.

```SQL
ALTER TABLE iceberg.sales.order CREATE TAG `test-tag` 
AS OF VERSION 12345
RETAIN 7 DAYS;
```

### Fast forward a branch to another

**Syntax**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE fast_forward('<from_branch>', '<to_branch>')
```

**Parameters**

- `from_branch`: The branch you want to fast forward. Wrap the branch name in quotes.
- `to_branch`: The branch to which you want to fast forwards the `from_branch`. Wrap the branch name in quotes.

**Example**

Fast forward the `main` branch to the branch `test-branch`.

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE fast_forward('main', 'test-branch');
```

### Cherry pick a snapshot 

You can cherry pick a specific snapshot and apply it to the current status of the table. This operation will create a new snapshot based on an existing snapshot, and the original snapshot will not be affected.

**Syntax**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE cherrypick_snapshot(<snapshot_id>)
```

**Parameter**

`snapshot_id`: ID of the snapshot which you want to cherry pick.

**Example**

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE cherrypick_snapshot(54321);
```

### Expire snapshots

You can expire snapshots earlier than a specific point of time. This operation will delete the data files of the expired snapshots.

**Syntax**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE expire_snapshot('<datetime>')
```

**Example**

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshot('2023-12-17 00:14:38')
```

### Drop a branch or a tag

**Syntax**

```SQL
ALTER TABLE [catalog.][database.]table_name
DROP { BRANCH <branch_name> | TAG <tag_name> }
```

**Exmaple**

```SQL
ALTER TABLE iceberg.sales.order
DROP BRANCH `test-branch`;

ALTER TABLE iceberg.sales.order
DROP TAG `test-tag`;
```

## Query with Time Travel

### Time Travel to a specific branch or tag

**Syntax**

```SQL
[FOR] VERSION AS OF '<branch_or_tag>'
```

**Parameter**

`tag_or_branch`: Name of the branch or tag to which you want to Time Travel. If a branch name is specified, the query will Time Travel to the head snapshot of the branch. If a tag name is specified, the query will Time Travel to the snapshot that the tag referenced.

**Example**

```SQL
-- Time Travel to the head snapshot of a branch.
SELECT * FROM iceberg.sales.order VERSION AS OF 'test-branch';
-- Time Travel to the snapshot that the tag referenced.
SELECT * FROM iceberg.sales.order VERSION AS OF 'test-tag';
```

### Time Travel to a specific snapshot

**Syntax**

```SQL
[FOR] VERSION AS OF '<snapshot_id>'
```

**Parameter**

`snapshot_id`: ID of the snapshot to which you want to Time Travel.

**Example**

```SQL
SELECT * FROM iceberg.sales.order VERSION AS OF 12345;
```

### Time Travel to a specific datetime or date

**Syntax**

```SQL
[FOR] TIMESTAMP AS OF { '<datetime>' | '<date>' | date_and_time_function }
```

**Parameter**

`date_and_time_function`: Any [date and time functions](../../../sql-reference/sql-functions/date-time-functions/now.md) supported by StarRocks.

**Example**

```SQL
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF '1986-10-26 01:21:00';
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF '1986-10-26';
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF now();
```
