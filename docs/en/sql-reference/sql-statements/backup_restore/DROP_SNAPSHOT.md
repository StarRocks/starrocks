---
displayed_sidebar: docs
description: "Deletes a data snapshot and its data files from a repository."
---

# DROP SNAPSHOT

Deletes a data snapshot from a repository, together with the data files it holds in the remote storage system. Unlike [DROP REPOSITORY](./DROP_REPOSITORY.md), which only removes the mapping in StarRocks, this statement removes the backed-up data itself.

The deletion is irreversible. Once a snapshot is dropped, it can no longer be restored with [RESTORE](./RESTORE.md).

This statement is supported from v4.2.0 onwards.

:::caution

- The snapshot cannot be dropped while a BACKUP job is writing it or a RESTORE job is reading it.
- The snapshot cannot be dropped if its repository is read-only.

:::

## Privilege requirement

Users must have the REPOSITORY privilege at the System level.

```SQL
GRANT REPOSITORY ON SYSTEM TO ROLE <role_name>;
```

## Syntax

```SQL
DROP SNAPSHOT <snapshot_name> ON <repository_name> [FORCE]
```

## Parameters

| **Parameter**   | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| snapshot_name   | Name of the snapshot to be deleted.                          |
| repository_name | Name of the repository that the snapshot belongs to.         |
| FORCE           | Deletes the snapshot without checking which cluster created it. See below. |

## Snapshot ownership

Each snapshot records the ID of the cluster that created it. Without `FORCE`, StarRocks deletes the snapshot only when that ID matches the current cluster, which you can read from the `ClusterId` column of [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md) and the `ClusterId` column of [SHOW FRONTENDS](../cluster-management/nodes_processes/SHOW_FRONTENDS.md).

The statement is rejected when the snapshot:

- was created by another cluster sharing the same repository,
- was created before v4.2.0 and therefore records no cluster ID, or
- has no readable snapshot metadata, which is the case while a backup is still running and after a backup was interrupted.

Use `FORCE` to delete such a snapshot anyway. Deleting a snapshot that this cluster did not create removes data that another cluster may still depend on, so confirm that the data is yours to remove before using it.

## Examples

Example 1: Deletes the snapshot `backup1` from repository `example_repo`.

```SQL
DROP SNAPSHOT backup1 ON example_repo;
```

Example 2: Deletes the snapshot `legacy_backup` from repository `example_repo` even though it was not created by this cluster.

```SQL
DROP SNAPSHOT legacy_backup ON example_repo FORCE;
```

## References

- [BACKUP](./BACKUP.md)
- [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md)
- [RESTORE](./RESTORE.md)
