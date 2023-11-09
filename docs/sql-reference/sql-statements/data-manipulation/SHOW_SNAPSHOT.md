---
displayed_sidebar: "English"
---

# SHOW SNAPSHOT

## Description

Views data snapshots in a specified repository. For more information, see [data backup and restoration](../../../administration/Backup_and_restore.md).

## Syntax

```SQL
SHOW SNAPSHOT ON <repo_name>
[WHERE SNAPSHOT = <snapshot_name> [AND TIMESTAMP = <backup_timestamp>]]
```

## Parameters

| **Parameter**    | **Description**                                      |
| ---------------- | ---------------------------------------------------- |
| repo_name        | Name of the repository that the snapshot belongs to. |
| snapshot_nam     | Name of the snapshot.                                |
| backup_timestamp | Backup timestamp of the snapshot.                    |

## Return

| **Return** | **Description**                                              |
| ---------- | ------------------------------------------------------------ |
| Snapshot   | Name of the snapshot.                                        |
| Timestamp  | Backup timestamp of the snapshot.                            |
| Status     | Displays `OK` if the snapshot is okay. Displays error message if the snapshot is not okay. |
| Database   | Name of the database that the snapshot belongs to.           |
| Details    | JSON-formatted directory and structure of the snapshot.      |

## Example

Example 1: Views snapshots in repository `example_repo`.

```SQL
SHOW SNAPSHOT ON example_repo;
```

Example 2: Views the snapshot named `backup1` in repository `example_repo`.

```SQL
SHOW SNAPSHOT ON example_repo
WHERE SNAPSHOT = "backup1";
```

Example 3: Views the snapshot named `backup1` with backup timestamp `2018-05-05-15-34-26` in repository `example_repo`.

```SQL
SHOW SNAPSHOT ON example_repo 
WHERE SNAPSHOT = "backup1" AND TIMESTAMP = "2018-05-05-15-34-26";
```
