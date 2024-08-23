---
displayed_sidebar: docs
---

# DROP REPOSITORY

## Description

Deletes a repository. Repositories are used to store data snapshots for data backup and restoration.

> **CAUTION**
>
> - Only root user and superuser can delete a repository.
> - This operation only deletes the mapping of the repository in StarRocks, and does not delete the actual data. You need to delete it manually in the remote storage system. After deletion, you can map to that repository again by specifying the same remote storage system path.

## Syntax

```SQL
DROP REPOSITORY <repository_name>
```

## Parameters

| **Parameter**   | **Description**                       |
| --------------- | ------------------------------------- |
| repository_name | Name of the repository to be deleted. |

## Example

Example 1: deletes a repository named `oss_repo`.

```SQL
DROP REPOSITORY `oss_repo`;
```
