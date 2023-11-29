---
displayed_sidebar: "English"
---

# SHOW REPOSITORIES

## Description

Views the repositories created in StarRocks.

## Syntax

```SQL
SHOW REPOSITORIES
```

## Return

| **Return** | **Description**                                          |
| ---------- | -------------------------------------------------------- |
| RepoId     | Repository ID.                                           |
| RepoName   | Repository name.                                         |
| CreateTime | Repository creation time.                                |
| IsReadOnly | If the repository is read-only.                          |
| Location   | Location of the repository in the remote storage system. |
| Broker     | Broker used to create the repository.                    |
| ErrMsg     | Error message during connection to the repository.       |

## Examples

Example 1: Views the repositories created in StarRocks.

```SQL
SHOW REPOSITORIES;
```
