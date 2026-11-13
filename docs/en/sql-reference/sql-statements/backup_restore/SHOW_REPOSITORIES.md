---
displayed_sidebar: docs
<<<<<<< HEAD
=======
products: [oss, enterprise, anywhere]
description: "Views the repositories created in StarRocks."
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SHOW REPOSITORIES

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
