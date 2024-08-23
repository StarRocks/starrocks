---
displayed_sidebar: docs
---

# SHOW REPOSITORIES

## 功能

查看当前已创建的仓库。

## 语法

```SQL
SHOW REPOSITORIES
```

## 返回

| **返回**   | **说明**                |
| ---------- | ----------------------- |
| RepoId     | 仓库 ID。               |
| RepoName   | 仓库名。                |
| CreateTime | 仓库创建时间。          |
| IsReadOnly | 是否为只读仓库。        |
| Location   | 远端存储系统路径。      |
| Broker     | 用于创建仓库的 Broker。 |
| ErrMsg     | 仓库连接错误信息。      |

## 示例

示例一：查看已创建的仓库。

```SQL
SHOW REPOSITORIES;
```
