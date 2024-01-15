---
displayed_sidebar: "Chinese"
---

# SHOW PIPES

## 功能

查看当前数据库或指定数据库下 Pipe。该命令从 3.2 版本开始支持。

## 语法

```SQL
SHOW PIPES [FROM <db_name>]
[
   WHERE [ NAME { = "<pipe_name>" | LIKE "pipe_matcher" } ]
         [ [AND] STATE = { "SUSPENDED" | "RUNNING" | "ERROR" } ]
]
[ ORDER BY <field_name> [ ASC | DESC ] ]
[ LIMIT { [offset, ] limit | limit OFFSET offset } ]
```

## 参数说明

### FROM `<db_name>`

指定待查询数据库的名称。如果不指定数据库，则默认查询当前数据库下的 Pipe。

### WHERE

指定查询条件。

### ORDER BY `<field_name>`

指定结果记录的排序字段。

### LIMIT

指定系统返回的最大结果记录数。

## 返回结果

返回结果包括如下字段：

| **字段**      | **说明**                                                     |
| ------------- | ------------------------------------------------------------ |
| DATABASE_NAME | Pipe 所属数据库的名称。                                      |
| ID            | Pipe 的唯一 ID。                                             |
| NAME          | Pipe 的名称。                                                |
| TABLE_NAME    | StarRocks 目标表的名称。                                     |
| STATE         | Pipe 的状态，包括 `RUNNING`、`FINISHED`、`SUSPENDED`、`ERROR`。 |
| LOAD_STATUS   | Pipe 下待导入数据文件的整体状态，包括如下字段：<ul><li>`loadedFiles`：已导入的数据文件总个数。</li><li>`loadedBytes`：已导入的数据总量，单位为字节。</li><li>`LastLoadedTime`：最近一次执行导入的时间。格式：`yyyy-MM-dd HH:mm:ss`。例如，`2023-07-24 14:58:58`。</li></ul> |
| LAST_ERROR    | Pipe 执行过程中最近一次错误的详细信息。默认值为 `NULL`。     |
| CREATED_TIME  | Pipe 的创建时间。格式：`yyyy-MM-dd HH:mm:ss`。例如：`2023-07-24 14:58:58`。 |

## 示例

### 查询所有 Pipe

进入数据库 `mydatabase`，然后查看该数据库下所有 Pipe：

```SQL
USE mydatabase;
SHOW PIPES \G
```

### 查询指定 Pipe

进入数据库 `mydatabase`，然后查看该数据库下名为 `user_behavior_replica` 的 Pipe：

```SQL
USE mydatabase;
SHOW PIPES WHERE NAME = 'user_behavior_replica' \G
```
