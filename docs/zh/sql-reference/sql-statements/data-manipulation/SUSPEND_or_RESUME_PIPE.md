# SUSPEND or RESUME PIPE

## 功能

暂停或重新启动 Pipe：

- 当导入作业正在进行时（即，处于 `RUNNING` 状态），暂停 (`SUSPEND`) Pipe 会中断正在执行的作业。
- 当出现导入错误时，重新启动 (`RESUME`) Pipe 会继续执行出错的作业。

## 语法

```SQL
ALTER PIPE [ IF EXISTS ] [ <pipe_name> ] { SUSPEND | RESUME [ IF SUSPENDED ] }
```

## 参数说明

### pipe_name

Pipe 的名称。

## 示例

### 暂停 Pipe

暂停数据库 `mydatabase` 下名为 `user_behavior_replica` 的 Pipe（Pipe 当前处于 `RUNNING` 状态）：

```SQL
USE mydatabase;
ALTER PIPE user_behavior_replica SUSPEND;
```

通过 [SHOW PIPES](../../../sql-reference/sql-statements/data-manipulation/SHOW_PIPES.md) 查看该 Pipe，可以看到 Pipe 的状态变为 `SUSPEND`。

### 重新启动 Pipe

重新启动数据库 `mydatabase` 下名为 `user_behavior_replica` 的 Pipe：

```SQL
USE mydatabase;
ALTER PIPE user_behavior_replica RESUME;
```

通过 [SHOW PIPES](../../../sql-reference/sql-statements/data-manipulation/SHOW_PIPES.md) 查看该 Pipe，可以看到 Pipe 的状态变为 `RUNNING`。
