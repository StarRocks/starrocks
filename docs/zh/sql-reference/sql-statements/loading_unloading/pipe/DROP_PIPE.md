---
displayed_sidebar: docs
---

# DROP PIPE

## 功能

删除 Pipe，并删除相关的作业和元数据。该操作不会删除已经导入的数据。该命令自 3.2 版本起支持。

## 语法

```SQL
DROP PIPE [IF EXISTS] [db_name.]<pipe_name>
```

## 参数说明

### db_name

Pipe 所属的数据库的名称。

### pipe_name

Pipe 的名称。

## 示例

删除数据库 `mydatabase` 下名为 `user_behavior_replica` 的 Pipe：

```SQL
USE mydatabase;
DROP PIPE user_behavior_replica;
```

## 相关文档

- [CREATE PIPE](CREATE_PIPE.md)
- [ALTER PIPE](ALTER_PIPE.md)
- [SHOW PIPES](SHOW_PIPES.md)
- [SUSPEND or RESUME PIPE](SUSPEND_or_RESUME_PIPE.md)
- [RETRY FILE](RETRY_FILE.md)
