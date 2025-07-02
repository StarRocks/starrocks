---
keywords: ['xiugai'] 
displayed_sidebar: docs
---

# ALTER PIPE

## 功能

修改 Pipe 的执行参数。该命令自 3.2 版本起支持。

## 语法

```SQL
ALTER PIPE [db_name.]<pipe_name> 
SET
(
    "<key>" = <value>[, "<key>" = "<value>" ...]
) 
```

## 参数说明

### db_name

Pipe 所属的数据库的名称。

### pipe_name

Pipe 的名称。

### **PROPERTIES**

要修改的执行参数设置。格式：`"key" = "value"`。有关支持的执行参数，参见 [CREATE PIPE](CREATE_PIPE.md)。

## 示例

修改数据库 `mydatabase` 下名为 `user_behavior_replica` 的 Pipe 的 `AUTO_INGEST` 属性为 `FALSE`：

```SQL
USE mydatabase;
ALTER PIPE user_behavior_replica
SET
(
    "AUTO_INGEST" = "FALSE"
);
```

## 相关文档

- [CREATE PIPE](CREATE_PIPE.md)
- [DROP PIPE](DROP_PIPE.md)
- [SHOW PIPES](SHOW_PIPES.md)
- [SUSPEND or RESUME PIPE](SUSPEND_or_RESUME_PIPE.md)
- [RETRY FILE](RETRY_FILE.md)
