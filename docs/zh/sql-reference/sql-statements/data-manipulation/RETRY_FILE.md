# RETRY FILE

## 功能

重新尝试导入指定 Pipe 中所有数据文件或某个数据文件。

## 语法

```SQL
ALTER PIPE [ IF EXISTS ] <pipe_name> { RETRY ALL | RETRY FILE '<file_name>' }
```

## 参数说明

### pipe_name

Pipe 的名称。

### file_name

要重试的数据文件的存储路径。注意这里需要指定完整的路径。如果指定的文件不属于当前指定的 Pipe，则返回报错。

## 示例

重试导入名为 `user_behavior_replica` 的 Pipe 中所有数据文件：

```SQL
ALTER PIPE [ IF EXISTS ] user_behavior_replica RETRY ALL;
```

重试导入名为 `user_behavior_replica` 的 Pipe 中的数据文件 `s3://starrocks-datasets/user_behavior_ten_million_rows.parquet`：

```SQL
ALTER PIPE [ IF EXISTS ] user_behavior_replica RETRY FILE 's3://starrocks-datasets/user_behavior_ten_million_rows.parquet';
```
