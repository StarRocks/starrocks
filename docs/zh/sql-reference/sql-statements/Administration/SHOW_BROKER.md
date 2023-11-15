# SHOW BROKER

## 功能

该语句用于查看当前存在的 broker。

## 语法

```sql
SHOW BROKER
```

命令返回结果说明：

1. LastStartTime 表示最近一次 BE 启动时间。
2. LastHeartbeat 表示最近一次心跳。
3. Alive 表示节点是否存活。
4. ErrMsg 用于显示心跳失败时的错误信息。
