# SHOW COMPUTE NODES

## 功能

该语句用于查看集群内的 CN 节点。

## 语法

```sql
SHOW COMPUTE NODES
```

命令返回结果说明：

1. **LastStartTime**：表示最近一次 CN 节点启动时间。
2. **LastHeartbeat**：表示最近一次 CN 节点心跳。
3. **Alive**：表示 CN 节点是否存活。
4. **SystemDecommissioned** 为 true 表示 CN 节点正在安全下线中。
5. **ErrMsg**： 用于显示心跳失败时的错误信息。
6. **Status**： 用于以 JSON 格式显示 CN 节点的一些状态信息， 目前包括最后一次 CN 节点汇报其状态的时间信息。
