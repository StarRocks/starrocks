# SHOW BACKENDS

## 功能

该语句用于查看 cluster 内的 BE 节点。

## 语法

```sql
SHOW BACKENDS;
```

命令返回结果说明：

1. **LastStartTime** 表示最近一次 BE 启动时间。
2. **LastHeartbeat** 表示最近一次心跳。
3. **Alive** 表示节点是否存活。
4. **SystemDecommissioned** 为 true 表示节点正在安全下线中。
5. **ClusterDecommissioned** 为 true 表示节点正在当前 cluster 中下线。
6. **TabletNum** 表示该节点上分片数量。
7. **DataUsedCapacity** 表示实际用户数据所占用的空间。
8. **AvailCapacity** 表示磁盘的可使用空间。
9. **TotalCapacity** 表示总磁盘空间。`TotalCapacity = AvailCapacity + DataUsedCapacity + 其他非用户数据文件占用空间`。
10. **UsedPct** 表示磁盘已使用量百分比。
11. **ErrMsg** 用于显示心跳失败时的错误信息。
12. **Status** 用于以 JSON 格式显示 BE 的一些状态信息, 目前包括最后一次 BE 汇报其 tablet 的时间信息。

## 关键字(keywords)

SHOW， BACKENDS
