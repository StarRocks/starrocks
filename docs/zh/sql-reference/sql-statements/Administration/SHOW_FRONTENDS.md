---
displayed_sidebar: "Chinese"
---

# SHOW FRONTENDS

## 功能

该语句用于查看 FE 节点。FE 节点添加方式及高可用部署请参考 [集群部署](../../../administration/Deployment.md#部署-fe-高可用集群) 章节。

> **注意**
>
> 该操作需要 SYSTEM 级 OPERATE 权限或 cluster_admin 角色。

## 语法

```sql
SHOW FRONTENDS
```

命令返回结果说明：

1. name 表示该 FE 节点在 bdbje 中的名称。
2. Join 为 true 表示该节点曾经加入过集群。但不代表当前还在集群内（可能已失联）
3. Alive 表示节点是否存活。
4. ReplayedJournalId 表示该节点当前已经回放的最大元数据日志 id。
5. LastHeartbeat 是最近一次心跳。
6. IsHelper 表示该节点是否是 bdbje 中的 helper 节点。
7. ErrMsg 用于显示心跳失败时的错误信息。
