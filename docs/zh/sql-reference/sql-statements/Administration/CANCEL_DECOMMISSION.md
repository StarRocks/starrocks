---
displayed_sidebar: "Chinese"
---

# CANCEL DECOMMISSION

## 功能

该语句用于撤销一个节点下线操作。

> **注意**
>
> 只有拥有 `cluster_admin` 角色的用户才可以执行集群管理相关操作。

## 语法

```sql
CANCEL DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...]
```

## 示例

取消两个节点的下线操作:

```sql
CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";
```
