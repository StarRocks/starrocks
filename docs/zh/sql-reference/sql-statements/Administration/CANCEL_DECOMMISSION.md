---
displayed_sidebar: "Chinese"
---

# CANCEL DECOMMISSION

## 功能

该语句用于撤销一个节点下线操作。

> **注意**
>
> 仅管理员使用！

## 语法

```sql
CANCEL DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...]
```

## 示例

取消两个节点的下线操作:

```sql
CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";
```
