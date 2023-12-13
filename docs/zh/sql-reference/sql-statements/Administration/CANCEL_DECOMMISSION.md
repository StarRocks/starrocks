---
displayed_sidebar: "Chinese"
---

# CANCEL DECOMMISSION

## description

该语句用于撤销一个节点下线操作。（仅管理员使用！）

语法：

```sql
CANCEL DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
```

## example

1. 取消两个节点的下线操作:

    ```sql
    CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";
    ```

## keyword

CANCEL,DECOMMISSION,BACKEND
