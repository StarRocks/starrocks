---
displayed_sidebar: docs
---

# CANCEL DECOMMISSION

## 説明

このステートメントは、ノードのデコミッションを取り消すために使用されます。（管理者のみ！）

構文:

```sql
CANCEL DECOMMISSION BACKEND "<host>:<heartbeat_port>"[,"<host>:<heartbeat_port>"...]
```

## 例

1. 2つのノードのデコミッションをキャンセルします。

    ```sql
    CANCEL DECOMMISSION BACKEND "host1:port", "host2:port";
    ```