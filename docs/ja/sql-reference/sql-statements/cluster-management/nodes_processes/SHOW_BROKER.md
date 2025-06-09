---
displayed_sidebar: docs
---

# SHOW BROKER

## 説明

このステートメントは、現在存在する broker を表示するために使用されます。

:::tip

SYSTEM レベルの OPERATE 権限を持つユーザーまたは `cluster_admin` ロールを持つユーザーのみがこの操作を実行できます。

:::

## 構文

```sql
SHOW BROKER
```

注意:

1. LastStartTime は最新の BE の起動時間を表します。
2. LastHeartbeat は最新のハートビートを表します。
3. Alive はノードが生存しているかどうかを示します。
4. ErrMsg はハートビートが失敗したときにエラーメッセージを表示するために使用されます。