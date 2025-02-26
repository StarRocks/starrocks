---
displayed_sidebar: docs
---

# SHOW FRONTENDS

## 説明

このステートメントは FE ノードを表示するために使用されます。

:::tip

この操作を実行できるのは、SYSTEM レベルの OPERATE 権限を持つユーザーまたは `cluster_admin` ロールを持つユーザーのみです。

:::

## 構文

```sql
SHOW FRONTENDS
```

注意:

1. Name は BDBJE 内の FE ノードの名前を表します。
2. ジョイン が true の場合、このノードはすでにクラスタに参加しています。しかし、ノードがクラスタ内に残っていることを意味するわけではなく、欠けている可能性があります。
3. Alive はノードが生存しているかどうかを示します。
4. ReplayedJournalId は、ノードが現在リプレイした最大のメタデータログ ID を表します。
5. LastHeartbeat は最新のハートビートです。
6. IsHelper は、ノードが BDBJE からのヘルパーノードであるかどうかを示します。
7. ErrMsg は、ハートビートが失敗したときにエラーメッセージを表示するために使用されます。