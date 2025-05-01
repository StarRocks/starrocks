---
displayed_sidebar: docs
---

# SHOW COMPUTE NODES

## Description

StarRocks クラスター内のすべてのコンピュートノードを表示します。

:::tip

この操作を実行できるのは、SYSTEM レベルの OPERATE 権限を持つユーザーまたは `cluster_admin` ロールを持つユーザーのみです。

:::

## Syntax

```SQL
SHOW COMPUTE NODES
```

## Output

このステートメントによって返されるパラメーターを次の表に示します。

| **Parameter**        | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| LastStartTime        | コンピュートノードが最後に開始された時刻。                   |
| LastHeartbeat        | コンピュートノードが最後にハートビートを送信した時刻。        |
| Alive                | コンピュートノードが利用可能かどうか。                     |
| SystemDecommissioned | パラメーターの値が `true` の場合、コンピュートノードは StarRocks クラスターから削除されます。削除前に、コンピュートノード内のデータはクローンされます。 |
| ErrMsg               | コンピュートノードがハートビートの送信に失敗した場合のエラーメッセージ。  |
| Status               | コンピュートノードの状態を JSON 形式で表示します。現在、コンピュートノードが状態を最後に送信した時刻のみ表示されます。 |