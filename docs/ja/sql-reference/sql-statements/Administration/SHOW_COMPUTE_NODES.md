---
displayed_sidebar: docs
---

# SHOW COMPUTE NODES

## Description

StarRocks クラスター内のすべてのコンピュートノードを表示します。

## Syntax

```SQL
SHOW COMPUTE NODES
```

## Output

このステートメントによって返されるパラメーターを以下の表に示します。

| **Parameter**        | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| LastStartTime        | コンピュートノードが最後に開始された時刻。                   |
| LastHeartbeat        | コンピュートノードが最後にハートビートを送信した時刻。        |
| Alive                | コンピュートノードが利用可能かどうか。                     |
| SystemDecommissioned | パラメーターの値が `true` の場合、コンピュートノードは StarRocks クラスターから削除されます。削除前に、コンピュートノード内のデータはクローンされます。 |
| ErrMsg               | コンピュートノードがハートビートの送信に失敗した場合のエラーメッセージ。  |
| Status               | コンピュートノードの状態で、JSON 形式で表示されます。現在、コンピュートノードが状態を最後に送信した時刻のみ表示されます。 |