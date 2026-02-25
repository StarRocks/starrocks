---
displayed_sidebar: docs
---

# REFRESH CONNECTIONS

`SET GLOBAL` によって変更されたグローバル変数を適用するために、すべてのアクティブな接続をリフレッシュします。このコマンドにより、管理者はランタイムパラメータを調整した際に、接続を再接続することなく、長時間接続（接続プールからの接続など）を更新できます。

## 背景

デフォルトでは、StarRocks の `SET GLOBAL` は新しく作成された接続にのみ影響します。既存のセッションは、切断するか、明示的に `SET SESSION` を実行するまで、セッション変数のローカルコピーを保持します。この動作は MySQL と一致していますが、管理者がランタイムパラメータを調整する際に、長時間接続を更新することが困難です。

`REFRESH CONNECTIONS` コマンドは、管理者がすべてのアクティブな接続をリフレッシュして最新のグローバル変数値を適用できるようにすることで、この問題を解決します。

:::tip

このコマンドには、SYSTEM オブジェクトに対する OPERATE 権限が必要です。

:::

## 構文

```SQL
REFRESH CONNECTIONS [FORCE];
```

## パラメータ

| **パラメータ** | **必須** | **説明**                                              |
| ------------- | -------- | ------------------------------------------------------------ |
| FORCE         | いいえ   | 指定した場合、セッションで `SET SESSION` を使用して変更された変数であっても、すべての変数を強制的にリフレッシュします。`FORCE` を指定しない場合、セッションによって変更されていない変数のみがリフレッシュされます。 |

## 動作

- **選択的なリフレッシュ**：デフォルトでは、このコマンドはセッション自体によって変更されていない変数のみをリフレッシュします。`SET SESSION` を使用して明示的に設定された変数は保持されます。`FORCE` を指定した場合、セッションの変更に関係なく、すべての変数がリフレッシュされます。

- **即座の実行**：実行中のステートメントは即座には影響を受けません。リフレッシュは次のコマンドが実行される前に有効になります。

- **分散実行**：このコマンドは RPC を介してすべての FE ノードに自動的に配布され、クラスタ全体の一貫性を確保します。

- **権限要件**：SYSTEM オブジェクトに対する OPERATE 権限を持つユーザーのみがこのコマンドを実行できます。

## 例

例 1: グローバル変数を変更した後、すべての接続をリフレッシュします。

```Plain
mysql> SET GLOBAL query_timeout = 600;
Query OK, 0 rows affected (0.00 sec)

mysql> REFRESH CONNECTIONS;
Query OK, 0 rows affected (0.00 sec)
```

`REFRESH CONNECTIONS` を実行した後、すべてのアクティブな接続（セッションで `query_timeout` を変更した接続を除く）の `query_timeout` 値が 600 に更新されます。

例 2: 複数のグローバル変数を変更した後、接続をリフレッシュします。

```Plain
mysql> SET GLOBAL query_timeout = 600;
Query OK, 0 rows affected (0.00 sec)

mysql> SET GLOBAL exec_mem_limit = 2147483648;
Query OK, 0 rows affected (0.00 sec)

mysql> REFRESH CONNECTIONS;
Query OK, 0 rows affected (0.00 sec)
```

例 3: セッションで変更された変数を含むすべての接続を強制的にリフレッシュします。

```Plain
mysql> SET GLOBAL query_timeout = 600;
Query OK, 0 rows affected (0.00 sec)

mysql> REFRESH CONNECTIONS FORCE;
Query OK, 0 rows affected (0.00 sec)
```

`REFRESH CONNECTIONS FORCE` を実行した後、すべてのアクティブな接続の `query_timeout` 値が 600 に更新されます。以前に `SET SESSION query_timeout = 500` を使用してこの変数を変更した接続でも同様です。

## 注意事項

- デフォルトでは、`SET SESSION` を使用してセッションで変更された変数はリフレッシュされません。たとえば、接続が `SET SESSION query_timeout = 500` を実行した場合、`REFRESH CONNECTIONS` を実行した後でも、その接続の `query_timeout` 値は 500 のままです。ただし、`REFRESH CONNECTIONS FORCE` を使用すると、セッションの変更に関係なく、すべての変数がリフレッシュされます。

- `FORCE` を指定した場合、以前にセッションによって変更された変数はグローバルデフォルト値にリセットされ、変更フラグがクリアされます。これにより、今後の非強制リフレッシュでこれらの変数を更新できるようになります。

- このコマンドは、クラスタ内のすべての FE ノード上のすべてのアクティブな接続に影響します。

- セッション専用変数（グローバルに設定できない変数）は、このコマンドの影響を受けません。

- `DISABLE_FORWARD_TO_LEADER` フラグを持つ変数はリフレッシュされません。

## 関連ステートメント

- [SET](./SET.md): システム変数またはユーザー定義変数を設定する
- [SHOW VARIABLES](./SHOW_VARIABLES.md): システム変数を表示する
