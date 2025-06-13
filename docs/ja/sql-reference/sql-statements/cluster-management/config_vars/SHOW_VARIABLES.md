---
displayed_sidebar: docs
---

# SHOW VARIABLES

## 説明

StarRocks のシステム変数を表示します。システム変数の詳細については、[System Variables](../../../System_variable.md) を参照してください。

:::tip

この操作には特権は必要ありません。

:::

## 構文

```SQL
SHOW [ GLOBAL | SESSION ] VARIABLES
    [ LIKE <pattern> | WHERE <expr> ]
```

## パラメータ

| **パラメータ**          | **説明**                                              |
| ---------------------- | ------------------------------------------------------------ |
| 修飾子:<ul><li>GLOBAL</li><li>SESSION</li></ul> | <ul><li>`GLOBAL` 修飾子を使用すると、ステートメントはグローバルシステム変数の値を表示します。これらは、StarRocks への新しい接続のために対応するセッション変数を初期化するために使用される値です。変数にグローバル値がない場合、値は表示されません。</li><li>`SESSION` 修飾子を使用すると、ステートメントは現在の接続に有効なシステム変数の値を表示します。変数にセッション値がない場合、グローバル値が表示されます。`LOCAL` は `SESSION` の同義語です。</li><li>修飾子がない場合、デフォルトは `SESSION` です。</li></ul> |
| pattern                | LIKE 句を使用して変数名で変数を一致させるために使用されるパターンです。このパラメータでは % ワイルドカードを使用できます。 |
| expr                   | WHERE 句を使用して変数名 `variable_name` または変数値 `value` で変数を一致させるために使用される式です。 |

## 戻り値

| **戻り値**    | **説明**            |
| ------------- | -------------------------- |
| Variable_name | 変数の名前。  |
| Value         | 変数の値。 |

## 例

例 1: LIKE 句を使用して変数名を正確に一致させて変数を表示します。

```Plain
mysql> SHOW VARIABLES LIKE 'wait_timeout';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| wait_timeout  | 28800 |
+---------------+-------+
1 row in set (0.01 sec)
```

例 2: LIKE 句とワイルドカード (%) を使用して変数名をおおよそ一致させて変数を表示します。

```Plain
mysql> SHOW VARIABLES LIKE '%imeou%';
+------------------------------------+-------+
| Variable_name                      | Value |
+------------------------------------+-------+
| interactive_timeout                | 3600  |
| net_read_timeout                   | 60    |
| net_write_timeout                  | 60    |
| new_planner_optimize_timeout       | 3000  |
| query_delivery_timeout             | 300   |
| query_queue_pending_timeout_second | 300   |
| query_timeout                      | 300   |
| tx_visible_wait_timeout            | 10    |
| wait_timeout                       | 28800 |
+------------------------------------+-------+
9 rows in set (0.00 sec)
```

例 3: WHERE 句を使用して変数名を正確に一致させて変数を表示します。

```Plain
mysql> show variables where variable_name = 'wait_timeout';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| wait_timeout  | 28800 |
+---------------+-------+
1 row in set (0.17 sec)
```

例 4: WHERE 句を使用して変数の値を正確に一致させて変数を表示します。

```Plain
mysql> show variables where value = '28800';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| wait_timeout  | 28800 |
+---------------+-------+
1 row in set (0.70 sec)
```