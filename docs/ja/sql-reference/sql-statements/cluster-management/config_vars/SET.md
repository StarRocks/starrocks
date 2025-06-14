---
displayed_sidebar: docs
---

# SET

## Description

StarRocks の指定されたシステム変数またはユーザー定義変数を設定します。StarRocks のシステム変数は [SHOW VARIABLES](./SHOW_VARIABLES.md) を使用して表示できます。システム変数の詳細については、[System Variables](../../../System_variable.md) を参照してください。ユーザー定義変数の詳細については、[User-defined variables](../../../user_defined_variables.md) を参照してください。

:::tip

この操作には特権は必要ありません。

:::

## Syntax

```SQL
SET [ GLOBAL | SESSION ] <variable_name> = <value> [, <variable_name> = <value>] ...
```

## Parameters

| **Parameter**          | **Description**                                              |
| ---------------------- | ------------------------------------------------------------ |
| Modifier:<ul><li>GLOBAL</li><li>SESSION</li></ul> | <ul><li>`GLOBAL` 修飾子を使用すると、ステートメントは変数をグローバルに設定します。</li><li>`SESSION` 修飾子を使用すると、ステートメントはセッション内で変数を設定します。`LOCAL` は `SESSION` の同義語です。</li><li>修飾子がない場合、デフォルトは `SESSION` です。</li></ul>グローバル変数とセッション変数の詳細については、[System Variables](../../../System_variable.md) を参照してください。<br/>**NOTE**<br/>ADMIN 権限を持つユーザーのみが変数をグローバルに設定できます。 |
| variable_name          | 変数の名前です。                                    |
| value                  | 変数の値です。                                   |

## Examples

Example 1: セッション内で `time_zone` を `Asia/Shanghai` に設定します。

```Plain
mysql> SET time_zone = "Asia/Shanghai";
Query OK, 0 rows affected (0.00 sec)
```

Example 2: `exec_mem_limit` をグローバルに `2147483648` に設定します。

```Plain
mysql> SET GLOBAL exec_mem_limit = 2147483648;
Query OK, 0 rows affected (0.00 sec)
```

Example 3: 複数のグローバル変数を設定します。各変数の前に `GLOBAL` キーワードを追加します。

```Plain
mysql> SET 
       GLOBAL exec_mem_limit = 2147483648,
       GLOBAL time_zone = "Asia/Shanghai";
Query OK, 0 rows affected (0.00 sec)
```