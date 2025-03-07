---
displayed_sidebar: docs
---

# routines

:::note

このビューは、StarRocks の利用可能な機能には適用されません。

:::

`routines` には、すべてのストアドルーチン（ストアドプロシージャおよびストアドファンクション）が含まれています。

`routine` で提供されるフィールドは次のとおりです:

| **Field**            | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| SPECIFIC_NAME        | ルーチンの名前。                                             |
| ROUTINE_CATALOG      | ルーチンが属する catalog の名前。この値は常に `def` です。   |
| ROUTINE_SCHEMA       | ルーチンが属するデータベースの名前。                         |
| ROUTINE_NAME         | ルーチンの名前。                                             |
| ROUTINE_TYPE         | ストアドプロシージャの場合は `PROCEDURE`、ストアドファンクションの場合は `FUNCTION`。 |
| DTD_IDENTIFIER       | ルーチンがストアドファンクションの場合、戻り値のデータ型。ルーチンがストアドプロシージャの場合、この値は空です。 |
| ROUTINE_BODY         | ルーチン定義に使用される言語。この値は常に `SQL` です。     |
| ROUTINE_DEFINITION   | ルーチンによって実行される SQL 文のテキスト。               |
| EXTERNAL_NAME        | この値は常に `NULL` です。                                   |
| EXTERNAL_LANGUAGE    | ストアドルーチンの言語。                                     |
| PARAMETER_STYLE      | この値は常に `SQL` です。                                    |
| IS_DETERMINISTIC     | ルーチンが `DETERMINISTIC` 特性で定義されているかどうかに応じて `YES` または `NO`。 |
| SQL_DATA_ACCESS      | ルーチンのデータアクセス特性。この値は `CONTAINS SQL`、`NO SQL`、`READS SQL DATA`、または `MODIFIES SQL DATA` のいずれかです。 |
| SQL_PATH             | この値は常に `NULL` です。                                   |
| SECURITY_TYPE        | ルーチンの `SQL SECURITY` 特性。この値は `DEFINER` または `INVOKER` のいずれかです。 |
| CREATED              | ルーチンが作成された日時。これは `DATETIME` 値です。         |
| LAST_ALTERED         | ルーチンが最後に変更された日時。これは `DATETIME` 値です。ルーチンが作成以来変更されていない場合、この値は `CREATED` 値と同じです。 |
| SQL_MODE             | ルーチンが作成または変更されたときに有効だった SQL モード、およびルーチンが実行されるモード。 |
| ROUTINE_COMMENT      | ルーチンにコメントがある場合、そのテキスト。ない場合、この値は空です。 |
| DEFINER              | `DEFINER` 句で指定されたユーザー（通常はルーチンを作成したユーザー）。 |
| CHARACTER_SET_CLIENT |                                                              |
| COLLATION_CONNECTION |                                                              |
| DATABASE_COLLATION   | ルーチンが関連付けられているデータベースの照合順序。         |