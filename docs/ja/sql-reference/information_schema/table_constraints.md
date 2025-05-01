---
displayed_sidebar: docs
---

# table_constraints

:::note

このビューは、StarRocks の利用可能な機能には適用されません。

:::

`table_constraints` は、どのテーブルに制約があるかを示します。

`table_constraints` には以下のフィールドが提供されています。

| **Field**          | **Description**                                              |
| ------------------ | ------------------------------------------------------------ |
| CONSTRAINT_CATALOG | 制約が属する catalog の名前。この値は常に `def` です。 |
| CONSTRAINT_SCHEMA  | 制約が属するデータベースの名前。    |
| CONSTRAINT_NAME    | 制約の名前。                                  |
| TABLE_SCHEMA       | テーブルが属するデータベースの名前。         |
| TABLE_NAME         | テーブルの名前。                                       |
| CONSTRAINT_TYPE    | 制約のタイプ。                                      |