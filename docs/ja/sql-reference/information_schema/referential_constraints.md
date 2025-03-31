---
displayed_sidebar: docs
---

# referential_constraints

:::note

このビューは、StarRocks の利用可能な機能には適用されません。

:::

`referential_constraints` には、すべての参照 (外部キー) 制約が含まれています。

`referential_constraints` で提供されるフィールドは次のとおりです:

| **Field**                 | **Description**                                              |
| ------------------------- | ------------------------------------------------------------ |
| CONSTRAINT_CATALOG        | 制約が属する catalog の名前。この値は常に `def` です。 |
| CONSTRAINT_SCHEMA         | 制約が属するデータベースの名前。    |
| CONSTRAINT_NAME           | 制約の名前。                                  |
| UNIQUE_CONSTRAINT_CATALOG | 制約が参照する一意の制約を含む catalog の名前。この値は常に `def` です。 |
| UNIQUE_CONSTRAINT_SCHEMA  | 制約が参照する一意の制約を含むスキーマの名前。 |
| UNIQUE_CONSTRAINT_NAME    | 制約が参照する一意の制約の名前。 |
| MATCH_OPTION              | 制約の `MATCH` 属性の値。現在有効な値は `NONE` のみです。 |
| UPDATE_RULE               | 制約の `ON UPDATE` 属性の値。有効な値: `CASCADE`、`SET NULL`、`SET DEFAULT`、`RESTRICT`、`NO ACTION`。 |
| DELETE_RULE               | 制約の `ON DELETE` 属性の値。有効な値: `CASCADE`、`SET NULL`、`SET DEFAULT`、`RESTRICT`、`NO ACTION`。 |
| TABLE_NAME                | テーブルの名前。この値は `TABLE_CONSTRAINTS` テーブルと同じです。 |
| REFERENCED_TABLE_NAME     | 制約によって参照されるテーブルの名前。          |