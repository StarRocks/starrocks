---
displayed_sidebar: docs
---

# key_column_usage

:::note

このビューは、StarRocks の利用可能な機能には適用されません。

:::

`key_column_usage` は、ユニークキー、主キー、または外部キー制約によって制限されているすべてのカラムを識別します。

`key_column_usage` で提供されるフィールドは以下の通りです:

| **Field**                     | **Description**                                              |
| ----------------------------- | ------------------------------------------------------------ |
| CONSTRAINT_CATALOG            | 制約が属する catalog の名前。この値は常に `def` です。 |
| CONSTRAINT_SCHEMA             | 制約が属するデータベースの名前。    |
| CONSTRAINT_NAME               | 制約の名前。                                  |
| TABLE_CATALOG                 | テーブルが属する catalog の名前。この値は常に `def` です。 |
| TABLE_SCHEMA                  | テーブルが属するデータベースの名前。         |
| TABLE_NAME                    | 制約を持つテーブルの名前。               |
| COLUMN_NAME                   | 制約を持つカラムの名前。制約が外部キーの場合、これは外部キーのカラムであり、外部キーが参照するカラムではありません。 |
| ORDINAL_POSITION              | 制約内でのカラムの位置であり、テーブル内でのカラムの位置ではありません。カラムの位置は 1 から始まります。 |
| POSITION_IN_UNIQUE_CONSTRAINT | ユニークキーおよび主キー制約の場合は `NULL`。外部キー制約の場合、このカラムは参照されるテーブルのキー内での序数位置です。 |
| REFERENCED_TABLE_SCHEMA       | 制約によって参照されるスキーマの名前。         |
| REFERENCED_TABLE_NAME         | 制約によって参照されるテーブルの名前。          |
| REFERENCED_COLUMN_NAME        | 制約によって参照されるカラムの名前。         |