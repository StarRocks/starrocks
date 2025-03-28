---
displayed_sidebar: docs
---

# columns

`columns` には、すべてのテーブル列（またはビュー列）に関する情報が含まれています。

:::note
[同期マテリアライズドビュー](../../using_starrocks/Materialized_view-single_table.md) のメタデータは `columns` に記録されません。`SHOW PROC '/dbs/db/table/index_schema'` を実行することでアクセスできます。
:::

`columns` には次のフィールドが提供されています:

| **Field**                | **Description**                                              |
| ------------------------ | ------------------------------------------------------------ |
| TABLE_CATALOG            | 列を含むテーブルが属する catalog の名前。この値は常に `NULL` です。 |
| TABLE_SCHEMA             | 列を含むテーブルが属するデータベースの名前。 |
| TABLE_NAME               | 列を含むテーブルの名前。                 |
| COLUMN_NAME              | 列の名前。                                      |
| ORDINAL_POSITION         | テーブル内での列の順序位置。         |
| COLUMN_DEFAULT           | 列のデフォルト値。列が `NULL` の明示的なデフォルトを持つ場合、または列定義に `DEFAULT` 句が含まれていない場合は `NULL` です。 |
| IS_NULLABLE              | 列の null 許可属性。`NULL` 値を列に格納できる場合は `YES`、できない場合は `NO` です。 |
| DATA_TYPE                | 列のデータ型。`DATA_TYPE` の値は型名のみで、他の情報は含まれません。`COLUMN_TYPE` の値には型名と、精度や長さなどの他の情報が含まれる場合があります。 |
| CHARACTER_MAXIMUM_LENGTH | 文字列列の場合、文字単位の最大長。        |
| CHARACTER_OCTET_LENGTH   | 文字列列の場合、バイト単位の最大長。             |
| NUMERIC_PRECISION        | 数値列の場合、数値の精度。                  |
| NUMERIC_SCALE            | 数値列の場合、数値のスケール。                      |
| DATETIME_PRECISION       | 時間列の場合、秒の小数部の精度。      |
| CHARACTER_SET_NAME       | 文字列列の場合、文字セットの名前。        |
| COLLATION_NAME           | 文字列列の場合、照合順序の名前。            |
| COLUMN_TYPE              | 列のデータ型。<br />`DATA_TYPE` の値は型名のみで、他の情報は含まれません。`COLUMN_TYPE` の値には型名と、精度や長さなどの他の情報が含まれる場合があります。 |
| COLUMN_KEY               | 列がインデックスされているかどうか:<ul><li>`COLUMN_KEY` が空の場合、列はインデックスされていないか、複数列の非一意インデックスの二次列としてのみインデックスされています。</li><li>`COLUMN_KEY` が `PRI` の場合、列は `PRIMARY KEY` であるか、複数列の `PRIMARY KEY` の一部です。</li><li>`COLUMN_KEY` が `UNI` の場合、列は `UNIQUE` インデックスの最初の列です。（`UNIQUE` インデックスは複数の `NULL` 値を許可しますが、列が `NULL` を許可するかどうかは `Null` 列を確認することで判断できます。）</li><li>`COLUMN_KEY` が `DUP` の場合、列は非一意インデックスの最初の列であり、列内で特定の値の複数の出現が許可されます。</li></ul>テーブルの特定の列に複数の `COLUMN_KEY` 値が適用される場合、`COLUMN_KEY` は優先順位の高い順に `PRI`、`UNI`、`DUP` の順で表示されます。<br />`UNIQUE` インデックスは、`NULL` 値を含むことができず、テーブルに `PRIMARY KEY` がない場合、`PRI` として表示されることがあります。複数の列が複合 `UNIQUE` インデックスを形成する場合、`UNIQUE` インデックスは `MUL` として表示されることがあります。列の組み合わせは一意ですが、各列は特定の値の複数の出現を保持することができます。 |
| EXTRA                    | 特定の列に関して利用可能な追加情報。 |
| PRIVILEGES               | 列に対して持っている権限。                      |
| COLUMN_COMMENT           | 列定義に含まれるコメント。               |
| COLUMN_SIZE              |                                                              |
| DECIMAL_DIGITS           |                                                              |
| GENERATION_EXPRESSION    | 生成列の場合、列の値を計算するために使用される式を表示します。非生成列の場合は空です。 |
| SRS_ID                   | この値は空間列に適用されます。列に格納されている値の空間参照系を示す列 `SRID` 値を含みます。 |