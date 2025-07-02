---
displayed_sidebar: docs
---

# tables

`tables` はテーブルに関する情報を提供します。

`tables` には以下のフィールドが含まれています:

| **Field**       | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| TABLE_CATALOG   | テーブルを格納する catalog の名前。                          |
| TABLE_SCHEMA    | テーブルを格納するデータベースの名前。                       |
| TABLE_NAME      | テーブルの名前。                                             |
| TABLE_TYPE      | テーブルのタイプ。 有効な値: `BASE TABLE` または `VIEW`。   |
| ENGINE          | テーブルのエンジンタイプ。 有効な値: `StarRocks`, `MySQL`, `MEMORY` または空文字列。 |
| VERSION         | StarRocks で利用できない機能に適用されます。                 |
| ROW_FORMAT      | StarRocks で利用できない機能に適用されます。                 |
| TABLE_ROWS      | テーブルの行数。                                             |
| AVG_ROW_LENGTH  | テーブルの平均行長（サイズ）。これは `DATA_LENGTH`/`TABLE_ROWS` に相当します。単位: バイト。 |
| DATA_LENGTH     | テーブルのデータ長は、すべてのレプリカにわたるテーブルのデータ長を合計して決定されます。単位: バイト。 |
| MAX_DATA_LENGTH | StarRocks で利用できない機能に適用されます。                 |
| INDEX_LENGTH    | StarRocks で利用できない機能に適用されます。                 |
| DATA_FREE       | StarRocks で利用できない機能に適用されます。                 |
| AUTO_INCREMENT  | StarRocks で利用できない機能に適用されます。                 |
| CREATE_TIME     | テーブルが作成された時間。                                   |
| UPDATE_TIME     | テーブルが最後に更新された時間。                             |
| CHECK_TIME      | テーブルに対して整合性チェックが最後に行われた時間。         |
| TABLE_COLLATION | テーブルのデフォルトの照合順序。                             |
| CHECKSUM        | StarRocks で利用できない機能に適用されます。                 |
| CREATE_OPTIONS  | StarRocks で利用できない機能に適用されます。                 |
| TABLE_COMMENT   | テーブルに関するコメント。                                   |