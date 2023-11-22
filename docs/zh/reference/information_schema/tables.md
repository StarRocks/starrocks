---
displayed_sidebar: "Chinese"
---

# tables

`tables` 提供有关表的信息。

`tables` 提供以下字段：

| **字段**        | **描述**                                                     |
| --------------- | ------------------------------------------------------------ |
| TABLE_CATALOG   | 表所属的 Catalog 名称。                                      |
| TABLE_SCHEMA    | 表所属的数据库名称。                                         |
| TABLE_NAME      | 表名。                                                       |
| TABLE_TYPE      | 表的类型。有效值：`BASE TABLE` 和 `VIEW`。                  |
| ENGINE          | 表的引擎类型。有效值：`StarRocks`、`MySQL`、`MEMORY` 和空字符串。 |
| VERSION         | 该字段暂不可用。                                             |
| ROW_FORMAT      | 该字段暂不可用。                                             |
| TABLE_ROWS      | 表的行数。                                                   |
| AVG_ROW_LENGTH  | 表的平均行长度（大小），等于 `DATA_LENGTH`/`TABLE_ROWS`。单位：Byte。 |
| DATA_LENGTH     | 数据长度（大小）。单位：Byte。                              |
| MAX_DATA_LENGTH | 该字段暂不可用。                                             |
| INDEX_LENGTH    | 该字段暂不可用。                                             |
| DATA_FREE       | 该字段暂不可用。                                             |
| AUTO_INCREMENT  | 该字段暂不可用。                                             |
| CREATE_TIME     | 创建表的时间。                                               |
| UPDATE_TIME     | 最后一次更新表的时间。                                       |
| CHECK_TIME      | 最后一次对表进行一致性检查的时间。                           |
| TABLE_COLLATION | 表的默认 Collation。                                         |
| CHECKSUM        | 该字段暂不可用。                                             |
| CREATE_OPTIONS  | 该字段暂不可用。                                             |
| TABLE_COMMENT   | 表的 Comment。                                               |
