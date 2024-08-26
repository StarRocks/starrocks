---
displayed_sidebar: docs
keywords: ['Yuanshuju']
---

# Information Schema

import DocCardList from '@theme/DocCardList';

Information Schema 是 StarRocks 实例中的一个数据库。该数据库包含数张由系统定义的视图，这些视图中存储了关于 StarRocks 实例中所有对象的大量元数据信息。

自 v3.2.0 起，Information Schema 支持管理 External Catalog 中的元数据信息。

## 通过 Information Schema 查看元数据信息

您可以通过查询 Information Schema 中的视图来查看 StarRocks 实例中的元数据信息。

以下示例通过查询视图 `tables` 查看 StarRocks 中名为 `table1` 的表相关的元数据信息。

```Plain
MySQL > SELECT * FROM information_schema.tables WHERE TABLE_NAME like 'table1'\G
*************************** 1. row ***************************
  TABLE_CATALOG: def
   TABLE_SCHEMA: test_db
     TABLE_NAME: table1
     TABLE_TYPE: BASE TABLE
         ENGINE: StarRocks
        VERSION: NULL
     ROW_FORMAT: 
     TABLE_ROWS: 4
 AVG_ROW_LENGTH: 1657
    DATA_LENGTH: 6630
MAX_DATA_LENGTH: NULL
   INDEX_LENGTH: NULL
      DATA_FREE: NULL
 AUTO_INCREMENT: NULL
    CREATE_TIME: 2023-06-13 11:37:00
    UPDATE_TIME: 2023-06-13 11:38:06
     CHECK_TIME: NULL
TABLE_COLLATION: utf8_general_ci
       CHECKSUM: NULL
 CREATE_OPTIONS: 
  TABLE_COMMENT: 
1 row in set (0.01 sec)
```

## Information Schema 中的视图

StarRocks Information Schema 中包含以下视图：

<DocCardList />
