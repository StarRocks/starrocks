---
displayed_sidebar: docs
---

# Information Schema

import DocCardList from '@theme/DocCardList';

StarRocks Information Schema は、各 StarRocks インスタンス内のデータベースです。Information Schema には、StarRocks インスタンスが管理するすべてのオブジェクトの広範なメタデータ情報を格納する、読み取り専用のシステム定義ビューがいくつか含まれています。StarRocks Information Schema は、SQL-92 ANSI Information Schema に基づいていますが、StarRocks に特有のビューと関数が追加されています。

バージョン 3.2.0 から、StarRocks Information Schema は external catalogs のメタデータ管理をサポートしています。

## Information Schema を通じたメタデータの表示

StarRocks インスタンス内のメタデータ情報は、Information Schema 内のビューの内容をクエリすることで表示できます。

次の例では、StarRocks 内の `table1` という名前のテーブルに関するメタデータ情報を、ビュー `tables` をクエリすることで確認します。

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

## Information Schema のビュー

StarRocks Information Schema には、以下のメタデータビューが含まれています。

<DocCardList />