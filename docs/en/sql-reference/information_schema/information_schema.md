---
displayed_sidebar: docs
---

# Information Schema

import DocCardList from '@theme/DocCardList';

The StarRocks Information Schema is a database within each StarRocks instance. Information Schema contains several read-only, system-defined views that store extensive metadata information of all objects that the StarRocks instance maintains. The StarRocks Information Schema is based on the SQL-92 ANSI Information Schema, but with the addition of views and functions that are specific to StarRocks.

From v3.2.0, The StarRocks Information Schema supports manage metadata for external catalogs.

## View metadata via Information Schema

You can view the metadata information within a StarRocks instance by querying the content of views in Information Schema.

The following example checks metadata information about a table named `table1` in StarRocks by querying the view `tables`.

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

## Views in Information Schema

The StarRocks Information Schema contains the following metadata views:

<DocCardList />
