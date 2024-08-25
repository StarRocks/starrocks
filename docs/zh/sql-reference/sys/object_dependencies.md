---
displayed_sidebar: docs
---

# object_dependencies

您可以通过查询 `object_dependencies` 视图查看异步物化视图的依赖关系。

`object_dependencies` 提供以下字段：

| **字段**            | **描述**                                           |
| ------------------- | -------------------------------------------------- |
| object_id           | 对象的 ID。                                        |
| object_name         | 对象的名称。                                       |
| object_database     | 对象所在的数据库。                                 |
| object_catalog      | 对象所在的 Catalog。该字段常为 `default_catalog`。 |
| object_type         | 对象的类型。该字段常为 `MATERIALIZED_VIEW`。       |
| ref_object_id       | 依赖对象的 ID。                                    |
| ref_object_name     | 依赖对象的名称。                                   |
| ref_object_database | 依赖对象所在的数据库。                             |
| ref_object_catalog  | 依赖的对象所在的 Catalog。                         |
| ref_object_type     | 依赖的对象的类型。                                 |

示例：

```Plain
MySQL > SELECT * FROM sys.object_dependencies\G
*************************** 1. row ***************************
          object_id: 11115
        object_name: mv2
    object_database: test_db
     object_catalog: default_catalog
        object_type: MATERIALIZED_VIEW
      ref_object_id: 11092
    ref_object_name: mv1
ref_object_database: test_db
 ref_object_catalog: default_catalog
    ref_object_type: MATERIALIZED_VIEW
*************************** 2. row ***************************
          object_id: 11092
        object_name: mv1
    object_database: test_db
     object_catalog: default_catalog
        object_type: MATERIALIZED_VIEW
      ref_object_id: 11074
    ref_object_name: test_tbl
ref_object_database: test_db
 ref_object_catalog: default_catalog
    ref_object_type: OLAP
```

上述示例表明，物化视图 `mv1` 是基于 StarRocks 内表 `test_tbl` 创建的，物化视图 `mv2` 是基于物化视图 `mv1` 创建的。
