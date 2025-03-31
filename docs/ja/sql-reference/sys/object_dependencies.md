---
displayed_sidebar: docs
---

# object_dependencies

非同期マテリアライズドビューの依存関係を、ビュー `object_dependencies` をクエリすることで確認できます。

`object_dependencies` には以下のフィールドが提供されています:

| **Field**           | **Description**                                              |
| ------------------- | ------------------------------------------------------------ |
| object_id           | オブジェクトの ID。                                          |
| object_name         | オブジェクトの名前。                                         |
| object_database     | オブジェクトが存在するデータベース。                         |
| object_catalog      | オブジェクトが存在するカタログ。このフィールドは常に `default_catalog` です。 |
| object_type         | オブジェクトのタイプ。このフィールドは常に `MATERIALIZED_VIEW` です。 |
| ref_object_id       | 参照されるオブジェクトの ID。                                |
| ref_object_name     | 参照されるオブジェクトの名前。                               |
| ref_object_database | 参照されるオブジェクトが存在するデータベース。               |
| ref_object_catalog  | 参照されるオブジェクトが存在するカタログ。                   |
| ref_object_type     | 参照されるオブジェクトのタイプ。                             |

例:

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

上記の例は、マテリアライズドビュー `mv1` が StarRocks 内部テーブル `test_tbl` に基づいて作成され、マテリアライズドビュー `mv2` がマテリアライズドビュー `mv1` に基づいて作成されていることを示しています。