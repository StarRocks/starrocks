---
displayed_sidebar: docs
---

# object_dependencies

You can view the dependency relationship of asynchronous materialized views by querying the view `object_dependencies`.

The following fields are provided in `object_dependencies`:

| **Field**           | **Description**                                              |
| ------------------- | ------------------------------------------------------------ |
| object_id           | ID of the object.                                            |
| object_name         | The name of the object.                                      |
| object_database     | The Database where the object is located.                    |
| object_catalog      | The catalog where the object is located. This field is always `default_catalog`. |
| object_type         | The type of the object. This field is always `MATERIALIZED_VIEW`. |
| ref_object_id       | ID of the referenced object.                                 |
| ref_object_name     | The name of the referenced object.                           |
| ref_object_database | The database where the referenced object is located.         |
| ref_object_catalog  | The catalog where the referenced object is located.          |
| ref_object_type     | The type of the referenced object.                           |

Example:

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

The above example indicates that the materialized view `mv1` is created based on the StarRocks internal table `test_tbl`, and the materialized view `mv2` is created based on the materialized view `mv1`.
