---
displayed_sidebar: docs
---

# policy_references

import FeatureAvailability from '../../_assets/commonMarkdown/_feature_availability.mdx'

<FeatureAvailability />

您可以通过查询视图 `policy_references` 来查看对象行访问策略和列掩码策略的参考。

`policy_references` 中提供了以下字段：

| **Field**           | **Description**                                                                                |
| ------------------- | ---------------------------------------------------------------------------------------------- |
| POLICY_DATABASE     | 策略所在的数据库。策略的命名空间是数据库，仅在 `default_catalog` 下允许。                     |
| POLICY_NAME         | 策略的名称。                                                                                   |
| POLICY_TYPE         | 策略的类型。有效值：`Row Access` 和 `Column Masking`。                                         |
| REF_CATALOG         | 策略引用的 catalog。                                                                           |
| REF_DATABASE        | 策略引用的数据库。                                                                             |
| REF_OBJECT_NAME     | 策略引用的表。                                                                                 |
| REF_COLUMN          | 策略引用的列。对于行访问策略，此字段始终为 NULL。                                              |

示例：

```SQL
mysql > SELECT * FROM policy_references\G
*************************** 1. row ***************************
POLICY_DATABASE: test_db
    POLICY_NAME: region_data
    POLICY_TYPE: Row Access
    REF_CATALOG: default_catalog
   REF_DATABASE: test_db
REF_OBJECT_NAME: test1 -- 表名
     REF_COLUMN: NULL
*************************** 2. row ***************************
POLICY_DATABASE: test_db
    POLICY_NAME: phone_mask
    POLICY_TYPE: Column Masking
    REF_CATALOG: default_catalog
   REF_DATABASE: test_db
REF_OBJECT_NAME: test
     REF_COLUMN: phone
2 rows in set (0.03 sec)
```