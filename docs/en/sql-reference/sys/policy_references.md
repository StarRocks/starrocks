---
displayed_sidebar: docs
---

# policy_references

import FeatureAvailability from '../../_assets/commonMarkdown/_feature_availability.mdx'

<FeatureAvailability />

You can view the objects row access policies and column masking policies reference by querying the view `policy_references`.

The following fields are provided in `policy_references`:

| **Field**           | **Description**                                                                                |
| ------------------- | ---------------------------------------------------------------------------------------------- |
| POLICY_DATABASE     | The database where the policy is located. The namespace of the policy is the database, which is allowed only under `default_catalog`. |
| POLICY_NAME         | The name of the policy.                                                                        |
| POLICY_TYPE         | The type of the policy. Valid values: `Row Access` and `Column Masking`.                       |
| REF_CATALOG         | The catalog where the policy is referenced.                                                    |
| REF_DATABASE        | The database where the policy is referenced.                                                   |
| REF_OBJECT_NAME     | The table where the policy is referenced.                                                      |
| REF_COLUMN          | The column where the policy is referenced.  This field is always NULL for row access policies. |

Example:

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
