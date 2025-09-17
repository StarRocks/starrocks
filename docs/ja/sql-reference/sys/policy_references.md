---
displayed_sidebar: docs
---

# policy_references

import FeatureAvailability from '../../_assets/commonMarkdown/_feature_availability.mdx'

<FeatureAvailability />

オブジェクトの行アクセスポリシーとカラムマスキングポリシーの参照を、ビュー `policy_references` をクエリすることで確認できます。

`policy_references` には以下のフィールドが提供されています:

| **Field**           | **Description**                                                                                   |
| ------------------- | ------------------------------------------------------------------------------------------------- |
| POLICY_DATABASE     | ポリシーが所在するデータベース。ポリシーの名前空間はデータベースであり、`default_catalog` のみで許可されます。 |
| POLICY_NAME         | ポリシーの名前。                                                                                  |
| POLICY_TYPE         | ポリシーのタイプ。有効な値は `Row Access` と `Column Masking` です。                               |
| REF_CATALOG         | ポリシーが参照されるカタログ。                                                                    |
| REF_DATABASE        | ポリシーが参照されるデータベース。                                                                |
| REF_OBJECT_NAME     | ポリシーが参照されるテーブル。                                                                    |
| REF_COLUMN          | ポリシーが参照されるカラム。このフィールドは行アクセスポリシーの場合、常に NULL です。             |

例:

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