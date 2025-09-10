---
displayed_sidebar: docs
---

# recyclebin_catalogs

`recyclebin_catalogs` 提供有关 FE 回收站里暂存的已删除的数据库、表、分区的元数据信息。

`recyclebin_catalogs` 提供以下字段：

| **字段**                  | **描述**                               |
| ------------------------- | ---------------------------------------|
| TYPE                      | 被删除的元数据类型，包括 Database、Table 和 Partition。 |
| NAME                      | 被删除对象的名字。                     |
| DB_ID                     | 被删除对象的 DB ID。                   |
| TABLE_ID                  | 被删除对象的 Table ID。                |
| PARTITION_ID              | 被删除对象的 Partition ID。            |
| DROP_TIME                 | 删除时间。                             |
