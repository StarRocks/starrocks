---
displayed_sidebar: docs
---

# recyclebin_catalogs

`recyclebin_catalogs` provides metadata information of deleted databases, tables, and partitions temporarily stored in the FE recycle bin.

`recyclebin_catalogs` provides the following fields:

| **Field**    | **Description**                         | 
| ------------ | --------------------------------------- | 
| TYPE         | The type of metadata that was deleted, including Database, Table, and Partition. | 
| NAME         | The name of the deleted object.         | 
| DB_ID        | The DB ID of the deleted object.        | 
| TABLE_ID     | The Table ID of the deleted object.     | 
| PARTITION_ID | The Partition ID of the deleted object. | 
| DROP_TIME    | The time when the object is deleted.    |
