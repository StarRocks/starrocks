---
displayed_sidebar: docs
---

# CREATE DATABASE

## Description

This statement is used to create databases.

:::tip

This operation requires the CREATE DATABASE privilege on the target catalog. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
CREATE DATABASE [IF NOT EXISTS] <db_name>
[PROPERTIES ("key"="value", ...)]
```

## Parameters

`db_name`: the name of the database to create. For the naming conventions, see [System limits](../../System_limit.md).

**PROPERTIES (Optional)**

`storage_volume`: Specifies the name of the storage volume that is used to store the database in a shared-data cluster.

## Examples

1. Create the database `db_test`.

   ```sql
   CREATE DATABASE db_test;
   ```

2. Create the cloud-native database `cloud_db` with the storage volume `s3_storage_volume`.

   ```sql
   CREATE DATABASE cloud_db
   PROPERTIES ("storage_volume"="s3_storage_volume");
   ```

## References

- [SHOW CREATE DATABASE](SHOW_CREATE_DATABASE.md)
- [USE](USE.md)
- [SHOW DATABASES](SHOW_DATABASES.md)
- [DESC](../table_bucket_part_index/DESCRIBE.md)
- [DROP DATABASE](DROP_DATABASE.md)
