---
displayed_sidebar: docs
---

# [Experimental] Kudu catalog

StarRocks supports Kudu catalogs from v3.3 onwards.

A Kudu catalog is a kind of external catalog that enables you to query data from Apache Kudu without ingestion.

Also, you can directly transform and load data from Kudu by using [INSERT INTO](../../sql-reference/sql-statements/loading_unloading/INSERT.md) based on Kudu catalogs.

To ensure successful SQL workloads on your Kudu cluster, your StarRocks cluster needs to integrate with the following important components:

- Metastore like your kudu file system or Hive metastore

## Usage notes

You can only use Kudu catalogs to query data. You cannot use Kudu catalogs to drop, delete, or insert data into your Kudu cluster.

## Integration preparations

Before you create a Kudu catalog, make sure your StarRocks cluster can integrate with the storage system and metastore of your Kudu cluster.

> **NOTE**
>
> If an error indicating an unknown host is returned when you send a query, you must add the mapping between the host names and IP addresses of your KUDU cluster nodes to the **/etc/hosts** path.

### Kerberos authentication

If Kerberos authentication is enabled for your KUDU cluster or Hive metastore, configure your StarRocks cluster as follows:

- Run the `kinit -kt keytab_path principal` command on each FE and each BE to obtain Ticket Granting Ticket (TGT) from Key Distribution Center (KDC). To run this command, you must have the permissions to access your KUDU cluster and Hive metastore. Note that accessing KDC with this command is time-sensitive. Therefore, you need to use cron to run this command periodically.
- Add `JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"` to the **$FE_HOME/conf/fe.conf** file of each FE and to the **$BE_HOME/conf/be.conf** file of each BE. In this example, `/etc/krb5.conf` is the save path of the **krb5.conf** file. You can modify the path based on your needs.

## Create a Kudu catalog

### Syntax

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES
(
    "type" = "kudu",
    CatalogParams
)
```

### Parameters

#### catalog_name

The name of the Kudu catalog. The naming conventions are as follows:

- The name can contain letters, digits (0-9), and underscores (_). It must start with a letter.
- The name is case-sensitive and cannot exceed 1023 characters in length.

#### comment

The description of the Kudu catalog. This parameter is optional.

#### type

The type of your data source. Set the value to `kudu`.

#### CatalogParams

A set of parameters about how StarRocks accesses the metadata of your Kudu cluster.

The following table describes the parameter you need to configure in `CatalogParams`.

| Parameter           | Required | Description                                                                                                                                                                                                                                                                                                                                                                                                             |
|---------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| kudu.catalog.type   | Yes      | The type of metastore that you use for your Kudu cluster. Set this parameter to `kudu` or `hive`.                                                                                                                                                                                                                                                                                                                       |
| kudu.master                   | No       | Specifies the Kudu Master address, which defaults to `localhost:7051`.                                                                                                                                                                                                                                                                                                                                         |
| hive.metastore.uris | No       | The URI of your Hive metastore. Format: `thrift://<metastore_IP_address>:<metastore_port>`. If high availability (HA) is enabled for your Hive metastore, you can specify multiple metastore URIs and separate them with commas (`,`), for example, `"thrift://<metastore_IP_address_1>:<metastore_port_1>,thrift://<metastore_IP_address_2>:<metastore_port_2>,thrift://<metastore_IP_address_3>:<metastore_port_3>"`. |
| kudu.schema-emulation.enabled | No       | option to enable or disable the `schema` emulation. By default, it is turned off (false), which means that all tables belong to the `default` `schema`.                                                                                                                                                                                                                                                                 |
| kudu.schema-emulation.prefix | No       | The prefix for `schema` emulation should only be set when `kudu.schema-emulation.enabled` = `true`. The default prefix used is empty string: ` `.                                                                                                                                                                                                                                                                       |

> **NOTE**
>
> If you use Hive metastore, you must add the mapping between the host names and IP addresses of your Hive metastore nodes to the `/etc/hosts` path before you query Kudu data. Otherwise, StarRocks may fail to access your Hive metastore when you start a query.

### Examples

- The following examples create a Kudu catalog named `kudu_catalog` whose metastore type `kudu.catalog.type` is set to `kudu` to query data from your Kudu cluster.

  ```SQL
  CREATE EXTERNAL CATALOG kudu_catalog
  PROPERTIES
  (
      "type" = "kudu",
      "kudu.master" = "localhost:7051",
      "kudu.catalog.type" = "kudu",
      "kudu.schema-emulation.enabled" = "true",
      "kudu.schema-emulation.prefix" = "impala::"
  );
  ```

- The following examples create a Kudu catalog named `kudu_catalog` whose metastore type `kudu.catalog.type` is set to `hive` to query data from your Kudu cluster.

  ```SQL
  CREATE EXTERNAL CATALOG kudu_catalog
  PROPERTIES
  (
      "type" = "kudu",
      "kudu.master" = "localhost:7051",
      "kudu.catalog.type" = "hive",
      "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
      "kudu.schema-emulation.enabled" = "true",
      "kudu.schema-emulation.prefix" = "impala::"
  );
  ```

## View Kudu catalogs

You can use [SHOW CATALOGS](../../sql-reference/sql-statements/Catalog/SHOW_CATALOGS.md) to query all catalogs in the current StarRocks cluster:

```SQL
SHOW CATALOGS;
```

You can also use [SHOW CREATE CATALOG](../../sql-reference/sql-statements/Catalog/SHOW_CREATE_CATALOG.md) to query the creation statement of an external catalog. The following example queries the creation statement of a Kudu catalog named `kudu_catalog`:

```SQL
SHOW CREATE CATALOG kudu_catalog;
```

## Drop a Kudu catalog

You can use [DROP CATALOG](../../sql-reference/sql-statements/Catalog/DROP_CATALOG.md) to drop an external catalog.

The following example drops a Kudu catalog named `kudu_catalog`:

```SQL
DROP Catalog kudu_catalog;
```

## View the schema of a Kudu table

You can use one of the following syntaxes to view the schema of a Kudu table:

- View schema

  ```SQL
  DESC[RIBE] <catalog_name>.<database_name>.<table_name>;
  ```

- View schema and location from the CREATE statement

  ```SQL
  SHOW CREATE TABLE <catalog_name>.<database_name>.<table_name>;
  ```

## Query a Kudu table

1. Use [SHOW DATABASES](../../sql-reference/sql-statements/Database/SHOW_DATABASES.md) to view the databases in your Kudu cluster:

   ```SQL
   SHOW DATABASES FROM <catalog_name>;
   ```

2. Use [SET CATALOG](../../sql-reference/sql-statements/Catalog/SET_CATALOG.md) to switch to the destination catalog in the current session:

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   Then, use [USE](../../sql-reference/sql-statements/Database/USE.md) to specify the active database in the current session:

   ```SQL
   USE <db_name>;
   ```

   Or, you can use [USE](../../sql-reference/sql-statements/Database/USE.md) to directly specify the active database in the destination catalog:

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

3. Use [SELECT](../../sql-reference/sql-statements/table_bucket_part_index/SELECT.md) to query the destination table in the specified database:

   ```SQL
   SELECT count(*) FROM <table_name> LIMIT 10;
   ```

## Load data from Kudu

Suppose you have an OLAP table named `olap_tbl`, you can transform and load data like below:

```SQL
INSERT INTO default_catalog.olap_db.olap_tbl SELECT * FROM kudu_table;
```
