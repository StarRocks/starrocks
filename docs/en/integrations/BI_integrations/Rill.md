---
displayed_sidebar: docs
---

# Rill

Rill supports connecting to StarRocks as an OLAP connector to power Rill dashboards with external tables. Rill can query and visualize both internal data and external data in StarRocks.

## Connection

Rill connects to StarRocks using the MySQL protocol. You can configure the connection using either connection parameters or a DSN connection string.

### Connection Parameters

When adding a data source in Rill, select **StarRocks** and configure the following parameters:

- **Host**: The FE host IP address or hostname of your StarRocks cluster
- **Port**: The MySQL protocol port of the StarRocks FE (default: `9030`)
- **Username**: The username for authentication (default: `root`)
- **Password**: The password for authentication
- **Catalog**: The StarRocks catalog name (default: `default_catalog`). Supports both internal and external catalogs (e.g., Iceberg, Hive)
- **Database**: The StarRocks database name
- **SSL**: Enable SSL/TLS encryption (default: `false`)

### Connection String (DSN)

Alternatively, you can use a MySQL-format DSN connection string:

```
user:password@tcp(host:9030)/database?parseTime=true
```

For external catalogs, specify the catalog and database as separate properties.

## External Catalogs

Rill supports querying data from external catalogs in StarRocks, including Hive, Iceberg, Delta Lake, and other external data sources. Set the `catalog` property to your external catalog name (e.g., `iceberg_catalog`) and the `database` property to the database within that catalog.

## More Information

For detailed configuration options, examples, troubleshooting, and the most up-to-date information, see the [Rill Data StarRocks connector documentation](https://docs.rilldata.com/developers/build/connectors/olap/starrocks).
