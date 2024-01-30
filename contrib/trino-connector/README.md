# StarRocks connector
This is a connector for StarRocks that is compatible with [Trino](https://trino.io/). The connector allows querying and inserting in an external StarRocks instance. This can be used to join data between different systems like StarRocks and Hive, or between two different StarRocks instances.

## Requirements
To connect to StarRocks, you need:
- StarRocks 2.5.0 or higher.
- Network access from the Trino coordinator and workers to StarRocks. Port 9030 and 8030 are the default port.


## Configuration
To configure the StarRocks connector, create a catalog properties file in `etc/catalog` named, for example, example.properties, to mount the StarRocks connector as the starrocks catalog. Create the file with the following contents, replacing the connection properties as appropriate for your setup:
```
connector.name=starrocks
connection-url=jdbc:mysql://starrocks:9030
connection-user=root
connection-password=
starrocks.client.load-url=starrocks:8080
```

The `connection-url` defines the connection information and parameters to pass to the MySQL JDBC driver. The `connection-user` and `connection-password` are typically required and determine the user credentials for the connection, often a service user. You can use secrets to avoid actual values in the catalog properties files.

The following table describes configuration properties for connection credentials:
| Property name | Required | Default value | Description |
|-|-|-|-|
| connector.name | Yes | NONE | Connector name. |
| connection-url | Yes | NONE | Connection information and parameters. |
| connection-user | Yes | NONE | Connection user name. |
| connection-password | Yes | NONE | Connection password. |
| starrocks.client.load-url | Yes | NONE | The address that is used to connect to the HTTP server of the FE. You can specify multiple addresses, which must be separated by a semicolon (;). Format: <fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>. |
| starrocks.client.label-prefix | No | trino- | The label prefix used by Stream Load. |
| starrocks.client.max-cache-bytes | No | 268435456 | The maximum size of data that can be accumulated in memory before being sent to StarRocks at a time. The maximum value ranges from 64 MB to 10 GB, the default value is 256MB. |
| starrocks.client.connect-timeout |  No | 30000 | The timeout for establishing HTTP connection. Valid values: 100 to 60000. Unit: ms, the default value is 30000. |


## Multiple StarRocks servers
You can have as many catalogs as you need, so if you have additional StarRocks servers, simply add another properties file to `etc/catalog` with a different name, making sure it ends in .properties. For example, if you name the property file sales.properties, Trino creates a catalog named sales using the configured connector.

## Type mapping
Because Trino and StarRocks each support types that the other does not, this connector modifies some types when reading or writing data. Data types may not map the same way in both directions between Trino and the data source. Refer to the following sections for type mapping in each direction.

### StarRocks to Trino type mapping
The connector maps StarRocks types to the corresponding Trino types following this table:
| StarRocks type | Trino type | Notes |
|-|-|-|
| `BOOLEAN` | `TINYINT	` |  |
| `TINYINT` | `TINYINT` |  |
| `SMALLINT` | `SMALLINT` |  |
| `INT` | `INTEGER` |  |
| `BIGINT` | `BIGINT` |  |
| `LARGEINT` | `DECIMAL(20, 0)` |  |
| `DECIMAL(p, s)` | `DECIMAL(p, s)` |  |
| `DOUBLE` | `DOUBLE` |  |
| `FLOAT` | `REAL` |  |
| `CHAR(n)` | `CHAR(n)` |  |
| `STRING` | `VARCHAR(65533)` |  |
| `VARCHAR(n)` | `VARCHAR(n)` |  |
| `BINARY, VARBINARY` | `VARBINARY` |  |
| `DATE` | `DATE` |  |
| `DATETIME` | `TIMESTAMP(0)` |  |
| `JSON` | `JSON` |  |

### Trino to StarRocks type mapping
The connector maps StarRocks types to the corresponding Trino types following this table:
| Trino type | StarRocks type | Notes |
|-|-|-|
| `BOOLEAN` | `BOOLEAN` |  |
| `TINYINT` | `TINYINT` |  |
| `SMALLINT` | `SMALLINT` |  |
| `INTEGER` | `INT` |  |
| `BIGINT` | `BIGINT` |  |
| `REAL` | `FLOAT` |  |
| `DOUBLE` | `DOUBLE` |  |
| `DECIMAL(p, s)` | `DECIMAL(p, s)` |  |
| `VARCHAR(n)` | `VARCHAR(n)` |  |
| `VARCHAR(0)` | `VARCHAR(65533)` |  |
| `CHAR(n)` | `CHAR(n)` |  |
| `VARBINARY` | `VARBINARY` |  |
| `JSON` | `JSON` |  |
| `DATE` | `DATE` |  |
| `TIMESTAMP(0)` | `DATETIME` |  |

No other types are supported.

### Type mapping configuration properties
The following properties can be used to configure how data types from the connected data source are mapped to Trino data types and how the metadata is cached in Trino.
| Property name | Description | Default value |
|-|-|-|
| `upsupported-type-handling` | Configure how unsupported column data types are handled: <br>`IGNORE`, column is not accessible.<br>`CONVERT_TO_VARCHAR`, column is converted to unbounded VARCHAR.<br>The respective catalog session property is unsupported_type_handling. | `IGNORE` |

## Querying StarRocks
The StarRocks connector provides a schema for every StarRocks database. You can see the available StarRocks databases by running SHOW SCHEMAS:

```sql
SHOW SCHEMAS FROM starrocks;
```

If you have a StarRocks database named example, you can view the tables in this database by running SHOW TABLES:

```
SHOW TABLES FROM starrocks.example;
```

You can see a list of the columns in the clicks table in the example database using either of the following:

```sql
DESCRIBE starrocks.example.clicks;
SHOW COLUMNS FROM starrocks.example.clicks;
```

Finally, you can access the clicks table in the example database:

```sql
SELECT * FROM starrocks.example.clicks;
```

If you used a different name for your catalog properties file, use that catalog name instead of starrocks in the above examples.

## SQL support
The connector provides read access and write access to data and metadata in the StarRocks database. In addition to the globally available and read operation statements, the connector supports the following statements:

- [INSERT](https://trino.io/docs/current/sql/insert.html)
- [DELETE](https://trino.io/docs/current/sql/delete.html)
- [TRUNCATE](https://trino.io/docs/current/sql/truncate.html)
- [DROP TABLE](https://trino.io/docs/current/sql/drop-table.html)
- [CREATE SCHEMA](https://trino.io/docs/current/sql/create-schema.html)
- [DROP SCHEMA](https://trino.io/docs/current/sql/drop-schema.html)

### SQL DELETE
If a WHERE clause is specified, the DELETE operation only works if the predicate in the clause can be fully pushed down to the data source.

## Fault-tolerant execution support
The connector does not support [Fault-tolerant](https://trino.io/docs/current/admin/fault-tolerant-execution.html) execution of query processing. 

