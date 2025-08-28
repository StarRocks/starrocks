---
displayed_sidebar: docs
---

# Manage audit logs within StarRocks via AuditLoader

This topic describes how to manage StarRocks audit logs within a table via the plugin - AuditLoader.

StarRocks stores its audit logs in the local file **fe/log/fe.audit.log** rather than an internal database. The plugin AuditLoader allows you to manage audit logs directly within your cluster. Once installed, AuditLoader reads logs from the file, and loads them into StarRocks via HTTP PUT. You can then query the audit logs in StarRocks using SQL statements.

## Create a table to store audit logs

Create a database and a table in your StarRocks cluster to store its audit logs. See [CREATE DATABASE](../../sql-reference/sql-statements/Database/CREATE_DATABASE.md) and [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) for detailed instructions.

Because the fields of audit logs vary among different StarRocks versions, it is important to follow recommendations mentioned below to avoid compatibility issues during upgrade:

> **CAUTION**
>
> - All new fields should be marked as `NULL`.
> - Fields should NOT be renamed, as users may rely on them.
> - Only backward compatible changes should be applied to field types, e.g. `VARCHAR(32)` -> `VARCHAR(64)`, to avoid errors during insert.
> - `AuditEvent` fields are resolved by name only. The order of columns within table doesn't matter, and can be changed by user in any time.
> - `AuditEvent` fields which doesn't exist in the table are ignored, so users can remove columns they don't need.

```SQL
CREATE DATABASE starrocks_audit_db__;

CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__ (
  `queryId` VARCHAR(64) COMMENT "Unique ID of the query",
  `timestamp` DATETIME NOT NULL COMMENT "Query start time",
  `queryType` VARCHAR(12) COMMENT "Query type (query, slow_query, connection)",
  `clientIp` VARCHAR(32) COMMENT "Client IP",
  `user` VARCHAR(64) COMMENT "Query username",
  `authorizedUser` VARCHAR(64) COMMENT "Unique identifier of the user, i.e., user_identity",
  `resourceGroup` VARCHAR(64) COMMENT "Resource group name",
  `catalog` VARCHAR(32) COMMENT "Catalog name",
  `db` VARCHAR(96) COMMENT "Database where the query runs",
  `state` VARCHAR(8) COMMENT "Query state (EOF, ERR, OK)",
  `errorCode` VARCHAR(512) COMMENT "Error code",
  `queryTime` BIGINT COMMENT "Query execution time (milliseconds)",
  `scanBytes` BIGINT COMMENT "Number of bytes scanned by the query",
  `scanRows` BIGINT COMMENT "Number of rows scanned by the query",
  `returnRows` BIGINT COMMENT "Number of rows returned by the query",
  `cpuCostNs` BIGINT COMMENT "CPU time consumed by the query (nanoseconds)",
  `memCostBytes` BIGINT COMMENT "Memory consumed by the query (bytes)",
  `stmtId` INT COMMENT "Incremental ID of the SQL statement",
  `isQuery` TINYINT COMMENT "Whether the SQL is a query (1 or 0)",
  `feIp` VARCHAR(128) COMMENT "FE IP that executed the statement",
  `stmt` VARCHAR(1048576) COMMENT "Original SQL statement",
  `digest` VARCHAR(32) COMMENT "Fingerprint of slow SQL",
  `planCpuCosts` DOUBLE COMMENT "CPU usage during query planning (nanoseconds)",
  `planMemCosts` DOUBLE COMMENT "Memory usage during query planning (bytes)",
  `pendingTimeMs` BIGINT COMMENT "Time the query waited in the queue (milliseconds)",
  `candidateMVs` VARCHAR(65533) NULL COMMENT "List of candidate materialized views",
  `hitMvs` VARCHAR(65533) NULL COMMENT "List of matched materialized views",
  `warehouse` VARCHAR(32) NULL COMMENT "Warehouse name"
) ENGINE = OLAP
DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)
COMMENT "Audit log table"
PARTITION BY date_trunc('day', `timestamp`)
PROPERTIES (
  "replication_num" = "1",
  "partition_live_number" = "30"
);
```

`starrocks_audit_tbl__` is created with dynamic partitions. By default, the first dynamic partition is created 10 minutes after the table is created. Audit logs can then be loaded into the table. You can check the partitions in the table using the following statement:

```SQL
SHOW PARTITIONS FROM starrocks_audit_db__.starrocks_audit_tbl__;
```

After a partition is created, you can move on to the next step.

## Download and configure AuditLoader

1. [Download](https://releases.starrocks.io/resources/auditloader.zip) the AuditLoader installation package. The package is compatible with all available versions of StarRocks.

2. Unzip the installation package.

    ```shell
    unzip auditloader.zip
    ```

    The following files are inflated:

    - **auditloader.jar**: the JAR file of AuditLoader.
    - **plugin.properties**: the properties file of AuditLoader. You do not need to modify this file.
    - **plugin.conf**: the configuration file of AuditLoader. In most cases, you only need to modify the `user` and `password` fields in the file.

3. Modify **plugin.conf** to configure AuditLoader. You must configure the following items to make sure AuditLoader can work properly:

    - `frontend_host_port`: FE IP address and HTTP port, in the format `<fe_ip>:<fe_http_port>`. It is recommended to set it to its default value `127.0.0.1:8030`. Each FE in StarRocks manages its own Audit Log independently, and after installing the plugin, each FE will start its own background thread to fetch and save Audit Logs, and write them via Stream Load. The `frontend_host_port` configuration item is used to provide the IP and port of the HTTP protocol for the background Stream Load task of the plug-in, and this parameter does not support multiple values. The IP part of the parameter can use the IP of any FE in the cluster, but it is not recommended because if the corresponding FE crashes, the audit log writing task in the background of other FEs will also fail due to the failure of communication. It is recommended to set it to the default value `127.0.0.1:8030`, so that each FE uses its own HTTP port to communicate, thus avoiding the impact on the communication in case of an exception of the other FEs (all the write tasks will be forwarded to the FE Leader node to be executed eventually).
    - `database`: name of the database you created to host audit logs.
    - `table`: name of the table you created to host audit logs.
    - `user`: your cluster username. You MUST have the privilege to load data (LOAD_PRIV) into the table.
    - `password`: your user password.
    - `secret_key`: the key (string, must not be longer than 16 bytes) used to encrypt the password. If this parameter is not set, it indicates that the password in **plugin.conf** will not be encrypted, and you only need to specify the plaintext password in `password`. If this parameter is specified, it indicates that the password is encrypted by this key, and you need to specify the encrypted string in `password`. The encrypted password can be generated in StarRocks using the `AES_ENCRYPT` function: `SELECT TO_BASE64(AES_ENCRYPT('password','secret_key'));`.
    - `filter`: the filter conditions for audit log loading. This parameter is based on the [WHERE parameter](../../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#opt_properties)  in Stream Load, i.e. `-H “where: <condition>”`, defaults to an empty string. Example: `filter=isQuery=1 and clientIp like '127.0.0.1%' and user='root'`.

4. Zip the files back into a package.

    ```shell
    zip -q -m -r auditloader.zip auditloader.jar plugin.conf plugin.properties
    ```

5. Dispatch the package to all machines that host FE nodes. Make sure all packages are stored in an identical path. Otherwise, the installation fails. Remember to copy the absolute path to the package after you dispatched the package.

  > **NOTE**
  >
  > You can also distribute **auditloader.zip** to an HTTP service accessible to all FEs (for example, `httpd` or `nginx`) and install it using the network. Note that in both cases the **auditloader.zip** needs to be persisted in the path after the installation is performed, and the source files should not be deleted after installation.

## Install AuditLoader

Execute the following statement along with the path you copied to install AuditLoader as a plugin in StarRocks:

```SQL
INSTALL PLUGIN FROM "<absolute_path_to_package>";
```

Example of installation from a local package:

```SQL
INSTALL PLUGIN FROM "<absolute_path_to_package>";
```

If you want install the plugin via a network path, you need to provide the md5 of the package in the properties of the INSTALL statement.

Example:

```sql
INSTALL PLUGIN FROM "http://xx.xx.xxx.xxx/extra/auditloader.zip" PROPERTIES("md5sum" = "3975F7B880C9490FE95F42E2B2A28E2D");
```

See [INSTALL PLUGIN](../../sql-reference/sql-statements/cluster-management/plugin/INSTALL_PLUGIN.md) for detailed instructions.

## Verify the installation and query audit logs

1. You can check if the installation is successful via [SHOW PLUGINS](../../sql-reference/sql-statements/cluster-management/plugin/SHOW_PLUGINS.md).

    In the following example, the `Status` of the plugin `AuditLoader` is `INSTALLED`,  meaning installation is successful.

    ```Plain
    mysql> SHOW PLUGINS\G
    *************************** 1. row ***************************
        Name: __builtin_AuditLogBuilder
        Type: AUDIT
    Description: builtin audit logger
        Version: 0.12.0
    JavaVersion: 1.8.31
    ClassName: com.starrocks.qe.AuditLogBuilder
        SoName: NULL
        Sources: Builtin
        Status: INSTALLED
    Properties: {}
    *************************** 2. row ***************************
        Name: AuditLoader
        Type: AUDIT
    Description: Available for versions 3.3.11+. Load audit log to starrocks, and user can view the statistic of queries
        Version: 5.0.0
    JavaVersion: 11
    ClassName: com.starrocks.plugin.audit.AuditLoaderPlugin
        SoName: NULL
        Sources: /x/xx/xxx/xxxxx/auditloader.zip
        Status: INSTALLED
    Properties: {}
    2 rows in set (0.01 sec)
    ```

2. Execute some random SQLs to generate audit logs, and wait for 60 seconds (or the time you have specified in the item `max_batch_interval_sec` when you configure AuditLoader) to allow AuditLoader to load audit logs into StarRocks.

3. Check the audit logs by querying the table.

    ```SQL
    SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__;
    ```

    The following example shows when audit logs are loaded into the table successfully:

    ```Plain
    mysql> SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__\G
    *************************** 1. row ***************************
           queryId: 01975a33-4129-7520-97a2-05e641cec6c9
         timestamp: 2025-06-10 14:16:37
         queryType: query
          clientIp: xxx.xx.xxx.xx:65283
              user: root
    authorizedUser: 'root'@'%'
     resourceGroup: default_wg
           catalog: default_catalog
                db: 
             state: EOF
         errorCode:
         queryTime: 3
         scanBytes: 0
          scanRows: 0
        returnRows: 1
         cpuCostNs: 33711
      memCostBytes: 4200
            stmtId: 102
           isQuery: 1
              feIp: xxx.xx.xxx.xx
              stmt: SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__
            digest:
      planCpuCosts: 908
      planMemCosts: 0
     pendingTimeMs: -1
      candidateMvs: null
            hitMVs: null
    …………
    ```

## Troubleshooting

If no audit logs are loaded to the table after the dynamic partition is created and the plugin is installed, you can check whether **plugin.conf** is configured properly or not. To modify it, you must first uninstall the plugin:

```SQL
UNINSTALL PLUGIN AuditLoader;
```

Logs of AuditLoader are printed in **fe.log**, you can retrieve them by searching the keyword `audit` in **fe.log**. After all configurations are set correctly, you can follow the above steps to install AuditLoader again.
