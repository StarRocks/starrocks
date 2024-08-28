---
displayed_sidebar: docs
---

# Manage audit logs within StarRocks via AuditLoader

This topic describes how to manage StarRocks audit logs within a table via the plugin - AuditLoader.

StarRocks stores its audit logs in the local file **fe/log/fe.audit.log** rather than an internal database. The plugin AuditLoader allows you to manage audit logs directly within your cluster. Once installed, AuditLoader reads logs from the file, and loads them into StarRocks via HTTP PUT. You can then query the audit logs in StarRocks using SQL statements.

## Create a table to store audit logs

Create a database and a table in your StarRocks cluster to store its audit logs. See [CREATE DATABASE](../../sql-reference/sql-statements/Database/CREATE_DATABASE.md) and [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) for detailed instructions.

Because the fields of audit logs vary among different StarRocks versions, you must choose among the following examples to create a table that is compatible with your StarRocks.

> **CAUTION**
>
> - DO NOT change the table schema in the examples, or the log loading will fail.
> - Because the fields of audit logs vary among different StarRocks versions, the new version AuditLoader collects the common fields among them from all available StarRocks versions.

```SQL
CREATE DATABASE starrocks_audit_db__;

CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__ (
  `queryId`           VARCHAR(64)                COMMENT "Unique query ID",
  `timestamp`         DATETIME         NOT NULL  COMMENT "Query start time",
  `queryType`         VARCHAR(12)                COMMENT "Query type (query, slow_query, connection）",
  `clientIp`          VARCHAR(32)                COMMENT "Client IP address",
  `user`              VARCHAR(64)                COMMENT "User who initiates the query",
  `authorizedUser`    VARCHAR(64)                COMMENT "user_identity",
  `resourceGroup`     VARCHAR(64)                COMMENT "Resource group name",
  `catalog`           VARCHAR(32)                COMMENT "Catalog name",
  `db`                VARCHAR(96)                COMMENT "Database that the query scans",
  `state`             VARCHAR(8)                 COMMENT "Query state (EOF, ERR, OK)",
  `errorCode`         VARCHAR(512)               COMMENT "Error code",
  `queryTime`         BIGINT                     COMMENT "Query latency in milliseconds",
  `scanBytes`         BIGINT                     COMMENT "Size of the scanned data in bytes",
  `scanRows`          BIGINT                     COMMENT "Row count of the scanned data",
  `returnRows`        BIGINT                     COMMENT "Row count of the result",
  `cpuCostNs`         BIGINT                     COMMENT "CPU resources consumption time for query in nanoseconds",
  `memCostBytes`      BIGINT                     COMMENT "Memory cost for query in bytes",
  `stmtId`            INT                        COMMENT "Incremental SQL statement ID",
  `isQuery`           TINYINT                    COMMENT "If the SQL is a query (0 and 1)",
  `feIp`              VARCHAR(128)               COMMENT "IP address of FE that executes the SQL",
  `stmt`              VARCHAR(1048576)           COMMENT "Original SQL statement",
  `digest`            VARCHAR(32)                COMMENT "Slow SQL fingerprint",
  `planCpuCosts`      DOUBLE                     COMMENT "CPU resources consumption time for planning in nanoseconds",
  `planMemCosts`      DOUBLE                     COMMENT "Memory cost for planning in bytes"
) ENGINE = OLAP
DUPLICATE KEY (`queryId`, `timestamp`, `queryType`)
COMMENT "Audit log table"
PARTITION BY RANGE (`timestamp`) ()
DISTRIBUTED BY HASH (`queryId`) BUCKETS 3 
PROPERTIES (
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-30",       -- Keep the audit logs from the latest 30 days. You can adjust this value based on you business demand.
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.buckets" = "3",
  "dynamic_partition.enable" = "true",
  "replication_num" = "3"                 -- Keep three replicas of audit logs. It is recommended to keep three replicas in a production environment.
);
```

`starrocks_audit_tbl__` is created with dynamic partitions. By default, the first dynamic partition is created 10 minutes after the table is created. Audit logs can then be loaded into the table. You can check the partitions in the table using the following statement:

```SQL
SHOW PARTITIONS FROM starrocks_audit_db__.starrocks_audit_tbl__;
```

After a partition is created, you can move on to the next step.

## Download and configure AuditLoader

1. [Download](https://releases.starrocks.io/resources/AuditLoader.zip) the AuditLoader installation package. The package is compatible with all available versions of StarRocks.

2. Unzip the installation package.

    ```shell
    unzip auditloader.zip
    ```

    The following files are inflated:

    - **auditloader.jar**: the JAR file of AuditLoader.
    - **plugin.properties**: the properties file of AuditLoader. You do not need to modify this file.
    - **plugin.conf**: the configuration file of AuditLoader. In most cases, you only need to modify the `user` and `password` fields in the file.

3. Modify **plugin.conf** to configure AuditLoader. You must configure the following items to make sure AuditLoader can work properly:

    - `frontend_host_port`: FE IP address and HTTP port, in the format `<fe_ip>:<fe_http_port>`. The default value is `127.0.0.1:8030`.
    - `database`: name of the database you created to host audit logs.
    - `table`: name of the table you created to host audit logs.
    - `user`: your cluster username. You MUST have the privilege to load data (LOAD_PRIV) into the table.
    - `password`: your user password.

4. Zip the files back into a package.

    ```shell
    zip -q -m -r auditloader.zip auditloader.jar plugin.conf plugin.properties
    ```

5. Dispatch the package to all machines that host FE nodes. Make sure all packages are stored in an identical path. Otherwise, the installation fails. Remember to copy the absolute path to the package after you dispatched the package.

## Install AuditLoader

Execute the following statement along with the path you copied to install AuditLoader as a plugin in StarRocks:

```SQL
INSTALL PLUGIN FROM "<absolute_path_to_package>";
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
    Description: Available for versions 2.3+. Load audit log to starrocks, and user can view the statistic of queries.
        Version: 4.0.0
    JavaVersion: 1.8.0
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
         queryId: 84a69010-d47e-11ee-9647-024228044898
       timestamp: 2024-02-26 16:10:35
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
    planCpuCosts: 0
    planMemCosts: 0
    1 row in set (0.01 sec)
    ```

## Troubleshooting

If no audit logs are loaded to the table after the dynamic partition is created and the plugin is installed, you can check whether **plugin.conf** is configured properly or not. To modify it, you must first uninstall the plugin:

```SQL
UNINSTALL PLUGIN AuditLoader;
```

After all configurations are set correctly, you can follow the above steps to install AuditLoader again.
