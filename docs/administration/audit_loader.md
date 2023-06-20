# Manage audit logs within StarRocks via Audit Loader

This topic describes how to manage StarRocks audit logs within a table via the plugin - Audit Loader.

StarRocks stores its audit logs in the local file **fe/log/fe.audit.log** rather than an internal database. The plugin Audit Loader allows you to manage audit logs directly within your cluster. Audit Loader reads logs from the file, and loads them into StarRocks via HTTP PUT.

## Create a table to store audit logs

Create a database and a table in your StarRocks cluster to store its audit logs. See [CREATE DATABASE](../sql-reference/sql-statements/data-definition/CREATE%20DATABASE.md) and [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE%20TABLE.md) for detailed instructions.

Because the fields of audit logs vary among different StarRocks versions, you must choose among the following examples to create a table that is compatible with your StarRocks.

> **CAUTION**
>
> DO NOT change the table schema in the examples, or the log loading will fail.

- StarRocks v2.4.0 and later minor versions:

```SQL
CREATE DATABASE starrocks_audit_db__;
CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__ (
  `queryId`        VARCHAR(48)            COMMENT "Unique query ID",
  `timestamp`      DATETIME     NOT NULL  COMMENT "Query start time",
  `clientIp`       VARCHAR(32)            COMMENT "Client IP address",
  `user`           VARCHAR(64)            COMMENT "User who initiates the query",
  `resourceGroup`  VARCHAR(64)            COMMENT "Resource group name",
  `db`             VARCHAR(96)            COMMENT "Database that the query scans",
  `state`          VARCHAR(8)             COMMENT "Query state (EOF, ERR, OK)",
  `errorCode`      VARCHAR(96)            COMMENT "Error code",
  `queryTime`      BIGINT                 COMMENT "Query latency in milliseconds",
  `scanBytes`      BIGINT                 COMMENT "Size of the scanned data in bytes",
  `scanRows`       BIGINT                 COMMENT "Row count of the scanned data",
  `returnRows`     BIGINT                 COMMENT "Row count of the result",
  `cpuCostNs`      BIGINT                 COMMENT "CPU resources consumption time for query in nanoseconds",
  `memCostBytes`   BIGINT                 COMMENT "Memory cost for query in bytes",
  `stmtId`         INT                    COMMENT "Incremental SQL statement ID",
  `isQuery`        TINYINT                COMMENT "If the SQL is a query (0 and 1)",
  `feIp`           VARCHAR(32)            COMMENT "IP address of FE that executes the SQL",
  `stmt`           STRING                 COMMENT "SQL statement",
  `digest`         VARCHAR(32)            COMMENT "SQL fingerprint",
  `planCpuCosts`   DOUBLE                 COMMENT "CPU resources consumption time for planning in nanoseconds",
  `planMemCosts`   DOUBLE                 COMMENT "Memory cost for planning in bytes"
) ENGINE = OLAP
DUPLICATE KEY (`queryId`, `timestamp`, `clientIp`)
COMMENT "Audit log table"
PARTITION BY RANGE (`timestamp`) ()
DISTRIBUTED BY HASH (`queryId`)
PROPERTIES (
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-30",
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.enable" = "true",
  "replication_num" = "3"
);
```

- StarRocks v2.3.0 and later minor versions:

```SQL
CREATE DATABASE starrocks_audit_db__;
CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__ (
  `queryId`        VARCHAR(48)            COMMENT "Unique query ID",
  `timestamp`      DATETIME     NOT NULL  COMMENT "Query start time",
  `clientIp`       VARCHAR(32)            COMMENT "Client IP address",
  `user`           VARCHAR(64)            COMMENT "User who initiates the query",
  `resourceGroup`  VARCHAR(64)            COMMENT "Resource group name",
  `db`             VARCHAR(96)            COMMENT "Database that the query scans",
  `state`          VARCHAR(8)             COMMENT "Query state (EOF, ERR, OK)",
  `errorCode`      VARCHAR(96)            COMMENT "Error code",
  `queryTime`      BIGINT                 COMMENT "Query latency in milliseconds",
  `scanBytes`      BIGINT                 COMMENT "Size of the scanned data in bytes",
  `scanRows`       BIGINT                 COMMENT "Row count of the scanned data",
  `returnRows`     BIGINT                 COMMENT "Row count of the result",
  `cpuCostNs`      BIGINT                 COMMENT "CPU resources consumption time for query in nanoseconds",
  `memCostBytes`   BIGINT                 COMMENT "Memory cost for query in bytes",
  `stmtId`         INT                    COMMENT "Incremental SQL statement ID",
  `isQuery`        TINYINT                COMMENT "If the SQL is a query (0 and 1)",
  `feIp`           VARCHAR(32)            COMMENT "IP address of FE that executes the SQL",
  `stmt`           STRING                 COMMENT "SQL statement",
  `digest`         VARCHAR(32)            COMMENT "SQL fingerprint",
  `planCpuCosts`   DOUBLE                 COMMENT "CPU resources consumption time for planning in nanoseconds",
  `planMemCosts`   DOUBLE                 COMMENT "Memory cost for planning in bytes"
) ENGINE = OLAP
DUPLICATE KEY (`queryId`, `timestamp`, `clientIp`)
COMMENT "Audit log table"
PARTITION BY RANGE (`timestamp`) ()
DISTRIBUTED BY HASH (`queryId`)
PROPERTIES (
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.start" = "-30",
  "dynamic_partition.end" = "3",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.enable" = "true",
  "replication_num" = "3"
);
```

- StarRocks v2.2.1 and later minor versions:

```SQL
CREATE DATABASE starrocks_audit_db__;
CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__
(
    query_id         VARCHAR(48)            COMMENT "Unique query ID",
    time             DATETIME     NOT NULL  COMMENT "Query start time",
    client_ip        VARCHAR(32)            COMMENT "Client IP address",
    user             VARCHAR(64)            COMMENT "User who initiates the query",
    db               VARCHAR(96)            COMMENT "Database that the query scans",
    state            VARCHAR(8)             COMMENT "Query state (EOF, ERR, OK)",
    query_time       BIGINT                 COMMENT "Query latency in milliseconds",
    scan_bytes       BIGINT                 COMMENT "Size of the scanned data in bytes",
    scan_rows        BIGINT                 COMMENT "Row count of the scanned data",
    return_rows      BIGINT                 COMMENT "Row count of the result",
    cpu_cost_ns      BIGINT                 COMMENT "CPU resources consumption time for query in nanoseconds",
    mem_cost_bytes   BIGINT                 COMMENT "Memory cost for query in bytes",
    stmt_id          INT                    COMMENT "Incremental SQL statement ID",
    is_query         TINYINT                COMMENT "If the SQL is a query (0 and 1)",
    frontend_ip      VARCHAR(32)            COMMENT "IP address of FE that executes the SQL",
    stmt             STRING                 COMMENT "SQL statement",
    digest           VARCHAR(32)            COMMENT "SQL fingerprint"
) engine=OLAP
duplicate key(query_id, time, client_ip)
partition by range(time) ()
distributed by hash(query_id)
properties(
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.enable" = "true",
    "replication_num" = "3"
);
```

- StarRocks v2.2.0, v2.1.0 and later minor versions:

```SQL
CREATE DATABASE starrocks_audit_db__;
CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__
(
    query_id        VARCHAR(48)           COMMENT "Unique query ID",
    time            DATETIME    NOT NULL  COMMENT "Query start time",
    client_ip       VARCHAR(32)           COMMENT "Client IP address",
    user            VARCHAR(64)           COMMENT "User who initiates the query",
    db              VARCHAR(96)           COMMENT "Database that the query scans",
    state           VARCHAR(8)            COMMENT "Query state (EOFE, RR, OK)",
    query_time      BIGINT                COMMENT "Query latency in milliseconds",
    scan_bytes      BIGINT                COMMENT "Size of the scanned data in bytes",
    scan_rows       BIGINT                COMMENT "Row count of the scanned data",
    return_rows     BIGINT                COMMENT "Row count of the result",
    stmt_id         INT                   COMMENT "Incremental SQL statement ID",
    is_query        TINYINT               COMMENT "If the SQL is a query (0 and 1)",
    frontend_ip     VARCHAR(32)           COMMENT "IP address of FE that executes the SQL",
    stmt            STRING                COMMENT "SQL statement",
    digest          VARCHAR(32)           COMMENT "SQL fingerprint"
) engine=OLAP
DUPLICATE KEY(query_id, time, client_ip)
PARTITION BY RANGE(time) ()
DISTRIBUTED BY HASH(query_id)
PROPERTIES(
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.enable" = "true",
    "replication_num" = "3"
);
```

- StarRocks v2.0.0 and later minor versions, StarRocks v1.19.0 and later minor versions:

```SQL
CREATE DATABASE starrocks_audit_db__;
CREATE TABLE starrocks_audit_db__.starrocks_audit_tbl__
(
    query_id        VARCHAR(48)           COMMENT "Unique query ID",
    time            DATETIME    NOT NULL  COMMENT "Query start time",
    client_ip       VARCHAR(32)           COMMENT "Client IP address",
    user            VARCHAR(64)           COMMENT "User who initiates the query",
    db              VARCHAR(96)           COMMENT "Database that the query scans",
    state           VARCHAR(8)            COMMENT "Query state (EOF, ERR, OK)",
    query_time      BIGINT                COMMENT "Query latency in milliseconds",
    scan_bytes      BIGINT                COMMENT "Size of the scanned data in bytes",
    scan_rows       BIGINT                COMMENT "Row count of the scanned data",
    return_rows     BIGINT                COMMENT "Row count of the result",
    stmt_id         INT                   COMMENT "Incremental SQL statement ID",
    is_query        TINYINT               COMMENT "If the SQL is a query (0 and 1)",
    frontend_ip     VARCHAR(32)           COMMENT "IP address of FE that executes the SQL",
    stmt            STRING                COMMENT "SQL statement"
) engine=OLAP
DUPLICATE KEY(query_id, time, client_ip)
PARTITION BY RANGE(time) ()
DISTRIBUTED BY HASH(query_id)
PROPERTIES(
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.enable" = "true",
    "replication_num" = "3"
);
```

`starrocks_audit_tbl__` is created with dynamic partitions. By default, the first dynamic partition is created 10 minutes after the table is created. Audit logs can then be loaded into the table. You can check the partitions in the table using the following statement:

```SQL
SHOW PARTITIONS FROM starrocks_audit_db__.starrocks_audit_tbl__;
```

After a partition is created, you can move on to the next step.

## Download and configure Audit Loader

1. [Download](https://releases.starrocks.io/resources/AuditLoader.zip) the Audit Loader installation package. The package contains multiple directories for different StarRocks versions. You must navigate to the corresponding directory and install the package that is compatible with your StarRocks.

    - **2.4**: StarRocks v2.4.0 and later minor versions
    - **2.3**: StarRocks v2.3.0 and later minor versions
    - **2.2.1+**: StarRocks v2.2.1 and later minor versions
    - **2.1-2.2.0**: StarRocks v2.2.0, StarRocks v2.1.0 and later minor versions
    - **1.18.2-2.0**: StarRocks v2.0.0 and later minor versions, StarRocks v1.19.0 and later minor versions

2. Unzip the installation package.

    ```shell
    unzip auditloader.zip
    ```

    The following files are inflated:

    - **auditloader.jar**: the JAR file of Audit Loader.
    - **plugin.properties**: the properties file of Audit Loader.
    - **plugin.conf**: the configuration file of Audit Loader.

3. Modify **plugin.conf** to configure Audit Loader. You must configure the following items to make sure Audit Loader can work properly:

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

## Install Audit Loader

Execute the following statement along with the path you copied to install Audit Loader as a plugin in StarRocks:

```SQL
INSTALL PLUGIN FROM "<absolute_path_to_package>";
```

See [INSTALL PLUGIN](../sql-reference/sql-statements/Administration/INSTALL%20PLUGIN.md) for detailed instructions.

## Verify the installation and query audit logs

1. You can check if the installation is successful via [SHOW PLUGINS](../sql-reference/sql-statements/Administration/SHOW%20PLUGINS.md).

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
    Description: load audit log to olap load, and user can view the statistic of queries
        Version: 1.0.1
    JavaVersion: 1.8.0
    ClassName: com.starrocks.plugin.audit.AuditLoaderPlugin
        SoName: NULL
        Sources: /x/xx/xxx/xxxxx/auditloader.zip
        Status: INSTALLED
    Properties: {}
    2 rows in set (0.01 sec)
    ```

2. Execute some random SQLs to generate audit logs, and wait for 60 seconds (or the time you have specified in the item `max_batch_interval_sec` when you configure Audit Loader) to allow Audit Loader to load audit logs into StarRocks.

3. Check the audit logs by querying the table.

    ```SQL
    SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__;
    ```

    The following example shows when audit logs are loaded into the table successfully:

    ```Plain
    mysql> SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__\G
    *************************** 1. row ***************************
        queryId: 082ddf02-6492-11ed-a071-6ae6b1db20eb
        timestamp: 2022-11-15 11:03:08
        clientIp: xxx.xx.xxx.xx:33544
            user: root
    resourceGroup: default_wg
                db: 
            state: EOF
        errorCode: 
        queryTime: 8
        scanBytes: 0
        scanRows: 0
        returnRows: 0
        cpuCostNs: 62380
    memCostBytes: 14504
            stmtId: 33
        isQuery: 1
            feIp: xxx.xx.xxx.xx
            stmt: SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__
            digest: 
    planCpuCosts: 21
    planMemCosts: 0
    1 row in set (0.01 sec)
    ```

## Troubleshooting

If no audit logs are loaded to the table after the dynamic partition is created and the plugin is installed, you can check whether **plugin.conf** is configured properly or not. To modify it, you must first uninstall the plugin:

```SQL
UNINSTALL PLUGIN AuditLoader;
```

After all configurations are set correctly, you can follow the above steps to install Audit Loader again.
