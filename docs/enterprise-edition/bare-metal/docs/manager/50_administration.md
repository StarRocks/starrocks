# Administration with CelerData Manager

import TimezoneError from '../_assets/commonMarkdown/_timezone.md

After logging in to CelerData Manager, you can view the status of your cluster on the **Dashboard** page, including **cluster basic information, Cluster overview,** **Query** **overview, and Alarm**.

## Cluster basic information

Displays **data information** and **node information**.

- **Data information** includes the total data volume, today's added data volume, and yesterday's added data volume.
- **Cluster node information**
  -  Leader FE IP and port (the port is the BE heartbeat port, such as `9212` in the following figure). The following figure shows a cluster with one FE node (leader FE) and three BE nodes.

## Cluster overview

The **Cluster overview** section displays general information of the cluster.

It displays QPS, average query response time, 99 percentile response, number of database connections, and volume of loaded data. You can also specify a time interval in the upper right corner.

### **Query** **QPS**

You can view the QPS to monitor cluster load.

**Average** **query** **response time**

By querying the average response time, you can observe the average query latency of the cluster in the current time period to determine whether the query latency meets expectation, and whether some SQL needs to be tuned.

Click **View more** to view the 50 quantile response time, 75 quantile response time, 90 quantile response time, 95 quantile response time, 99 quantile response time, and 999 quantile response time.

### Number of database connections

You can view the number of connections in the current cluster. The statistics are not the number of connected users. Connections from the same account to the cluster are also counted.

### Data ingestion volume

You can view the data loading frequency of the cluster. The default unit of the y-coordinate is KB/second.

Click **View More** at the bottom to view the number of load tasks initiated, number of loaded rows, and amount of loaded data.

## Query overview

Choose **Dashboard** > **Query** **overview** to view the query execution success rate, number of successful queries, and number of failed queries.

Click **View More** at the bottom to view **All Queries** and **Slow Queries**.

**All Queries** displays all the SQL statements that have been executed in the cluster, as well as the execution status, time consumption, and users who execute the SQL.

You can click a query ID to show SQL details, query plan, execution time, and execution details.

**Query** **plan** shows the result of the EXPLAIN statement.

**Query** **details** show the detailed profile of the query. If this tab is unclickable, query profile is not enabled. You can enable query profile using `set is_report_success = true` (for versions earlier than v2.5) or `set enable_profile = true` (for v2.5 and later) and later, and run EXPLAIN again.

## Alarm

> **To detect any exceptions and risks in your cluster in advance, you must configure alarms for your cluster. For more information, see the "Alarms and Diagnose" section.**

In the Alarm area, you can view the number of alarms triggered. Alarms are classified into three types in descending order of severity: Fatal, Warning, and Info.

Click **View more** to go to the **Alarms** page.

You can view alarm records, configure alarm rules, block nodes from reporting alarms, and configure alarm notification recipients when an alarm is triggered.

## Manage clusters and nodes

### Manage clusters

### Cluster performance

Click **Cluster** in the top navigation bar to display customer performance information, including CPU usage, memory usage, disk I/O usage, disk usage, free disk space, packet transmission rate, number of transmitted packets, packet receiving rate, and number of received packets.

You can adjust the slider, or adjust the time interval in the upper-right corner to observe this information at a specific time.

### Query monitoring

Click the **Query** **monitor** tab to display the query information of the cluster, including Query QPS, Query response time (AVG), 50 Quantile, 75 quantile, 90 quatinle, 95 quantile, 99 quantile, and 999 quantile.

### Data ingestion volume

Click the **Data Loaded** tab to display the query information of the cluster, including information such as the number of load tasks initiated, the number of rows loaded, and the size of the loaded data.

### Compaction

Click the **Compaction** tab to display the query information of the cluster, including baseline merged data versions, cumulative merged data versions, baseline merged data volume, and cumulative merged data volume.

### TopN scans

Click the **Scan** **TopN** tab to display the query information of the cluster, including the number of finished scans, the scanned data size, and the number of scanned rows.

### TopN loads

Click the **Load** **TopN** tab to display the load information of the cluster, including the number of finished load tasks, the size of data loaded, and the number of rows loaded.

You can click a table to query the real-time loading of a single table. Use starrocks_audit_tbl_ as an example.

## Manage nodes

### Machine information

1. Click **Nodes** in the top navigation bar to display the cluster information, including FE node information, BE node information, and Broker node information.
2. Click **Version Management** in the upper-right corner to switch cluster versions.
3. Click **Add** and enter the path of the target StarRocks version to add a version.

1. After the version is added, click **Switch**.

You can also start and stop a node, edit its configuration, or decommission the node.

### Metrics

Click the **Metrics** tab to display the monitoring information of the current cluster.

You can select metrics from the metrics list to monitor the cluster.

## Manage databases

### View database information

#### Catalogs

1. Click **Catalogs** in the top navigation bar to display catalog information, including catalog name, type, and comments.

1. Click a catalog to view its detailed information, including database name, size, and comments.
2. Click a database (for example, `jd`) and the **Database detail** page is displayed, which contains three tabs: **Tables**, **Tasks**, and **Statistics**.
3. Click a table to view the table information, for example, `site_access`.

Note: If a materialized view is built on this table, the materialized view will also be displayed when you click this table.

Detailed information includes partition name, visible data version, partition status, partition key, partition range, bucketing column, number of buckets, number of replicas, and data size of the partition.

- **Visible version**: the number of data versions of the partition. The number varies depending on the number of load tasks. More load tasks result in a larger value of this parameter.
- **State**: The partition status must be **NORMAL**.

1. Click a partition (`p20221206`) to view the partition information.

The lower level of **Partitions** is **Tablets.** This figure shows the number of tablets, data rows, data versions, and data size on a BE node.

**Data group**: The total number of data versions in the output of the SHOW PROC command.

1. Check details of tablets on a BE mode. Use BE node `10410` as an example.

This figure shows information such as the tablet ID, the BE node on which the tablet is located, the valid versions, the number of data rows, the data version, the data size, and the status of the tablet.

**Valid version**: The cumulative number of versions of the tablet. A large number of load tasks results in a large value of this metric.

**Data group**: The number of versions of the tablet. The higher the load frequency, the larger the value. The default limit is 1000. A value greater than 1000 will affect load performance.

### Task

Use database `jd` as an example. On the **Tasks** tab, you can view detailed information of load tasks in this database, including **Kafka** **Import,** **Other Import**, **Export**, **Schema Change**, **Creating View**, and **Clone**.

-  **Kafka** **Import,** **Other Import**
  -  You can filter tasks by label name, table name, and task status. You can perform the following operations on the target task: resume tasks in the PAUSED state, stop a RUNNING task, and drop a task. In addition, you can view other task information.

- **Export**
  -  Displays task status, progress, task type, start time, end time, and details.
- **Schema Change**
  -  Displays table name, task execution status, progress, start time, end time, timeout duration, and details.
-  **Creating View**
  -  Displays materialized view name, base table name, creation status, progress, start time, end time, timeout duration, and details.
- **Clone**
  -  Displays information of replica cloning tasks in the cluster, including two types of tasks (running tasks and pending tasks).
  -  Details include tablet ID, database, table, partition, and start time.

### Statistics

Click the **Statistics** tab to view information about tablets in the cluster, including tablets with inconsistent data and tablets with slow queries.

### Use database editor

Click **Editor** in the top navigation bar to write SQL. This eliminates the need to use the MySQL client to connect to CelerData. You can directly run SQL in the web UI.

- Write SQL

![img](./_assets/manager/manager-009.png)

- Select database
  -  Click the drop-down list in the left pane to select a database you want to query.

- Search for table
  -  You can enter a keyword in the search box to search for a table. In addition, you can click the **Table details** tab in the lower-right part to view the table structure and data in the table.
  ![img](./_assets/manager/manager-010.png)
- Tab
  -  You can click **+** in the top area to add your own tab and open a new editor page. This way, you can have a separate operation page.

Click **Saved tabs** to view all tabs and manage the tabs, including open, delete, search, and batch delete operations.

- Use the Query editor
  -  You can write SQL in the **Query** **editor** and then click **Run**. You can choose to execute a single query or execute SQL in batches. After execution, you can view the query history, query result, and table details.
  ![img](./_assets/manager/manager-011.png)
  - The **Query** **history** tab displays query ID, query start time, status, duration, and SQL.
  - The **Query** **result** tab displays query ID, execution time, execution status, number of returned rows, and total number of result rows.
  - The **Table details** tab displays column ID, column type, whether the column value can be nullable,  whether the table is a Primary Key table, the default value of the column, and comments. You can also query the table schema and view data in the table.
  - 
- Analyze
  -  This function is equivalent to `set is_report_success = true`, that is, enabling profile reporting to analyze the SQL.
  - 
- Clear: Clears the **Query** **editor** pane to write new SQL.

### Manage queries

Click **Queries** in the top navigation bar to view all the query records (a query that takes more than **5s** is regarded as a slow query).

#### All queries

The **All queries** tab displays the start time, ID, status, duration, query user, and SQL statement of a query. If you have enabled the query profile feature (`set enable_profile = true` for v2.5 and later and `set is_report_success = true` for versions earlier than v2.5), click the corresponding query ID to view the query execution plan and query profile.

- You can click a SQL statement to copy it.
- You can enter a keyword in the search box to filter queries.

- You can click the **All queries** drop-down list to filter queries.
- You can filter query records by time range.

The query records are displayed by page. At the bottom of the page, you can choose the number of entries to display on each page. You can view query records following the sequence of page numbers or select a target page number.

#### Slow queries

The **Slow queries** tab shows all the slow queries identified by the system. Similar to the **All queries** tab, you can filter queries using the search box, by execution status, or by specifying a time range.

## Alarms and Diagnose

### Alarms

#### Alarm records

Choose **Alarms** > **Alarm Record**. This tab displays historical alarm records. You can filter alarms based on time, alarm severity, and alarm status. You can also search for an alarm by entering a keyword in the search box.

#### Alarm rules

The **Alarm Rules** tab displays all the configured alarm rules. You can modify, delete, and disable an alarm metric on this page. You can also view the details of an alarm rule and historical alarms.

You can click **Create** in the upper-right corner to add an alarm rule.

- **Trigger period**: You can configure the time range during which the alarm is effective, for example, 08:00:00--18:00:00.
- **Alarm interval**: the alarm reporting interval. An alarm cannot be repetitively reported within the period specified by this parameter.
- **Nodes**: the node on which the alarm is triggered. For example, you can configure an FE node.
- **Metric:** The alarm metric can be searched.
- **Rule name:** Generally, the name is the same as the alarm metric. You can also customize the name.
- **Rule1**
  - Alarm interval: value + time unit, for example, 1 hour.
  - Trigger condition: The supported conditions are average value and value. The comparison operators are `>=`, `>`, `=`, `<`, `<=`. You need to set a value threshold. Example: Average value < 80%.
  - Alarm severity in ascending order
    - **Info:** Cluster load or other functions exceed the normal range and you need to pay attention to this.
    - **Warning**: Some functions of the cluster are unavailable. You need to pay attention and fix it.
    - **Fatal**: The cluster is unavailable. You need to check related information at the earliest time and communicate with the business team to identify issues.
  -  You can add an alarm rule by clicking the plus sign (+) to the right of each rule.
- **Remarks**: You can add a remark for the alarm rule to describe the meaning of the metric and the severity.

#### Block nodes

You can block alarms for specific nodes. This way, alarms related to this node will not be reported. This function avoids unnecessary alarm triggering when you perform node maintenance operations.

#### Alarm notification

Currently, CelerData supports alarm notifications via email and Webhook. You can choose any method that suits your needs.

##### Configure email

###### Configure the SMTP server

Versions later than v2.2 support online modification of SMTP server. You can click **Settings** under **root** to configure the mailbox and SMTP server.

For v2.2 and earlier versions, you need to modify the [notify] option in the **center_service.conf** configuration file of the **center/conf** directory of the CelerData Manager installation directory and restart the Center service.

```SQL
email_user = user@xxxx.com
email_password = xxxx
email_addr = smtp.xxxx.com:587
```

Restart the Center service.

```Shell
./centerctl.sh restart center-service
```

###### Configure mailbox

On the **Notify management tab**, click **Create** in the upper-right corner. In the **Create** dialog box, configure email.

##### Configure Webhooks

On the **Webhooks** tab, click **Create** in the upper-right corner.

You need to develop an interface to receive Webhook alarms from the server.

CelerData sends the following HTTP request to the configured URL:

```Plain
method:
POST

header:
x-starrocks-db-signature = [signature: hex_str(sha1(secret+post_body))]
content-type = [req_header.content-type]

body:
{
 "level":        "Alarm severity",
 "ruleName":     "Alarm rule name",
 "alarmMessage": "Alarm message",
 "startTime":    "Alarm start time",
}

Note
1. The result of x-starrocks-db-signature must be verified by the receiver. The calculation method is as follows:
Use sha1 to encode the string (Secret + Received post body) and convert it into a hex string.

Example for Golang:
hash := shal.New()
hash.Write ([]byte(secret))
hash.Write(body)
signBytes := hash.Sum(nil)
sign := fmt.Sprintf("&x", signBytes)

2. content-type can be application/json or application/x-www-form-urlencoded.
3. Secret
```

### Diagnose

#### Log

Choose **Diagnose** > **Log**. On the **Log** tab** you can view FE and BE logs. You can also search for a log in the search box or by specifying a time interval.

#### System Diagnose

You can click **Create System Diagnose** in the upper-right corner to collect the following information for troubleshooting:

- **Cluster basic info**: including host list, hardware information, and StarRocks version.
- **StarRocks Configurations**: including items in the `fe.conf` and `be.conf` configuration files and session variables.
- **StarRocks** **Log**: including FE and BE logs.
- **Hardware Test**: CPU, environment variables, Iperf network interface test results, maximum number of opened files (ulimit -n), configuration of `/proc/sys/vm/overcommit_memory`, and configuration of `/proc/sys/vm/swappiness`.
- **Slow queries**: slow queries within a specific period (4 days by default) and the profile.
- **System Metrics**: metrics related to memory, CPU, and io.util.
- **BE Memory Info**: See [Memory management](https://docs.starrocks.io/en-us/latest/administration/Memory_management).

#### Hardware Test

The Hardware test will check the CPU, memory, maximum number of opened files (ulimit -n), configuration of `/proc/sys/vm/overcommit_memory`, configuration of `/proc/sys/vm/swappiness`, Iperf network interface test results, environment variables, and disk random I/O test results of the selected node.
