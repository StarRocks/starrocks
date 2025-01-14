# Manage clusters

## Cluster performance

Click **Cluster** in the top navigation bar to display customer performance information, including CPU usage, memory usage, disk I/O usage, disk usage, free disk space, packet transmission rate, number of transmitted packets, packet receiving rate, and number of received packets.

![img](../_assets/manager/052.png)

You can adjust the slider, or adjust the time interval in the upper-right corner to observe this information at a specific time.

![img](../_assets/manager/055.png)

## Query monitoring

Click the **Query** **monitor** tab to display the query information of the cluster, including Query QPS, Query response time (AVG), 50 Quantile, 75 quantile, 90 quatinle, 95 quantile, 99 quantile, and 999 quantile.

![img](../_assets/manager/056.png)

## Data ingestion volume

Click the **Data Loaded** tab to display the query information of the cluster, including information such as the number of load tasks initiated, the number of rows loaded, and the size of the loaded data.

## Compaction

Click the **Compaction** tab to display the query information of the cluster, including baseline merged data versions, cumulative merged data versions, baseline merged data volume, and cumulative merged data volume.

![img](../_assets/manager/060.png)

## TopN scans

Click the **Scan TopN** tab to display the query information of the cluster, including the number of finished scans, the scanned data size, and the number of scanned rows.

![img](../_assets/manager/061.png)

## TopN loads

Click the **Load TopN** tab to display the load information of the cluster, including the number of finished load tasks, the size of data loaded, and the number of rows loaded.

![img](../_assets/manager/062.png)

You can click a table to query the real-time loading of a single table. Use starrocks_audit_tbl_ as an example.

