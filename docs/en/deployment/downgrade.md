---
displayed_sidebar: docs
---

# Downgrade StarRocks

This topic describes how to downgrade your StarRocks cluster.

If an exception occurs after you upgrade a StarRocks cluster, you can downgrade it to the earlier version to quickly recover the cluster.

## Overview

Review the information in this section before downgrading. Perform any recommended actions.

### Downgrade paths

- **For patch version downgrade**

  You can downgrade your StarRocks cluster across patch versions, for example, from v3.5.11 directly to v3.5.6.

- **For minor version downgrade**

  For compatibility and safety reasons, we strongly recommend you downgrade your StarRocks cluster **consecutively from one minor version to another**. For example, to downgrade a StarRocks v3.5 cluster to v3.2, you need to downgrade it in the following order: v3.5.x --> v3.4.x --> v3.3.x --> v3.2.x.

- **For major version downgrade**

  You can only downgrade your StarRocks v4.1 cluster to v4.0.6 and later versions.

  :::warning

  **Downgrade Notes**

  - After upgrading StarRocks to v4.1, DO NOT downgrade to any v4.0 version below v4.0.6.

    Due to internal changes in data layout introduced in v4.1 (related to tablet splitting and distribution mechanisms), clusters upgraded to v4.1 may generate metadata and storage structures that are not fully compatible with earlier versions. As a result, downgrade from v4.1 is only supported to v4.0.6 or later. Downgrading to versions prior to v4.0.6 is not supported. This limitation is due to backward compatibility constraints in how earlier versions interpret tablet layout and distribution metadata.

  :::

### Downgrade procedure

StarRocks' downgrade procedure is the reverse order of the [upgrade procedure](../deployment/upgrade.md#upgrade-procedure). Therefore, you need to **downgrade** **FEs** **first and then BEs and CNs**. Downgrading them in the wrong order may lead to incompatibility between FEs and BEs/CNs, and thereby cause the service to crash. For FE nodes, you must first downgrade all Follower FE nodes before downgrading the Leader FE node.

## Before you begin

During preparation, you must perform the compatibility configuration if you are up for a minor or major version downgrade. You also need to perform the downgrade availability test on one of the FEs or BEs before downgrading all nodes in the cluster.

### Perform compatibility configuration

If you want to downgrade your StarRocks cluster to an earlier minor or major version, you must perform the compatibility configuration. In addition to the universal compatibility configuration, detailed configurations vary depending on the version of the StarRocks cluster you downgrade from.

- **Universal compatibility configuration**

Before downgrading your StarRocks cluster, you must disable tablet clone. You can skip this step if you have disabled the balancer.

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("disable_balance"="true");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="true");
```

After the downgrade, you can enable tablet clone again if the status of all BE nodes becomes `Alive`.

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "10000");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "500");
ADMIN SET FRONTEND CONFIG ("disable_balance"="false");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="false");
```

## Downgrade FE

:::note

To downgrade a cluster from v3.3.0 or later to v3.2, follow these steps before downgrading:

1. Ensure that all ALTER TABLE SCHEMA CHANGE transactions initiated in the v3.3 cluster are either completed or canceled before downgrading.
2. Clear all transaction history by executing the following command:

   ```SQL
   ADMIN SET FRONTEND CONFIG ("history_job_keep_max_second" = "0");
   ```

3. Verify that there are no remaining historical records by running the following command:

   ```SQL
   SHOW PROC '/jobs/<db>/schema_change';
   ```
:::

After the compatibility configuration and the availability test, you can downgrade the FE nodes. You must first downgrade the Follower FE nodes and then the Leader FE node.

1. Create a metadata snapshot.

   a. Run [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md) to create a metadata snapshot.

   b. You can check whether the image file has been synchronized by viewing the log file **fe.log** of the Leader FE. A record of log like "push image.* from subdir [] to other nodes. totally xx nodes, push successful xx nodes" suggests that the image file has been successfully synchronized. 

2. Navigate to the working directory of the FE node and stop the node.

   ```Bash
   # Replace <fe_dir> with the deployment directory of the FE node.
   cd <fe_dir>/fe
   ./bin/stop_fe.sh
   ```

3. Replace the original deployment files under **bin**, **lib**, and **spark-dpp** with the ones of the earlier version.

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   mv spark-dpp spark-dpp.bak
   cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
   cp -r /tmp/StarRocks-x.x.x/fe/bin  .
   cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
   ```

4. Start the FE node.

   ```Bash
   ./bin/start_fe.sh --daemon
   ```

5. Check if the FE node is started successfully.

   ```Bash
   ps aux | grep StarRocksFE
   ```

6. Repeat the above Step 2 to Step 5 to downgrade other Follower FE nodes, and finally the Leader FE node.

   > **CAUTION**
   >
   > Suppose you have downgraded your cluster after a failed upgrade and you want to upgrade the cluster again, for example, 3.5->4.0->3.5->4.0. To prevent metadata upgrade failure for some Follower FEs, repeat Step 1 to trigger a new snapshot before upgrading.

## Downgrade BE

Having downgraded the FE nodes, you can then downgrade the BE nodes in the cluster.

1. Navigate to the working directory of the BE node and stop the node.

   ```Bash
   # Replace <be_dir> with the deployment directory of the BE node.
   cd <be_dir>/be
   ./bin/stop_be.sh
   ```

2. Replace the original deployment files under **bin** and **lib** with the ones of the earlier version.

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   ```

3. Start the BE node.

   ```Bash
   ./bin/start_be.sh --daemon
   ```

4. Check if the BE node is started successfully.

   ```Bash
   ps aux | grep starrocks_be
   ```

5. Repeat the above procedures to downgrade other BE nodes.

## Downgrade CN

1. Navigate to the working directory of the CN node and stop the node gracefully.

   ```Bash
   # Replace <cn_dir> with the deployment directory of the CN node.
   cd <cn_dir>/be
   ./bin/stop_cn.sh --graceful
   ```

2. Replace the original deployment files under **bin** and **lib** with the ones of the earlier version.

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   ```

3. Start the CN node.

   ```Bash
   ./bin/start_cn.sh --daemon
   ```

4. Check if the CN node is started successfully.

   ```Bash
   ps aux | grep  starrocks_be
   ```

5. Repeat the above procedures to downgrade other CN nodes.
