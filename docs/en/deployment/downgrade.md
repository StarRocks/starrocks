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

  You can downgrade your StarRocks cluster across patch versions, for example, from v2.2.11 directly to v2.2.6.

- **For minor version downgrade**

  For compatibility and safety reasons, we strongly recommend you downgrade your StarRocks cluster **consecutively from one minor version to another**. For example, to downgrade a StarRocks v2.5 cluster to v2.2, you need to downgrade it in the following order: v2.5.x --> v2.4.x --> v2.3.x --> v2.2.x.

  :::warning

  After upgrading StarRocks to v3.3, DO NOT downgrade it directly to v3.2.0, v3.2.1, or v3.2.2, otherwise it will cause metadata loss. You must downgrade the cluster to v3.2.3 or later to prevent the issue.

  :::

- **For major version downgrade**

  You can only downgrade your StarRocks v3.0 cluster to v2.5.3 and later versions.

  - StarRocks upgrades the BDB library in v3.0. However, BDBJE cannot be rolled back. You must use BDB library of v3.0 after a downgrade.
  - The new RBAC privilege system is used by default after you upgrade to v3.0. You can only use the RBAC privilege system after a downgrade.

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

- **If you downgrade from v2.2 and later versions**

Set the FE configuration item `ignore_unknown_log_id` to `true`. Because it is a static parameter, you must modify it in the FE configuration file **fe.conf** and restart the node to allow the modification to take effect. After the downgrade and the first checkpoint are completed, you can reset it to `false` and restart the node.

- **If you have enabled FQDN access**

If you have enabled FQDN access (supported from v2.4 onwards) and need to downgrade to versions earlier than v2.4, you must switch to IP address access before downgrading. See [Rollback FQDN](../administration/management/enable_fqdn.md#rollback) for detailed instructions.

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

   a. Run [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md) to create a meatedata snapshot.

   b. You can check whether the image file has been synchronized by viewing the log file **fe.log** of the Leader FE. A record of log like "push image.* from subdir [] to other nodes. totally xx nodes, push successful xx nodes" suggests that the image file has been successfully synchronized. 

   > **CAUTION**
   >
   > The ALTER SYSTEM CREATE IMAGE statement is supported in v2.5.3 and later. In earlier versions, you need to create a meatadata snapshot by restarting the Leader FE.

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

   > **CAUTION**
   >
   > If you are downgrading StarRocks v3.0 to v2.5, you must follow these steps after you replace the deployment files:
   >
   > 1. Copy the file **fe/lib/starrocks-bdb-je-18.3.13.jar** of the v3.0 deployment to the directory **fe/lib** of the v2.5 deployment.
   > 2. Delete the file **fe/lib/je-7.\*.jar**.

4. Start the FE node.

   ```Bash
   sh bin/start_fe.sh --daemon
   ```

5. Check if the FE node is started successfully.

   ```Bash
   ps aux | grep StarRocksFE
   ```

6. Repeat the above Step 2 to Step 5 to downgrade other Follower FE nodes, and finally the Leader FE node.

   > **CAUTION**
   >
   > Suppose you have downgraded your cluster after a failed upgrade and you want to upgrade the cluster again, for example, 2.5->3.0->2.5->3.0. To prevent metadata upgrade failure for some Follower FEs, repeat Step 1 to trigger a new snapshot before upgrading.

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
   sh bin/start_be.sh --daemon
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
   sh bin/start_cn.sh --daemon
   ```

4. Check if the CN node is started successfully.

   ```Bash
   ps aux | grep  starrocks_be
   ```

5. Repeat the above procedures to downgrade other CN nodes.
