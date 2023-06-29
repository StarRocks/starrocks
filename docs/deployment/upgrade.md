# Upgrade StarRocks

This topic describes how to upgrade your StarRocks cluster.

## Overview

Review the information in this section before upgrading, and perform any recommended actions.

### StarRocks versions

The version of StarRocks is represented by three numbers in the form **Major.Minor.Patch**, for example, `2.5.4`. The first number represents the major version of StarRocks, the second number represents the minor version, and the third number represents the patch version. StarRocks provides Long-term Support (LTS) for certain versions. Their support duration lasts more than half a year.

| **StarRocks version** | **Is the LTS version** |
| --------------------- | ---------------------- |
| v2.0.x                | No                     |
| v2.1.x                | No                     |
| v2.2.x                | No                     |
| v2.3.x                | No                     |
| v2.4.x                | No                     |
| v2.5.x                | Yes                    |
| v3.0.x                | No                     |

### Upgrade paths

- **For patch version upgrade**

  You can upgrade your StarRocks cluster across patch versions, for example, from v2.2.6 directly to v2.2.11.

- **For minor version upgrade**

  From StarRocks v2.0 onwards, you can upgrade a StarRocks cluster across minor versions, for example, from v2.2.x directly to v2.5.x. However, for compatibility and safety reasons, we strongly recommend you upgrade your StarRocks cluster **consecutively from one minor version to another**. For example, to upgrade a StarRocks v2.2 cluster to v2.5, you need to upgrade it in the following order: v2.2.x --> v2.3.x --> v2.4.x --> v2.5.x.

- **For major version upgrade**

  To upgrade your StarRocks cluster to v3.0, you must first upgrade it to v2.5.

### Upgrade procedure

StarRocks supports **rolling upgrades**, which allow you to upgrade your cluster without stopping the service. By design, BEs and CNs are backward compatible with the FEs. Therefore, you need to **upgrade BEs and CNs first and then FEs** to allow your cluster to run properly while being upgraded. Upgrading them in an inverted order may lead to incompatibility between FEs and BEs/CNs, and thereby cause the service to crash. For FE nodes, you must first upgrade all Follower FE nodes before upgrading the Leader FE node.

## Before you begin

During preparation, you must perform the compatibility configuration if you are up for a minor or major version upgrade. You also need to perform the upgrade availability test on one of the FEs and BEs before upgrading all nodes in the cluster.

### Perform compatibility configuration

If you want to upgrade your StarRocks cluster to a later minor or major version, you must perform the compatibility configuration. In addition to the universal compatibility configuration, detailed configurations vary depending on the version of the StarRocks cluster you upgrade from.

- **Universal compatibility configuration**

Before upgrading your StarRocks cluster, you must disable tablet clone.

```SQL
ADMIN SET FRONTEND CONFIG ("max_scheduling_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("disable_balance"="true");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="true");
```

After the upgrade, and the status of all BE nodes is `Alive`, you can re-enable tablet clone.

```SQL
ADMIN SET FRONTEND CONFIG ("max_scheduling_tablets" = "2000");
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets" = "100");
ADMIN SET FRONTEND CONFIG ("disable_balance"="false");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="false");
```

- **If you upgrade from v2.0 to later versions**

Before upgrading your StarRocks v2.0 cluster, you must set the following BE configuration and system variable.

1. If you have modified the BE configuration item `vector_chunk_size`, you must set it to `4096` before upgrading. Because it is a static parameter, you must modify it in the BE configuration file **be.conf** and restart the node to allow the modification to take effect.
2. Set the system variable `batch_size` to less than or equal to `4096` globally.

   ```SQL
   SET GLOBAL batch_size = 4096;
   ```

### Perform upgrade availability test

Before upgrading all nodes in your cluster in production, you are strongly advised to perform an upgrade availability test on one of the BE and FE nodes to see if the upgrade affects your current data.

#### BE/CN upgrade availability test

> **CAUTION**
>
> The BE upgrade availability test causes the loss of a data replica. Make sure you have at least three complete data replicas before performing the test.

Follow these steps to perform the BE/CN upgrade availability test:

1. Choose a random BE/CN node, navigate to its working directory, and stop it.

   - BE node:

     ```Bash
     # Replace <be_dir> with the deployment directory of the BE node.
     cd <be_dir>/be
     ./bin/stop_be.sh
     ```

   - CN node:

     ```Bash
     # Replace <cn_dir> with the deployment directory of the CN node.
     cd <cn_dir>/be
     ./bin/stop_cn.sh
     ```

2. Replace the original deployment files under **bin** and **lib** with the ones of the new version.

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   cp -r /tmp/StarRocks-x.x.x/be/lib  .
   cp -r /tmp/StarRocks-x.x.x/be/bin  .
   ```

3. Start the BE/CN node.

   - BE node:

     ```Bash
     sh bin/start_be.sh --daemon
     ```

   - CN node:

     ```Bash
     sh bin/start_cn.sh --daemon
     ```

4. Check if it is started successfully.

   ```Bash
   ps aux | grep starrocks_be
   ```

   - If the BE/CN node starts successfully, it is safe to upgrade the other BE/CN nodes.
   - If the BE/CN node fails to start, you must check the cause of the failure in the log files and solve the problem. If the problem is irresolvable, you can drop this BE/CN node, clean the data, restart the BE/CN node with deployment files of the previous version, and add the BE/CN node back to the cluster.

#### FE upgrade availability test

Follow these steps to perform the FE upgrade availability test:

1. In your development environment, deploy a test FE node of the new version. See [Deploy StarRocks - Start the Leader FE node](../deployment/deploy_manually.md#step-1-start-the-leader-fe-node) for detailed instructions.
2. Modify the FE configuration file **fe.conf** of the test FE node:

   - Assign different `http_port`, `rpc_port`, `query_port`, and `edit_log_port` from those of the production cluster.
   - Add `cluster_id = 123456`.
   - Add `metadata_failure_recovery = true`.

3. Copy the **meta** directory of the Leader FE node of the production cluster and paste it into the deployment directory of the test FE node.
4. Modify **meta/image/VERSION** of the test FE node. Set `cluster_id` to `123456`.
5. Start the test FE node.

   ```Bash
   sh bin/start_fe.sh --daemon
   ```

6. Check if it is started successfully.

   ```Bash
   ps aux | grep StarRocksFE
   ```

   - If the test FE node starts successfully, it is safe to upgrade the FE nodes in the production environment.
   - If the test FE node fails to start, you must check the cause of the failure in the FE log file **fe.log** and solve the problem. If the problem is irresolvable, you can simply remove this FE node.

## Upgrade BE

Having passed the upgrade availability test, you can first upgrade the BE nodes in the cluster.

1. Navigate to the working directory of the BE node and stop the node.

   ```Bash
   # Replace <be_dir> with the deployment directory of the BE node.
   cd <be_dir>/be
   ./bin/stop_be.sh
   ```

2. Replace the original deployment files under **bin** and **lib** with the ones of the new version.

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

5. Repeat the above procedures to upgrade other BE nodes.

## Upgrade CN

1. Navigate to the working directory of the CN node and stop the node gracefully.

   ```Bash
   # Replace <cn_dir> with the deployment directory of the CN node.
   cd <cn_dir>/be
   ./bin/stop_cn.sh --graceful
   ```

2. Replace the original deployment files under **bin** and **lib** with the ones of the new version.

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
   ps aux | grep starrocks_be
   ```

5. Repeat the above procedures to upgrade other CN nodes.

## Upgrade FE

After upgrading all BE and CN nodes, you can then upgrade the FE nodes. You must first upgrade the Follower FE nodes and then the Leader FE node.

1. Navigate to the working directory of the FE node and stop the node.

   ```Bash
   # Replace <fe_dir> with the deployment directory of the FE node.
   cd <fe_dir>/fe
   ./bin/stop_fe.sh
   ```

2. Replace the original deployment files under **bin**, **lib**, and **spark-dpp** with the ones of the new version.

   ```Bash
   mv lib lib.bak 
   mv bin bin.bak
   mv spark-dpp spark-dpp.bak
   cp -r /tmp/StarRocks-x.x.x/fe/lib  .   
   cp -r /tmp/StarRocks-x.x.x/fe/bin  .
   cp -r /tmp/StarRocks-x.x.x/fe/spark-dpp  .
   ```

3. Start the FE node.

   ```Bash
   sh bin/start_fe.sh --daemon
   ```

4. Check if the FE node is started successfully.

   ```Bash
   ps aux | grep StarRocksFE
   ```

5. Repeat the above procedures to upgrade other Follower FE nodes, and finally the Leader FE node.

   > **CAUTION**
   >
   > If you have downgraded your StarRocks cluster after upgrading it from v2.5 to v3.0, and again you upgrade it to v3.0, you must follow these steps in order to avoid metadata upgrade failure for some Follower FEs:
   >
   > 1. Run [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/Administration/ALTER%20SYSTEM.md) to create a new image.
   > 2. Wait for the new image to be synchronized to all Follower FEs.
   >
   > You can check whether the image file has been synchronized by viewing the log file **fe.log** of the Leader FE. A record of log like "push image.* from subdir [] to other nodes. totally xx nodes, push successful xx nodes" suggests that the image file has been successfully synchronized.
