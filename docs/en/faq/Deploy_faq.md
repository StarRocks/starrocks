---
displayed_sidebar: docs
---

# Deployment

This topic provides answers to some frequently asked questions about deployment.

## How do I bind a fixed IP address with the `priority_networks` parameter in the `fe.conf` file?

### Problem description

For example, if you have two IP addresses: 192.168.108.23 and 192.168.108.43. You might provide IP addresses as follows:

- If you specify the addresses as 192.168.108.23/24, StarRocks will recognize them as 192.168.108.43.
- If you specify the addresses as 192.168.108.23/32, StarRocks will recognize them as 127.0.0.1.

### Solution

There are the following two ways to solve this problem:

- Do not add "32" at the end of an IP address or change "32" to "28".
- You can also upgrade to StarRocks 2.1 or later.

## Why does the error "StarRocks BE http service did not start correctly, exiting" occur when I start a backend (BE) after installation?

When installing a BE, the system reports a startup error: StarRocks Be http service did not start correctly, exiting.

This error occurs because the web services port of the BE is occupied. Try to modify the ports in the `be.conf` file and restart the BE.

## What do I do when the error occurs: ERROR 1064 (HY000): Could not initialize class com.starrocks.rpc.BackendServiceProxy?

This error occurs when you run programs in Java Runtime Environment (JRE). To solve this problem, replace JRE with Java Development Kit (JDK). We recommend that you use Oracle's JDK 1.8 or later.

<!-- ## Why does the error "Failed to Distribute files to node" occur when I deploy StarRocks of Enterprise Edition and configure nodes?

This error occurs when Setuptools versions installed on multiple frontends (FEs) are inconsistent. To solve this problem, you can execute the following command as a root user.

```plaintext
yum remove python-setuptools

rm /usr/lib/python2.7/site-packages/setuptool* -rf

wget https://bootstrap.pypa.io/ez_setup.py -O - | python
```
-->

## Can FE and BE configuration items be modified and then take effect without restarting the cluster?

Yes. Perform the following steps to complete the modifications for an FE and a BE configuration item:

- FE: You can complete the modification for an FE in one of the following ways:
  - SQL

  ```plaintext
  ADMIN SET FRONTEND CONFIG ("key" = "value");
  ```

  Example:

  ```plaintext
  ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "false");
  ```

  - Shell

  ```plaintext
  curl --location-trusted -u username:password \
  http://<ip>:<fe_http_port/api/_set_config?key=value>
  ```

  Example:

  ```plaintext
  curl --location-trusted -u <username>:<password> \
  http://192.168.110.101:8030/api/_set_config?enable_statistic_collect=true
  ```

- BE: You can complete the modification for a BE in the following way:

```plaintext
curl -XPOST -u username:password \
http://<ip>:<be_http_port>/api/update_config?key=value
```

> Note: Make sure that the user has permission to log in remotely. If not, you can grant the permission to the user in the following way:

```plaintext
CREATE USER 'test'@'%' IDENTIFIED BY '123456';

GRANT SELECT ON . TO 'test'@'%';
```

## What do I do if the error "Failed to get scan range, no queryable replica found in tablet:xxxxx" occurs after I extend the BE disk space?

### Problem description

This error may occur during data loading into Primary Key tables. During data loading, the destination BE does not have enough disk space for the loaded data and the BE crashes. New disks are then added to extend the disk space. However, Primary Key tables do not support disk space re-balancing and the data cannot be offloaded to other disks.

### Solution

Patches to this bug (Primary Key tables do not support BE disk space re-balancing) is still under active development. Currently, you can fix it in either of the following two ways:

- Manually distribute data among disks. For example, copy the directory from the disk with a high space usage to a disk with a larger space.
- If the data on these disks is not important, we recommend you delete the disks and modify the disk path. If this error persists, use [TRUNCATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/TRUNCATE_TABLE.md) to clear data in the table to free up some space.

## Why does the error "Fe type:unknown ,is ready :false." occur when I start an FE during the cluster restart?

Check if the leader FE is running. If not, restart the FE nodes in your cluster one by one.

## Why does the error "failed to get service info err." occur when I deploy the cluster?

Check if OpenSSH Daemon (sshd) is enabled. If not, run the `/etc/init.d/sshd`` status` command to enable it.

## Why does the error "Fail to get master client from `cache. ``host= port=0 code=THRIFT_RPC_ERROR`" occur when I start a BE?

Run the `netstat -anp |grep port` command to check whether the ports in the `be.conf` file are occupied. If so, replace the occupied port with a free port and then restart the BE.

<!--

## Why does the error "Failed to transport upgrade files to agent host. src:…" occur when I upgrade a cluster of the Enterprise Edition?

This error occurs when the disk space specified in the deployment directory is insufficient. During the cluster upgrade, the StarRocks Manager distributes the binary file of the new version to each node. If the disk space specified in the deployment directory is insufficient, the file cannot be distributed to each node. To solve this problem, add data disks.

-->

## Why does the FE node log on the diagnostics page of StarRocks Manager display "Search log failed." for a newly deployed FE node that is running properly?

 By default, StarRocks Manager obtains the path configuration of the newly deployed FE within 30 seconds. This error occurs when the FE starts slowly or does not respond within 30 seconds due to other reasons. Check the log of Manager Web via the path:

`/starrocks-manager-xxx/center/log/webcenter/log/web/``drms.INFO`(you can customize the path). Then find that whether the message "Failed to update FE configurations" display in the log. If so, restart the corresponding FE to obtain the new path configuration.

## Why does the error "exceeds max permissable delta:5000ms." occur when I start an FE?

This error occurs when the time difference between two machines is more than 5s. To solve this problem, align the time of these two machines.

## How do I set the `storage_root_path` parameter if there are multiple disks in a BE for data storage?

Configure the `storage_root_path` parameter in the `be.conf` file and separate values of this parameter with `;`. For example: `storage_root_path=/the/path/to/storage1;/the/path/to/storage2;/the/path/to/storage3;`

## Why does the error "invalid cluster id: 209721925." occur after an FE is added to my cluster?

If you do not add the `--helper` option for this FE when starting your cluster for the first time, the metadata between two machines is inconsistent, thus this error occurs. To solve this problem, you need to   clear all metadata under the meta directory and then add an FE with the `--helper` option.

## Why Alive is `false` when an FE is running and prints log `transfer: follower`?

This issue occurs when more than half of memory of Java Virtual Machine (JVM) is used and no checkpoint is marked. In general, a checkpoint will be marked after the system accumulates 50,000 pieces of log. We recommend that you modify the JVM's parameters of each FE and restarting these FEs when they are not heavily loaded.
