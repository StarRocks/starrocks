---
---

The usage of shared-data StarRocks clusters is also similar to that of a classic shared-nothing StarRocks cluster, except that the shared-data cluster uses storage volumes and cloud-native tables to store data in object storage.

### Create default storage volume

You can use the built-in storage volumes that StarRocks automatically creates, or you can manually create and set the default storage volume. This section describes how to manually create and set the default storage volume.

> **NOTE**
>
> If your shared-data StarRocks cluster is upgraded from v3.0, you do not need to define a default storage volume because StarRocks created one with the object storage-related properties you specified in the FE configuration file **fe.conf**. You can still create new storage volumes with other object storage resources and set the default storage volume differently.

To give your shared-data StarRocks cluster permission to store data in your object storage, you must reference a storage volume when you create databases or cloud-native tables. A storage volume consists of the properties and credential information of the remote data storage. If you have deployed a new shared-data StarRocks cluster and disallow StarRocks to create a built-in storage volume (by specifying `enable_load_volume_from_conf` as `false`), you must define a default storage volume before you can create databases and tables in the cluster.

