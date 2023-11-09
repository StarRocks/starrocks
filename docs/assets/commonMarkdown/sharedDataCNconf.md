---
displayed_sidebar: "English"
---


**Before starting CNs**, add the following configuration items in the CN configuration file **cn.conf**:

```Properties
starlet_port = <starlet_port>
storage_root_path = <storage_root_path>
```

#### starlet_port

The CN heartbeat service port for the StarRocks shared-data cluster. Default value: `9070`.

#### storage_root_path

The storage volume directory that the local cached data depends on and the medium type of the storage. Multiple volumes are separated by semicolon (;). If the storage medium is SSD, add `,medium:ssd` at the end of the directory. If the storage medium is HDD, add `,medium:hdd` at the end of the directory. Example: `/data1,medium:hdd;/data2,medium:ssd`.

The default value for `storage_root_path` is `${STARROCKS_HOME}/storage`.

Local cache is effective when queries are frequent and the data being queried is recent, but there are cases that you may wish to turn off the local cache completely.

- In a Kubernetes environment with CN pods that scale up and down in number on demand, the pods may not have storage volumes attached.
- When the data being queried is in a data lake in remote storage and most of it is archive (old) data. If the queries are infrequent the data cache will have a low hit ratio and the benefit may not be worth having the cache.

To turn off the data cache set:

```Properties
storage_root_path =
```

> **NOTE**
>
> The data is cached under the directory **`<storage_root_path>/starlet_cache`**.
