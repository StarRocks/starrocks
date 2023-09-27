
**Before starting CNs**, add the following configuration items in the CN configuration file **cn.conf**:

```Properties
starlet_port = <starlet_port>
storage_root_path = <storage_root_path>
```

#### starlet_port

The CN heartbeat service port for the StarRocks shared-data cluster. Default value: `9070`.

#### storage_root_path

The storage volume directory that the local cached data depends on and the medium type of the storage. Multiple volumes are separated by semicolon (;). If the storage medium is SSD, add `,medium:ssd` at the end of the directory. If the storage medium is HDD, add `,medium:hdd` at the end of the directory. Example: `/data1,medium:hdd;/data2,medium:ssd`. Default value: `${STARROCKS_HOME}/storage`.

> **NOTE**
>
> The data is cached under the directory **<storage_root_path\>/starlet_cache**.

