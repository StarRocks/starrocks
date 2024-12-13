
**在启动 CN 之前**，在 CN 配置文件 **cn.conf** 中添加以下配置项：

```Properties
starlet_port = <starlet_port>
storage_root_path = <storage_root_path>
```

#### starlet_port

存算分离模式下，用于 CN 心跳服务的端口。默认值：`9070`。

#### storage_root_path

本地缓存数据依赖的存储目录，多块盘配置使用分号（;）隔开。例如：`/data1;/data2`。默认值：`${STARROCKS_HOME}/storage`。

本地缓存在查询频率较高且被查询的数据为最新数据的情况下非常有效，但以下情况下您可以关闭本地缓存。

- 在一个具有按需缩放的 CN pod 的 Kubernetes 环境中，pod 可能没有附加存储卷。
- 当查询的数据大部分是位于远程存储中的旧数据时，如果查询不频繁，缓存数据的命中率可能很低，此时开启本地缓存并不能显著提升查询性能。

如需关闭本地数据缓存：

```Properties
storage_root_path =
```

> **说明**
>
> 本地缓存数据将存储在 **`<storage_root_path>/starlet_cache`** 路径下。
