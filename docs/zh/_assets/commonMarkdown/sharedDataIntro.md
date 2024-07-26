本文介绍如何部署和使用 StarRocks 存算分离集群。该功能从 3.0 版本开始支持。

> **说明**
>
> - StarRocks v3.1 版本对存算分离部署和配置进行了一些更改。如果您正在运行 v3.1 版本或更高版本，请使用本文档。
> - 如果您需要部署 v3.0 版本，请使用 [v3.0 文档](https://docs.starrocks.io/zh/docs/3.0/deployment/deploy_shared_data/)。
> - StarRocks 存算分离集群不支持数据备份和恢复。

StarRocks 存算分离集群采用了存储计算分离架构，特别为云存储设计。在存算分离的模式下，StarRocks 将数据存储在对象存储（例如 AWS S3、GCS、OSS、Azure Blob 以及 MinIO）或 HDFS 中，而本地盘作为热数据缓存，用以加速查询。通过存储计算分离架构，您可以降低存储成本并且优化资源隔离。除此之外，集群的弹性扩展能力也得以加强。在查询命中缓存的情况下，存算分离集群的查询性能与存算一体集群性能一致。

在 v3.1 版本及更高版本中，StarRocks 存算分离集群由 FE 和 CN 组成。CN 取代了存算一体集群中的 BE。

相对存算一体架构，StarRocks 的存储计算分离架构提供以下优势：

- 廉价且可无缝扩展的存储。
- 弹性可扩展的计算能力。由于数据不存储在 CN 节点中，因此集群无需进行跨节点数据迁移或 Shuffle 即可完成扩缩容。
- 热数据的本地磁盘缓存，用以提高查询性能。
- 可选异步导入数据至对象存储，提高导入效率。