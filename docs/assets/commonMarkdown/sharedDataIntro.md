This topic describes how to deploy and use a shared-data StarRocks cluster. This feature is supported from v3.0 for S3 compatible storage and v3.1 for Azure Blob Storage. Version 3.1 brings some changes to the shared-data deployment and configuration. Please use this document if you are running version 3.1 or higher, and refer to the
[3.0 documentation](https://docs.starrocks.io/en-us/3.0/deployment/deploy_shared_data) if you
are using version 3.0.

The shared-data StarRocks cluster is specifically engineered for the cloud on the premise of separation of storage and compute. It allows data to be stored in object storage (for example, AWS S3, Google GCS, Azure Blob Storage, and MinIO). You can achieve not only cheaper storage and better resource isolation, but elastic scalability for your cluster. The query performance of the shared-data StarRocks cluster aligns with that of a shared-nothing StarRocks cluster when the local disk cache is hit.

In version 3.1 and higher the StarRocks shared-data cluster is made up of Frontend Engines (FEs) and Compute Nodes (CNs). The CNs replace the classic Backend Engines (BEs) in shared-data clusters.

> **NOTE**
>
> Shared-data configuration was introduced in version 3.0 using the traditional BEs. In the configuration details below where there are differences in property names or defaults these are noted.

Compared to the classic StarRocks architecture, separation of storage and compute offers a wide range of benefits. By decoupling these components, StarRocks provides:

- Inexpensive and seamlessly scalable storage.
- Elastic scalable compute. Because data is not stored in Compute Nodes (CNs), scaling can be done without data migration or shuffling across nodes.
- Local disk cache for hot data to boost query performance.
- Asynchronous data ingestion into object storage, allowing a significant improvement in loading performance.

