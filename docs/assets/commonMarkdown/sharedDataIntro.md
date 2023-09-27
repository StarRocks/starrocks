This topic describes how to deploy and use a shared-data StarRocks cluster. This feature is supported from v3.0 for S3 compatible storage, and from 3.1 for Azure Blob Storage.

The shared-data StarRocks cluster is specifically engineered for the cloud on the premise of separation of storage and compute. It allows data to be stored in object storage (for example, AWS S3, Google GCS, Azure Blob Storage, and MinIO). You can achieve not only cheaper storage and better resource isolation, but elastic scalability for your cluster. The query performance of the shared-data StarRocks cluster aligns with that of a shared-nothing StarRocks cluster when the local disk cache is hit.

The StarRocks shared-data cluster is made up of Frontend Engines (FEs) and Compute Nodes (CNs). The CNs replace the classic Backend Engines (BEs) in shared-data clusters

Compared to the classic StarRocks architecture, separation of storage and compute offers a wide range of benefits. By decoupling these components, StarRocks provides:

- Inexpensive and seamlessly scalable storage.
- Elastic scalable compute. Because data is not stored in Compute Nodes (CNs), scaling can be done without data migration or shuffling across nodes.
- Local disk cache for hot data to boost query performance.
- Asynchronous data ingestion into object storage, allowing a significant improvement in loading performance.

