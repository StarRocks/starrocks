This topic describes how to deploy and use a shared-data StarRocks cluster. This feature is supported from v3.0.

The shared-data StarRocks cluster is specifically engineered for the cloud on the premise of separation of storage and compute. It allows data to be stored in object storage that is compatible with the S3 protocol (for example, AWS S3 and MinIO). You can achieve not only cheaper storage and better resource isolation, but elastic scalability for your cluster. The query performance of the shared-data StarRocks cluster aligns with that of a shared-nothing StarRocks cluster when the local disk cache is hit.

Compared to the classic StarRocks architecture, separation of storage and compute offers a wide range of benefits. By decoupling these components, StarRocks provides:

- Inexpensive and seamlessly scalable storage.
- Elastic scalable compute. Because data is not stored in CN nodes, scaling can be done without data migration or shuffling across nodes.
- Local disk cache for hot data to boost query performance.
- Asynchronous data ingestion into object storage, allowing a significant improvement in loading performance.

