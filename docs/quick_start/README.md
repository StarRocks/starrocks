# Quick Start

* FE: The StarRocks frontend node is responsible for metadata management, management of client connectors, query planning, query scheduling, and so on.
* BE: The StarRocks backend node is responsible for data storage, calculation execution, compaction, replication management, and so on.
* Broker: A transit service that connects external data such as HDFS and object storage, assisting import and export functions.
* Tablet: The logical sharding of a StarRocks table, as well as the basic unit of copy management. Each table is divided into multiple tablets and stored on different BE nodes according to the partitioning and bucketing mechanisms.
