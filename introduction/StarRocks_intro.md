# Introduction

## What is StarRocks

* StarRocks is a high-performance, MySQL-compatible, distributed relational columnar database. It has been tested and modernized by the industry for multiple data analysis scenarios.

* StarRocks takes advantage of the relational Online Analytical Processing (OLAP) database and distributed storage system. Through architectural upgrades and functional optimization, StarRocks has developed into an enterprise-level product.

* StarRocks is committed to accommodating multiple data analysis scenarios for enterprise users. It supports multiple data warehouse schemas(flat tables, pre-aggregations, star or snowflake schema), multiple data import methods (batch and streaming) and allows direct access to data from Hive, MySQL and Elasticsearch without importing.

* StarRocks is compatible with the MySQL protocol. Users can use the MySQL client and common Business Intelligence (BI) tools to connect to StarRocks for data analysis.

* StarRocks uses a distributed architecture to divide the table horizontally and store it in multiple replications. The clusters are highly scalable and therefore support 1) 10PB-level data analysis, 2) Massively Parallel Processing (MPP), and 3) data replication and elastic fault tolerance.

* Leveraging a relational model, strong data typing, and a columnar storage engine, StarRocks reduces read-write amplification through encoding and compression techniques. Using vectorized query execution, it fully unleashes the power of parallel computing on multi-core CPUs, therefore significantly improves query performance.

## Main features

The architectural design of StarRocks integrates the MPP database and the design ideas of distributed systems, and has the following advantages:

### Simple architecture

StarRocks does not rely on any external systems. The simple architecture makes it easy to deploy, maintain and scale out.

### Native vectorized SQL engine

StarRocks adopts vectorization technology to make full use of the parallel computing power of CPU, achieving sub-second query returns in multi-dimensional analyses. Administrators only need to focus on the StarRocks system itself, without having to learn and manage other external systems.

### Query optimization

StarRocks can optimize complex queries through CBO (Cost Based Optimizer). With a better execution plan, the data analysis efficiency will be greatly improved.

### Query federation

StarRocks allows direct access to data from Hive, MySQL and Elasticsearch without importing.

### Efficiently update

The updated model of StarRocks can perform upsert/delete operations according to the primary key, and achieve efficient query while concurrent updates.

### Intelligent materialized view

StarRocks supports intelligent materialized views. Users can create materialized views and generate pre-aggregated tables to speed up aggregate queries. StarRocks's materialized view automatically runs the aggregation when data is imported, keeping it consistent with the original table. When querying, users do not need to specify a materialized view, StarRocks can automatically select the best-materialized view to satisfy the query.

### Standard SQL

StarRocks supports standard SQL syntax, including aggregation, JOIN, sorting, window functions, and custom functions. Users can perform data analysis with standard SQL. In addition, StarRocks is compatible with MySQL protocol. Users can use various existing client tools and BI software to access StarRocks and perform data analysis with a simple drag-and-drop in StarRocks.

### Unified batch and streaming

StarRocks supports batch and streaming data import. It supports Kafka, HDFS, and local files as data sources, and ORC, Parquet, and CSV data formats. StarRocks can consume real-time Kafka data in data importing to avoid data loss or duplication. StarRocks can also import data in batches from local or remote (HDFS) data sources.

### High availability, high scalability

StarRocks supports multi-replica data storage and multi-instance data deployment. The cluster has the ability of self-healing and elastic recovery.

StarRocks adopts a distributed architecture  which allows its storage capacity and computing power to be scaled horizontally. StarRocks clusters can be expanded to hundreds of nodes to support up to 10PB data storage.

## Use Case

StarRocks can meet a variety of analysis needs, including OLAP analysis, customized reports, real-time data analysis, ad hoc data analysis, etc. Specific business scenarios include:

* OLAP analysis
* Real time data analysis
* High concurrency query
* Unified analysis
