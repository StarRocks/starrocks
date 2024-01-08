![DLA](../1.1-8-dla.png)

除了作为数据库高效分析本地数据外，StarRocks 还可以作为计算引擎，分析存储在数据湖中的数据，如 Apache Hive、Apache Iceberg、Apache Hudi 和 Delta Lake。通过 External Catalog 特性，StarRocks 可以与外部元数据存储快速建立连接。您无需进行数据迁移，即可无缝查询外部数据源。此外，您可以分析来自不同系统（如 HDFS 和 Amazon S3）的数据，以及各种文件格式，如 Parquet、ORC 和 CSV 等。

在上图展示的数据湖分析场景中，StarRocks 负责数据计算和分析，而数据湖负责数据的存储、组织和维护。数据湖允许用户以开放的存储格式存储数据，并使用灵活的 Schema 为各种 BI、AI、ad-hoc 和报表生成可信的单一数据源，为数据湖 Single Source of Truth 提供了能力基础。StarRocks 充分利用其向量化引擎和 CBO 的优势，显著提高了数据湖分析的性能。
