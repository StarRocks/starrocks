![DLA](../1.1-8-dla.png)

除了对本地数据进行高效分析之外，StarRocks 还可以作为计算引擎，用于分析存储在数据湖中的数据，如 Apache Hive、Apache Iceberg、Apache Hudi 和 Delta Lake。StarRocks 提供 External Catalog 特性，可以快速建立与外部元数据存储的连接。用户无需进行数据迁移就能无缝查询外部数据源。此外，用户可以分析来自不同系统（如 HDFS 和 Amazon S3）的数据，以及各种文件格式，如 Parquet、ORC 和 CSV 等。

上图展示了一个数据湖分析场景，其中 StarRocks 负责数据计算和分析，而数据湖负责数据存储、组织和维护。数据湖允许用户以开放的存储格式存储数据，并使用灵活的 Schema 为各种 BI、AI、ad-hoc 和报表生成可信单一数据源，为数据湖 Single Source of Truth 提供了能力基础。StarRocks 充分利用其向量化引擎和 CBO 的优势，显著提高了数据湖分析的性能。
