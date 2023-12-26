![DLA](../1.1-8-dla.png)

除了对本地数据进行高效分析之外，StarRocks 还可以作为计算引擎，用于分析存储在数据湖中的数据，如 Apache Hive、Apache Iceberg、Apache Hudi 和 Delta Lake。StarRocks 的一个关键特性是其 External Catalog，它充当了与外部维护的元数据存储库的连接。此功能使用户能够无缝查询外部数据源，消除了数据迁移的需要。因此，用户可以分析来自不同系统（如 HDFS 和 Amazon S3）的数据，以及各种文件格式，如 Parquet、ORC 和 CSV 等。

上图显示了一个数据湖分析场景，其中 StarRocks 负责数据计算和分析，而数据湖负责数据存储、组织和维护。数据湖允许用户以开放的存储格式存储数据，并使用灵活的 Schema 生成用于各种 BI、AI、ad-hoc 和报告用例的可信单一数据源的报表。StarRocks 充分利用其矢量化引擎和 CBO 的优势，显著提高了数据湖分析的性能。