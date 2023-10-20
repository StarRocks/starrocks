# Table of content

## Index

+ [Introduction](./introduction/StarRocks_intro.md)
+ Quick Start
  + [Concepts](./quick_start/Concepts.md)
  + [Architecture](./quick_start/Architecture.md)
  + [Deploy](./quick_start/Deploy.md)
  + [Data Flow and Control Flow](./quick_start/Data_flow_and_control_flow.md)
  + [Import and Query](./quick_start/Import_and_query.md)
  + [Test FAQs](./quick_start/Test_faq.md)
+ Table Design
  + [StarRocks Table Design](./table_design/StarRocks_table_design.md)
  + [Data Model](./table_design/Data_model.md)
  + [Data Distribution](./table_design/Data_distribution.md)
  + [Sort Key and Shortkey Index](./table_design/Sort_key.md)
  + [Materialized View](./table_design/Materialized_view.md)
  + [Bitmap Indexing](./table_design/Bitmap_index.md)
  + [Bloomfilter Indexing](./table_design/Bloomfilter_index.md)
+ Data Loading
  + [Loading Intro](./loading/Loading_intro.md)
  + [Stream Load](./loading/StreamLoad.md)
  + [Broker Load](./loading/BrokerLoad.md)
  + [Routine Load](./loading/RoutineLoad.md)
  + [Spark Load](./loading/SparkLoad.md)
  + [Insert Into](./loading/InsertInto.md)
  + [ETL in Loading](./loading/Etl_in_loading.md)
  + [Json Loading](./loading/Json_loading.md)
  + [Synchronize data from MySQL](./loading/Flink_cdc_load.md)
  + [Load data by using flink-connector-starrocks](./loading/Flink-connector-starrocks.md)
  + [DataX Writer](./loading/DataX-starrocks-writer.md)
+ Data Export
  + [Export](./unloading/Export.md)
  + [Spark Connector](./unloading/Spark_connector.md)
+ Using StarRocks
  + [Precise De-duplication with Bitmap](./using_starrocks/Using_bitmap.md)
  + [Approximate De-duplication with HLL](./using_starrocks/Using_HLL.md)
  + [Materialized View](./using_starrocks/Materialized_view.md)
  + [Colocation Join](./using_starrocks/Colocation_join.md)
  + [External Table](./using_starrocks/External_table.md)
  + [Array](./using_starrocks/Array.md)
  + [Window Function](./using_starrocks/Window_function.md)
  + [Cost Based Optimizer](./using_starrocks/Cost_based_optimizer.md)
  + [Lateral Join](./using_starrocks/Lateral_join.md)
  + [Configure a time zone](./using_starrocks/timezone.md)
+ Administration
  + [Build in docker](./administration/Build_in_docker.md)
  + [Manage a cluster](./administration/Cluster_administration.md)
+ FAQs
  + [Deploy](./faq/Deploy_faq.md)
  + Data Migration
    + Data Ingestion
      + [Data Ingestion FAQ](./faq/loading/Loading_faq.md)
      + [Stream Load](./faq/loading/Stream_load_faq.md)
      + [Routine Load](./faq/loading/Routine_load_faq.md)
      + [Broker Load](./faq/loading/Broker_load_faq.md)
      + [Insert Into](./faq/loading/Insert_into_faq.md)
      + [Flink connector](./faq/loading/Flink_connector_faq.md)
      + [DataX](./faq/loading/DataX_faq.md)
    + [Data Export](./faq/Exporting_faq.md)
    + [SQL](./faq/Sql_faq.md)
    + [Other FAQs](./faq/Others.md)
+ Release Notes
  + [v1.19](./release_notes/release-1.19.md)
  + [v2.0](./release_notes/release-2.0.md)
