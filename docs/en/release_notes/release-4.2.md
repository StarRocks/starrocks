---
displayed_sidebar: docs
description: "StarRocks 4.2 release notes: default range distribution in shared-data clusters, shared-data vector indexes, AI functions, native GEO types, Iceberg MERGE INTO."
products: [oss]
---

# StarRocks version 4.2

:::warning

**Upgrade Notes**

- FE recommends JDK 21. JDK 17 is still supported but deprecated, and FE prints a warning when it starts on JDK 17. The `JAVA_OPTS_FOR_JDK_*` variables in **fe.conf** are no longer read. Move any JVM options in them to `JAVA_OPTS` before upgrading.
- From v4.2, BE and CN nodes write string columns with a new page encoding by default (BE configuration item `enable_binary_plain_delta_offset`, default `true`). Versions earlier than v4.2 cannot read segments written with this encoding. Before a rolling upgrade, set `enable_binary_plain_delta_offset` to `false` on every BE and CN node, and set it back to `true` only after all BE and CN nodes run v4.2 and you no longer plan to roll back.

**Downgrade Notes**

- TBD: the earliest version that a v4.2 cluster can be downgraded to.
- Segments already written with `enable_binary_plain_delta_offset` enabled cannot be read after a downgrade to a version earlier than v4.2.

:::

## 4.2.0

Release Date: November 27, 2026

### Behavior Changes

- In shared-data clusters, range distribution is now the default distribution for tables created without a `DISTRIBUTED BY` clause, so their tablets are split and merged automatically. The FE configuration item `enable_range_distribution` now defaults to `true` and takes effect only in shared-data clusters. Previously, such Primary Key tables used hash distribution, such Duplicate Key tables used random distribution, and Aggregate and Unique Key tables required an explicit `DISTRIBUTED BY` clause. Asynchronous materialized views keep the previous default unless the new FE configuration item `enable_mv_range_distribution` (default `false`) is also set to `true`. To restore the previous behavior for tables, set `enable_range_distribution` to `false`. [#76543](https://github.com/StarRocks/starrocks/pull/76543) [#78230](https://github.com/StarRocks/starrocks/pull/78230)
- Primary Key tables in shared-data clusters support only the cloud-native persistent index. Existing tables that use the `LOCAL` persistent index are switched to the cloud-native persistent index when FE loads their metadata after the upgrade. [#75940](https://github.com/StarRocks/starrocks/pull/75940) [#76260](https://github.com/StarRocks/starrocks/pull/76260) [#76327](https://github.com/StarRocks/starrocks/pull/76327)
- The session variable `enable_global_late_materialization` now defaults to `true`, so global late materialization is applied to eligible queries. [#70412](https://github.com/StarRocks/starrocks/pull/70412)
- The session variable `plan_mode` now defaults to `local` instead of `auto`, so Iceberg scan ranges are planned on the FE by default. Set it to `auto` to restore the previous behavior. [#73851](https://github.com/StarRocks/starrocks/pull/73851)
- The session variable `blacklist_backup_routing` now defaults to `TABLET_HASH` instead of `CIRCULAR`. When a compute node is unavailable, its tablets are spread evenly across the remaining nodes, and each tablet goes to the same backup node across queries, so its data cache warms up on only one node. [#81723](https://github.com/StarRocks/starrocks/pull/81723)
- `ALTER TABLE ... ADD COLUMN` on an Aggregate table is now rejected when the new column specifies neither an aggregate function nor the `KEY` keyword. Previously, such a column was silently added as a key column, which changed the aggregation granularity of the table. To restore the previous behavior, set the FE configuration item `allow_implicit_key_column_in_agg_add_column` to `true`. [#79102](https://github.com/StarRocks/starrocks/pull/79102)
- Names reserved for virtual columns, such as `_tablet_id_` and `_row_id_`, can no longer be used as table column names. Previously, such a column could be created but could not be queried. [#78090](https://github.com/StarRocks/starrocks/pull/78090)
- `SHOW DATABASES FROM <catalog>` on an external catalog now requires a privilege on that catalog, consistent with `SET CATALOG`. [#81414](https://github.com/StarRocks/starrocks/pull/81414)
- When `enable_http_auth` is enabled, the public probe and observability HTTP endpoints also require authentication. No privilege check is applied to them. [#74741](https://github.com/StarRocks/starrocks/pull/74741)
- Reads of Hive text tables (OpenCSVSerde and LazySimpleSerDe) now follow Hive semantics for quote characters, `ESCAPED BY`, MAP values in text format, and row splitting. Query results on affected data can differ from earlier versions. [#75595](https://github.com/StarRocks/starrocks/pull/75595)
- Paimon tables are now read with the native C++ reader (paimon-cpp) by default. The new session variable `paimon_reader_mode` (`AUTO`, `JNI`, or `NATIVE`; default `AUTO`) selects the reader. Set it to `JNI` to use the Java reader. [#78189](https://github.com/StarRocks/starrocks/pull/78189) [#78563](https://github.com/StarRocks/starrocks/pull/78563)
- In shared-data clusters, horizontal compaction no longer writes its input data to the data cache by default, which avoids evicting data that queries use. To restore the previous behavior, set the BE configuration item `lake_enable_horizontal_compaction_fill_data_cache` to `true`. [#76859](https://github.com/StarRocks/starrocks/pull/76859)
- The legacy `ai_query` function is removed. Use the AI functions backed by AI providers, such as `ai_complete`. The BE configuration items `llm_cache_size`, `llm_max_concurrent_queries`, and `llm_max_queue_size` are removed. [#81607](https://github.com/StarRocks/starrocks/pull/81607)
- The default value of the FE configuration item `history_job_keep_max_num` is increased from `1000` to `10000`. [#80583](https://github.com/StarRocks/starrocks/pull/80583)
- The `LAST_REFRESH_DURATION` column of `information_schema.materialized_views` now reports the wall-clock duration of the last refresh. [#74892](https://github.com/StarRocks/starrocks/pull/74892)
- FE recommends JDK 21 and prints a warning when it starts on JDK 17. The `JAVA_OPTS_FOR_JDK_*` variables are no longer read. Use `JAVA_OPTS` instead. [#76058](https://github.com/StarRocks/starrocks/pull/76058) [#78352](https://github.com/StarRocks/starrocks/pull/78352)

### Shared-data Architecture

- **Range Distribution by Default, with Broader DDL Support**

  Range distribution becomes the default distribution in shared-data clusters (see Behavior Changes), and range-distributed tables now support more DDL operations: changing key and sort-key columns, adding or dropping a key column, widening an integer sort-key column, `ADD ROLLUP` with an independent sort key, multiple rollups, synchronous materialized views, RANGE distribution for incremental materialized views, and online `ALTER MATERIALIZED VIEW ... ORDER BY`. Data can also be migrated into range distribution, and split boundaries can be derived from the Primary Key index. [#75067](https://github.com/StarRocks/starrocks/pull/75067) [#76047](https://github.com/StarRocks/starrocks/pull/76047) [#76717](https://github.com/StarRocks/starrocks/pull/76717) [#75618](https://github.com/StarRocks/starrocks/pull/75618) [#76473](https://github.com/StarRocks/starrocks/pull/76473) [#81071](https://github.com/StarRocks/starrocks/pull/81071) [#77214](https://github.com/StarRocks/starrocks/pull/77214) [#75928](https://github.com/StarRocks/starrocks/pull/75928) [#77360](https://github.com/StarRocks/starrocks/pull/77360) [#77654](https://github.com/StarRocks/starrocks/pull/77654)

- **Multi-node Writes to a Single Tablet**

  Several compute nodes can now write to the same tablet in one load, and the partial transaction logs they produce are merged at commit. Loads into partitions with fewer tablets than compute nodes are no longer bottlenecked on one node. The session variable `lake_multi_node_tablet_write_mode` controls this behavior and defaults to `auto`. Partitions created at runtime and automatic-bucket sub-partitions also spread their writes across compute nodes. [#79378](https://github.com/StarRocks/starrocks/pull/79378) [#79507](https://github.com/StarRocks/starrocks/pull/79507) [#79892](https://github.com/StarRocks/starrocks/pull/79892)

- **Wider Coverage for Sample-based Tablet Pre-split**

  Tablet pre-split now also applies to INSERT from any external catalog table, INSERT from FILES with projections or filters, CSV Broker Load, Broker Load with a column list or partition columns read from the file path, partitioned Broker Load (using Parquet and ORC footers), the target partition of an incremental materialized view refresh, and existing partitions of manually range-partitioned tables. Load profiles show the pre-split phases. [#79657](https://github.com/StarRocks/starrocks/pull/79657) [#79681](https://github.com/StarRocks/starrocks/pull/79681) [#79625](https://github.com/StarRocks/starrocks/pull/79625) [#79508](https://github.com/StarRocks/starrocks/pull/79508) [#79509](https://github.com/StarRocks/starrocks/pull/79509) [#78041](https://github.com/StarRocks/starrocks/pull/78041) [#79742](https://github.com/StarRocks/starrocks/pull/79742) [#79626](https://github.com/StarRocks/starrocks/pull/79626) [#79775](https://github.com/StarRocks/starrocks/pull/79775) [#78020](https://github.com/StarRocks/starrocks/pull/78020)

- **Parallel Scan of Large and Skewed Tablets**

  A large tablet can now be scanned by several scan operators in parallel, and skewed large tablets are split across scan operators automatically. [#76364](https://github.com/StarRocks/starrocks/pull/76364) [#76353](https://github.com/StarRocks/starrocks/pull/76353) [#76415](https://github.com/StarRocks/starrocks/pull/76415) [#76450](https://github.com/StarRocks/starrocks/pull/76450)

- Supports light-weight tablet creation for cloud-native tables, controlled by the table property `light_weight_tablet_creation` and the FE configuration item `lake_enable_light_weight_tablet_creation` (default `false`). Creating tablets for new partitions, schema changes, and rollups then skips writing initial tablet metadata to object storage. [#73422](https://github.com/StarRocks/starrocks/pull/73422) [#73845](https://github.com/StarRocks/starrocks/pull/73845) [#74132](https://github.com/StarRocks/starrocks/pull/74132)
- Supports a fast path for adding and dropping indexes on cloud-native tables (Index Delta Group), including on range-distributed tables. [#71689](https://github.com/StarRocks/starrocks/pull/71689) [#75791](https://github.com/StarRocks/starrocks/pull/75791)
- Supports bounded, incremental vacuum rounds driven by the FE. [#76848](https://github.com/StarRocks/starrocks/pull/76848) [#81558](https://github.com/StarRocks/starrocks/pull/81558) [#81571](https://github.com/StarRocks/starrocks/pull/81571)
- Supports time windows and intervals for base compaction in shared-data clusters, and skips automatic compaction for tablets whose compaction score is zero. [#79900](https://github.com/StarRocks/starrocks/pull/79900) [#80549](https://github.com/StarRocks/starrocks/pull/80549)
- Supports `ADD PARTITION` while a metadata-only schema change is running. [#74658](https://github.com/StarRocks/starrocks/pull/74658)
- Supports `CANCEL ALTER TABLE ... FORCE` to cancel alter jobs that are stuck in publish. [#73828](https://github.com/StarRocks/starrocks/pull/73828)
- A query is re-planned and retried when the tablet metadata version it needs has been vacuumed. [#79227](https://github.com/StarRocks/starrocks/pull/79227)
- Supports modifying the `datacache.enable` property of cloud-native tables dynamically. [#69011](https://github.com/StarRocks/starrocks/pull/69011)
- Supports the virtual columns `_tablet_id_`, `_row_id_`, and `_segment_id_`. [#66214](https://github.com/StarRocks/starrocks/pull/66214) [#68938](https://github.com/StarRocks/starrocks/pull/68938)
- Supports persisting AWS web identity credentials for storage volumes. [#77638](https://github.com/StarRocks/starrocks/pull/77638)
- Adds memory breakdown and load counts for the Primary Key index cache in shared-data clusters. [#80676](https://github.com/StarRocks/starrocks/pull/80676)
- The FE configuration item `lake_autovacuum_parallel_partitions` can now be modified dynamically, and vacuum is scheduled fairly across partitions. [#75763](https://github.com/StarRocks/starrocks/pull/75763)

### Vector Search

- **Vector Indexes in Shared-data Clusters**

  Shared-data clusters now support vector indexes. Vector indexes are built asynchronously in the warehouse of the table, and their build is scheduled around compaction. You can add a vector index to an existing table with `ALTER TABLE ... ADD INDEX`. Vector indexes no longer require the FE configuration item `enable_experimental_vector`, which is removed. [#72073](https://github.com/StarRocks/starrocks/pull/72073) [#72076](https://github.com/StarRocks/starrocks/pull/72076) [#72070](https://github.com/StarRocks/starrocks/pull/72070) [#74212](https://github.com/StarRocks/starrocks/pull/74212) [#75865](https://github.com/StarRocks/starrocks/pull/75865) [#72926](https://github.com/StarRocks/starrocks/pull/72926) [#75542](https://github.com/StarRocks/starrocks/pull/75542) [#77004](https://github.com/StarRocks/starrocks/pull/77004)

- **Filtered Vector Search**

  Supports pre-filter strategies for vector search with filter predicates, and supports ANN search with predicates that cannot be pushed down to the index. [#74715](https://github.com/StarRocks/starrocks/pull/74715) [#78094](https://github.com/StarRocks/starrocks/pull/78094)

- **HNSW Quantization and Tuning**

  Supports SQ4, SQ8, and PQ quantization for HNSW indexes, adaptive `ef_search`, and a user-controlled ANN refine option. [#73276](https://github.com/StarRocks/starrocks/pull/73276) [#72999](https://github.com/StarRocks/starrocks/pull/72999) [#74533](https://github.com/StarRocks/starrocks/pull/74533)

- Supports vector indexes with inner product distance. [#77614](https://github.com/StarRocks/starrocks/pull/77614)
- Supports vector search in prepared statements through `CAST(? AS ARRAY<FLOAT>)`. [#73585](https://github.com/StarRocks/starrocks/pull/73585)
- Supports global late materialization for HNSW vector queries. [#73584](https://github.com/StarRocks/starrocks/pull/73584)
- Adds a vector index cache with TTL eviction and asynchronous loading on cache misses. [#73508](https://github.com/StarRocks/starrocks/pull/73508) [#77349](https://github.com/StarRocks/starrocks/pull/77349) [#77416](https://github.com/StarRocks/starrocks/pull/77416)
- Adds vector index statistics to query profiles, and exposes the built version of asynchronous vector indexes in `information_schema.partitions_meta`. [#77353](https://github.com/StarRocks/starrocks/pull/77353) [#74359](https://github.com/StarRocks/starrocks/pull/74359)

### AI Functions

- **SQL-managed AI Providers**

  Supports `CREATE`, `ALTER`, `DROP`, `SHOW`, `DESC`, and `SET DEFAULT` for AI providers of the `chat`, `embedding`, and `rerank` types. API keys are masked in `SHOW` and `DESC` output and in audit logs. [#79008](https://github.com/StarRocks/starrocks/pull/79008) [#79066](https://github.com/StarRocks/starrocks/pull/79066)

- **AI Functions**

  Adds `ai_complete` and `ai_embed`, which run asynchronously against AI providers, and the text functions `ai_sentiment`, `ai_classify`, `ai_extract`, `ai_fix_grammar`, `ai_redact`, `ai_translate`, `ai_similarity`, `ai_summarize`, and `ai_filter`. [#76816](https://github.com/StarRocks/starrocks/pull/76816) [#78807](https://github.com/StarRocks/starrocks/pull/78807) [#81415](https://github.com/StarRocks/starrocks/pull/81415)

- **Multimodal Input**

  Adds the read-only `FILE` type. Paimon `BLOB` columns are read as `FILE`, and `ai_complete` and `ai_embed` accept `FILE` inputs. [#79390](https://github.com/StarRocks/starrocks/pull/79390) [#80350](https://github.com/StarRocks/starrocks/pull/80350)

- Supports privileges on AI functions and AI providers. [#79328](https://github.com/StarRocks/starrocks/pull/79328)
- Adds AI execution statistics to query profiles and audit logs. [#78880](https://github.com/StarRocks/starrocks/pull/78880)
- Pushes Top-N down below AI function projections to reduce the number of AI calls. [#78779](https://github.com/StarRocks/starrocks/pull/78779)

### Geospatial Analytics

- **Native GEOMETRY and GEOGRAPHY Types**

  Adds native `GEOMETRY` and `GEOGRAPHY` types. You can query scalar and nested geospatial columns in Iceberg tables, return values as WKB through the MySQL protocol, and transform between SRID 4326 and 3857. [#78916](https://github.com/StarRocks/starrocks/pull/78916) [#78999](https://github.com/StarRocks/starrocks/pull/78999) [#79005](https://github.com/StarRocks/starrocks/pull/79005) [#79043](https://github.com/StarRocks/starrocks/pull/79043) [#79642](https://github.com/StarRocks/starrocks/pull/79642) [#79723](https://github.com/StarRocks/starrocks/pull/79723) [#79110](https://github.com/StarRocks/starrocks/pull/79110) [#83357](https://github.com/StarRocks/starrocks/pull/83357) [#83472](https://github.com/StarRocks/starrocks/pull/83472) [#83617](https://github.com/StarRocks/starrocks/pull/83617) [#83063](https://github.com/StarRocks/starrocks/pull/83063) [#79035](https://github.com/StarRocks/starrocks/pull/79035) [#79999](https://github.com/StarRocks/starrocks/pull/79999)

- **Spatial and H3 Functions**

  Adds WKT and WKB constructors and serializers, containment predicates (such as `ST_Contains`, `ST_Within`, `ST_Covers`, and `ST_CoveredBy`), distance predicates, measurement and property functions, polygon overlay functions including `ST_Intersection`, `ST_Buffer`, `ST_SimplifyPreserveTopology`, the `ST_CoverageSimplify` window functions, and H3 functions. [#79439](https://github.com/StarRocks/starrocks/pull/79439) [#79525](https://github.com/StarRocks/starrocks/pull/79525) [#79730](https://github.com/StarRocks/starrocks/pull/79730) [#79786](https://github.com/StarRocks/starrocks/pull/79786) [#79808](https://github.com/StarRocks/starrocks/pull/79808) [#79903](https://github.com/StarRocks/starrocks/pull/79903) [#79964](https://github.com/StarRocks/starrocks/pull/79964) [#80052](https://github.com/StarRocks/starrocks/pull/80052) [#80055](https://github.com/StarRocks/starrocks/pull/80055) [#80058](https://github.com/StarRocks/starrocks/pull/80058) [#80064](https://github.com/StarRocks/starrocks/pull/80064) [#80073](https://github.com/StarRocks/starrocks/pull/80073) [#80037](https://github.com/StarRocks/starrocks/pull/80037)

### Data Lake Analytics

- **Iceberg UPDATE and MERGE INTO**

  Supports `UPDATE` and `MERGE INTO` on Iceberg tables, and supports `DELETE`, `UPDATE`, and `MERGE INTO` on an Iceberg branch. [#72884](https://github.com/StarRocks/starrocks/pull/72884) [#73203](https://github.com/StarRocks/starrocks/pull/73203) [#72064](https://github.com/StarRocks/starrocks/pull/72064) [#73707](https://github.com/StarRocks/starrocks/pull/73707) [#74725](https://github.com/StarRocks/starrocks/pull/74725) [#81554](https://github.com/StarRocks/starrocks/pull/81554)

- **Iceberg Metadata Tables and Table Maintenance**

  Adds the `$properties`, `$entries`, and all-snapshots metadata tables, the `add_equality_delete_files` procedure, `ALTER TABLE ... REPLACE PARTITION COLUMN`, and adding a `NOT NULL` column with a default value to an Iceberg v3 table. Upgrades the Iceberg library to 1.11.0. [#68504](https://github.com/StarRocks/starrocks/pull/68504) [#81551](https://github.com/StarRocks/starrocks/pull/81551) [#81129](https://github.com/StarRocks/starrocks/pull/81129) [#70508](https://github.com/StarRocks/starrocks/pull/70508) [#81670](https://github.com/StarRocks/starrocks/pull/81670) [#81550](https://github.com/StarRocks/starrocks/pull/81550) [#79870](https://github.com/StarRocks/starrocks/pull/79870)

- **Shredded VARIANT in Iceberg**

  Supports reading shredded VARIANT columns in Iceberg Parquet files, with typed-path reads, subfield pruning, and predicate pushdown based on shredded Parquet statistics. [#69884](https://github.com/StarRocks/starrocks/pull/69884) [#71077](https://github.com/StarRocks/starrocks/pull/71077) [#71741](https://github.com/StarRocks/starrocks/pull/71741) [#71315](https://github.com/StarRocks/starrocks/pull/71315) [#70291](https://github.com/StarRocks/starrocks/pull/70291)

- **Native Paimon Reader**

  Adds a native C++ reader for Paimon tables (paimon-cpp), enabled by default (see Behavior Changes), with `DATE` and `DECIMAL` predicate pushdown. Also supports the Paimon VARIANT type, Paimon deletion vector v2, and `TRUNCATE` on Paimon tables. Upgrades the Paimon library to 2.0.0. [#76898](https://github.com/StarRocks/starrocks/pull/76898) [#78189](https://github.com/StarRocks/starrocks/pull/78189) [#78563](https://github.com/StarRocks/starrocks/pull/78563) [#79129](https://github.com/StarRocks/starrocks/pull/79129) [#79856](https://github.com/StarRocks/starrocks/pull/79856) [#78074](https://github.com/StarRocks/starrocks/pull/78074) [#78124](https://github.com/StarRocks/starrocks/pull/78124) [#72249](https://github.com/StarRocks/starrocks/pull/72249) [#67559](https://github.com/StarRocks/starrocks/pull/67559) [#79390](https://github.com/StarRocks/starrocks/pull/79390)

- **Apache Fluss Catalog**

  Supports reading Apache Fluss tables through a Fluss catalog. Only tables with `table.datalake.enabled` set to `true` and Paimon as the lakehouse storage are supported. [#72424](https://github.com/StarRocks/starrocks/pull/72424) [#81503](https://github.com/StarRocks/starrocks/pull/81503)

- Supports native queries in JDBC catalogs, and adds the `schema_resolver` property to JDBC catalogs. [#72005](https://github.com/StarRocks/starrocks/pull/72005) [#68682](https://github.com/StarRocks/starrocks/pull/68682)
- Supports a custom catalog name for Iceberg JDBC catalogs. [#75966](https://github.com/StarRocks/starrocks/pull/75966)
- Supports automatic and manual metadata refresh for ODPS catalogs. [#69954](https://github.com/StarRocks/starrocks/pull/69954)
- Supports global shuffle for Hive table sinks. [#74121](https://github.com/StarRocks/starrocks/pull/74121)
- Supports `TRUNCATE` and `ALTER TABLE` in unified catalogs. [#77749](https://github.com/StarRocks/starrocks/pull/77749)
- Pushes join runtime filters down into the Parquet reader for row-level filtering, and reduces Parquet decoding CPU cost. [#77467](https://github.com/StarRocks/starrocks/pull/77467) [#73288](https://github.com/StarRocks/starrocks/pull/73288) [#81535](https://github.com/StarRocks/starrocks/pull/81535)
- Reduces FE CPU usage when planning scans of Iceberg tables with many files. [#81709](https://github.com/StarRocks/starrocks/pull/81709)
- External table statistics collection now tolerates busy BEs, resumes across runs, and estimates from the rows it has collected. [#81526](https://github.com/StarRocks/starrocks/pull/81526)
- A corrupted data cache entry of an external file is dropped and the file is read again from remote storage. [#81555](https://github.com/StarRocks/starrocks/pull/81555)
- Adds the BE configuration item `s3_multipart_equal_part_size` (default `false`) for equal-size S3 multipart uploads. [#80377](https://github.com/StarRocks/starrocks/pull/80377)
- Upgrades Delta Kernel to 4.3. [#75535](https://github.com/StarRocks/starrocks/pull/75535)

### Loading and Storage

- Supports the Apache Arrow format in Stream Load. [#74593](https://github.com/StarRocks/starrocks/pull/74593)
- Supports loading Debezium JSON data. [#71537](https://github.com/StarRocks/starrocks/pull/71537)
- Supports loading compressed JSON files. [#61786](https://github.com/StarRocks/starrocks/pull/61786)
- Supports running Stream Load and Routine Load on the pipeline engine, controlled by the FE configuration items `enable_pipeline_stream_load` and `enable_pipeline_routine_load` (both default `false`). [#76201](https://github.com/StarRocks/starrocks/pull/76201)
- Supports writing rejected records to a system table, controlled by the BE configuration item `enable_rejected_record_sync` (default `false`). [#71761](https://github.com/StarRocks/starrocks/pull/71761)
- Supports per-column ZSTD compression with a shared dictionary for large string and JSON columns. [#76871](https://github.com/StarRocks/starrocks/pull/76871)
- Adds a delta-offset page encoding for string columns, which compresses better than absolute offsets (see Upgrade Notes). [#74574](https://github.com/StarRocks/starrocks/pull/74574)
- Time columns in `information_schema.loads` now have millisecond precision. [#73740](https://github.com/StarRocks/starrocks/pull/73740)

### Query Engine

- Global late materialization is enabled by default (see Behavior Changes), uses an adaptive strategy, and supports native scans. [#70412](https://github.com/StarRocks/starrocks/pull/70412) [#70618](https://github.com/StarRocks/starrocks/pull/70618) [#69753](https://github.com/StarRocks/starrocks/pull/69753)
- Detects NULL skew and most-common-value skew in the same join. [#78350](https://github.com/StarRocks/starrocks/pull/78350)
- Adds the `[merge_sort]` window hint, and an optimizer rule that uses merge sort for window functions with skewed partition keys. [#76903](https://github.com/StarRocks/starrocks/pull/76903) [#76905](https://github.com/StarRocks/starrocks/pull/76905)
- Improves runtime filters: the runtime filter worker runs on several threads per BE, a global runtime filter degrades to pass-all instead of being lost, runtime filters can be pushed sideways, and monotonic runtime filter expressions can use indexes. [#81669](https://github.com/StarRocks/starrocks/pull/81669) [#81671](https://github.com/StarRocks/starrocks/pull/81671) [#81539](https://github.com/StarRocks/starrocks/pull/81539) [#76720](https://github.com/StarRocks/starrocks/pull/76720)
- Supports materialization hints for CTEs, and adds the `materialize()` function as an optimization barrier. [#70802](https://github.com/StarRocks/starrocks/pull/70802) [#71808](https://github.com/StarRocks/starrocks/pull/71808)
- Supports the query queue in ETL execution mode. [#69522](https://github.com/StarRocks/starrocks/pull/69522)
- Supports zone map pruning for `date_trunc` predicates. [#79021](https://github.com/StarRocks/starrocks/pull/79021)
- Computes `ROLLUP` levels by cascading aggregation. [#81525](https://github.com/StarRocks/starrocks/pull/81525)
- Improves cardinality estimation with NULL-aware histograms, MCV propagation through predicates, and statistics for more functions and operators. [#78865](https://github.com/StarRocks/starrocks/pull/78865) [#73452](https://github.com/StarRocks/starrocks/pull/73452) [#73489](https://github.com/StarRocks/starrocks/pull/73489) [#75812](https://github.com/StarRocks/starrocks/pull/75812) [#77415](https://github.com/StarRocks/starrocks/pull/77415) [#79230](https://github.com/StarRocks/starrocks/pull/79230) [#77850](https://github.com/StarRocks/starrocks/pull/77850) [#80858](https://github.com/StarRocks/starrocks/pull/80858)
- Extends low-cardinality optimization to `array_agg`, structs, `UNION ALL`, and CTEs. [#68842](https://github.com/StarRocks/starrocks/pull/68842) [#66713](https://github.com/StarRocks/starrocks/pull/66713) [#67359](https://github.com/StarRocks/starrocks/pull/67359) [#71169](https://github.com/StarRocks/starrocks/pull/71169) [#72781](https://github.com/StarRocks/starrocks/pull/72781)
- Query dumps now capture external Iceberg and Hive tables, column min/max statistics, histograms, and low-cardinality dictionaries for replay. [#76936](https://github.com/StarRocks/starrocks/pull/76936) [#77002](https://github.com/StarRocks/starrocks/pull/77002) [#76432](https://github.com/StarRocks/starrocks/pull/76432) [#76941](https://github.com/StarRocks/starrocks/pull/76941)
- Improves performance on ARM64 with NEON and CRC32C acceleration, and upgrades compression libraries. [#78737](https://github.com/StarRocks/starrocks/pull/78737) [#79031](https://github.com/StarRocks/starrocks/pull/79031) [#78457](https://github.com/StarRocks/starrocks/pull/78457) [#78566](https://github.com/StarRocks/starrocks/pull/78566) [#73486](https://github.com/StarRocks/starrocks/pull/73486)
- Adds metrics for SQL Plan Manager and Plan Advisor. [#71471](https://github.com/StarRocks/starrocks/pull/71471) [#71468](https://github.com/StarRocks/starrocks/pull/71468)

### Functions and SQL Syntax

- Added the following functions:
  - `ds_theta_accumulate`, `ds_theta_combine`, `ds_theta_intersect_cond_agg`, `ds_theta_estimate`, and related Apache DataSketches Theta sketch functions. [#76311](https://github.com/StarRocks/starrocks/pull/76311)
  - `min_n` and `max_n`. [#63807](https://github.com/StarRocks/starrocks/pull/63807)
  - `regexp_position`. [#67252](https://github.com/StarRocks/starrocks/pull/67252)
  - `http_request`: sends HTTP or HTTPS requests from SQL. Requests are restricted by an allowlist, and private networks are blocked by default. [#66206](https://github.com/StarRocks/starrocks/pull/66206)
  - `xx_hash32`, `xx_hash64`, and `xx_hash3_32`. [#74445](https://github.com/StarRocks/starrocks/pull/74445)
  - `get_json_scalar` and `is_json_scalar`. [#68815](https://github.com/StarRocks/starrocks/pull/68815) [#66050](https://github.com/StarRocks/starrocks/pull/66050)
  - `materialize`. [#71808](https://github.com/StarRocks/starrocks/pull/71808)
- Provides the following function or syntactic extensions:
  - Supports `GROUP BY ALL`. [#70274](https://github.com/StarRocks/starrocks/pull/70274)
  - Supports the PostgreSQL-style `::` cast shorthand. [#72330](https://github.com/StarRocks/starrocks/pull/72330)
  - Supports `RANGE` frames with offsets in window functions. [#73030](https://github.com/StarRocks/starrocks/pull/73030)
  - Supports `STRING_AGG` as syntactic sugar for `GROUP_CONCAT`. [#64704](https://github.com/StarRocks/starrocks/pull/64704)
  - Supports MySQL-compatible `TRIM([{BOTH | LEADING | TRAILING}] FROM str)`. [#73680](https://github.com/StarRocks/starrocks/pull/73680)
  - Supports named parameters in scalar functions. [#66344](https://github.com/StarRocks/starrocks/pull/66344)
  - Supports multiplication and division of `INTERVAL` values, and the `QUARTER` unit in timestamp arithmetic. [#68407](https://github.com/StarRocks/starrocks/pull/68407) [#75941](https://github.com/StarRocks/starrocks/pull/75941)
  - Supports `OR` conditions that contain `MATCH` expressions. [#68311](https://github.com/StarRocks/starrocks/pull/68311)
  - Supports UTF-8 in `ngram_search` functions. [#67207](https://github.com/StarRocks/starrocks/pull/67207)
  - Supports casting VARIANT strings to numeric and temporal types, and `UNNEST` on VARIANT arrays. [#79096](https://github.com/StarRocks/starrocks/pull/79096) [#79079](https://github.com/StarRocks/starrocks/pull/79079)
  - Supports `SHOW CREATE FUNCTION`. [#73519](https://github.com/StarRocks/starrocks/pull/73519)
  - Supports `SHOW WARNINGS` and `SHOW ERRORS`. [#75176](https://github.com/StarRocks/starrocks/pull/75176)
  - Adds the SQL mode `FORBID_INVALID_IMPLICIT_CAST` for strict type checking. [#71998](https://github.com/StarRocks/starrocks/pull/71998)
  - Adds the session variable `max_array_length`. [#77913](https://github.com/StarRocks/starrocks/pull/77913)
  - Adds the session variable `default_view_sql_security` to set the default SQL SECURITY of `CREATE VIEW`. [#75530](https://github.com/StarRocks/starrocks/pull/75530)
  - In the Trino dialect, `date()` follows Trino semantics, so partition pruning applies to predicates on it. [#69365](https://github.com/StarRocks/starrocks/pull/69365)

### Materialized Views

- Adds the `information_schema.materialized_view_refresh_jobs` system table. [#74769](https://github.com/StarRocks/starrocks/pull/74769) [#74682](https://github.com/StarRocks/starrocks/pull/74682)
- Adds the `BASE_TABLE_REFRESH_VERSION_TIMES` column, the last refresh time, and structured query rewrite eligibility to `information_schema.materialized_views`. [#74717](https://github.com/StarRocks/starrocks/pull/74717) [#71642](https://github.com/StarRocks/starrocks/pull/71642) [#74395](https://github.com/StarRocks/starrocks/pull/74395)
- Adds global and per-materialized-view refresh and query metrics. [#74964](https://github.com/StarRocks/starrocks/pull/74964) [#75142](https://github.com/StarRocks/starrocks/pull/75142) [#74907](https://github.com/StarRocks/starrocks/pull/74907)
- `EXPLAIN REFRESH` explains why an up-to-date materialized view has no refresh plan. [#76952](https://github.com/StarRocks/starrocks/pull/76952)
- A bucket-only `OPTIMIZE` on a base table no longer inactivates its materialized views. [#70786](https://github.com/StarRocks/starrocks/pull/70786)

### Management & Observability

- Supports the `ttl` property for `BACKUP`, and adds the `DROP SNAPSHOT` statement. [#78092](https://github.com/StarRocks/starrocks/pull/78092)
- Verifies the integrity of snapshot files during `RESTORE`. [#77732](https://github.com/StarRocks/starrocks/pull/77732)
- Supports `ALTER SYSTEM TRANSFER LEADER` with safe in-place leader demotion. [#75592](https://github.com/StarRocks/starrocks/pull/75592)
- Supports the `mem_used_pct_limit` property for resource groups, which stops admitting queued queries of a group when its memory usage reaches the limit. [#77213](https://github.com/StarRocks/starrocks/pull/77213)
- Supports `ADMIN EXECUTE` on multiple BE or CN nodes. [#73339](https://github.com/StarRocks/starrocks/pull/73339)
- Records the last update time and the last query access time of each partition, and persists them across FE restarts. [#76637](https://github.com/StarRocks/starrocks/pull/76637) [#77341](https://github.com/StarRocks/starrocks/pull/77341)
- Supports retrieving query profiles by a custom query ID, and analyzing the profile of `last_query_id()`. [#79108](https://github.com/StarRocks/starrocks/pull/79108) [#64557](https://github.com/StarRocks/starrocks/pull/64557)
- Adds a latency threshold for FE profile logging, and improves the readability and size of query profiles. [#69396](https://github.com/StarRocks/starrocks/pull/69396) [#65975](https://github.com/StarRocks/starrocks/pull/65975) [#70388](https://github.com/StarRocks/starrocks/pull/70388) [#80089](https://github.com/StarRocks/starrocks/pull/80089)
- Shows queries in the planning phase in `current_queries`. [#69351](https://github.com/StarRocks/starrocks/pull/69351)
- Adds the `TASK_SOURCE` column to `information_schema.task_runs`. [#74487](https://github.com/StarRocks/starrocks/pull/74487)
- Writes one audit log entry per statement for multi-statement requests. [#71194](https://github.com/StarRocks/starrocks/pull/71194)
- Supports rolling the FE edit log by size (`edit_log_roll_bytes`), configurable FE log rollover, and a configurable free disk threshold for the FE meta directory. [#75728](https://github.com/StarRocks/starrocks/pull/75728) [#76760](https://github.com/StarRocks/starrocks/pull/76760) [#78231](https://github.com/StarRocks/starrocks/pull/78231)
- Adds build information, retained journal, and statistics cache metrics. [#77359](https://github.com/StarRocks/starrocks/pull/77359) [#78234](https://github.com/StarRocks/starrocks/pull/78234) [#76044](https://github.com/StarRocks/starrocks/pull/76044)
- Supports a per-UDF `service_url` to connect to an external Python UDF worker, a launcher for the Python UDF worker process, and Arrow input for Java UDFs. [#76466](https://github.com/StarRocks/starrocks/pull/76466) [#80091](https://github.com/StarRocks/starrocks/pull/80091) [#75907](https://github.com/StarRocks/starrocks/pull/75907)
- Supports the MySQL compressed protocol (zlib and zstd). [#80180](https://github.com/StarRocks/starrocks/pull/80180)
- Provides Debian (.deb) packages for FE and BE. [#68821](https://github.com/StarRocks/starrocks/pull/68821)
- Adds the BE configuration item `enable_stop_be_action` to control the `/api/_stop_be` endpoint. [#74336](https://github.com/StarRocks/starrocks/pull/74336)
- The FE configuration item `label_clean_interval_second` can now be modified dynamically. [#78926](https://github.com/StarRocks/starrocks/pull/78926)

### Security

- Security integrations can read user groups from a claim of the verified token, and security integration users can authenticate over HTTP, Arrow Flight SQL, and load RPCs. [#80942](https://github.com/StarRocks/starrocks/pull/80942) [#80940](https://github.com/StarRocks/starrocks/pull/80940)
- Adds the catalog property `ranger.plugin.hive.catalog_prefix.enabled`, which prefixes the Ranger Hive `database` resource with the catalog name. [#80545](https://github.com/StarRocks/starrocks/pull/80545)
- Supports the EXPORT privilege on views and materialized views. [#79621](https://github.com/StarRocks/starrocks/pull/79621)
- Caps the nesting depth of the JSON parser. [#79530](https://github.com/StarRocks/starrocks/pull/79530)
- [CVE] Upgraded Thrift to v0.23.0. [#73625](https://github.com/StarRocks/starrocks/pull/73625)
- Upgraded OpenSSL to 3.5.7, krb5 to 1.21.3, protobuf-java to 3.25.9, and Broker libthrift to v0.22.0. [#77971](https://github.com/StarRocks/starrocks/pull/77971) [#78565](https://github.com/StarRocks/starrocks/pull/78565) [#71372](https://github.com/StarRocks/starrocks/pull/71372)
