---
displayed_sidebar: docs
---

# StarRocks version 4.0

:::warning

**Downgrade Notes**

- After upgrading StarRocks to v4.0, DO NOT downgrade it directly to v3.5.0 & v3.5.1, otherwise it will cause metadata incompatibility and FE crash. You must downgrade the cluster to v3.5.2 or later to prevent these issues.
- Before downgrading clusters from v4.0.2 to v4.0.1, v4.0.0, and v3.5.2~v3.5.10, execute the following statement:

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=false;
  ```

  After upgrading the cluster back to v4.0.2 and later, execute the following statement:

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=true;
  ```

:::

## 4.0.10

Release Date: May 9, 2026

### Behavior Changes

- Cloud storage credentials are now redacted in error messages produced by `INSERT INTO FILES`, preventing accidental exposure of secrets in error logs and `SHOW LOAD` output. [#71245](https://github.com/StarRocks/starrocks/pull/71245)
- StarRocks no longer permits queries against insert-only ACID Hive tables in Hive catalog. Previously such queries could silently return more rows than actually visible because INSERT OVERWRITE operations were not recognized. Affected tables now return an explicit error instead of incorrect results. [#71460](https://github.com/StarRocks/starrocks/pull/71460)

### Improvements

- Added an Avro schema cache in Iceberg `PartitionData` construction to remove redundant Jackson `ObjectMapper` allocations during partition load on tables with many partitions. [#72215](https://github.com/StarRocks/starrocks/pull/72215)
- Optimized `CatalogRecycleBin.getAdjustedRecycleTimestamp` to avoid rebuilding the table-id map on every call, reducing recycle-bin cleanup and tablet scheduling overhead. [#72128](https://github.com/StarRocks/starrocks/pull/72128)
- `OlapTableSink.createLocation` now batches tablet-location lookups in shared-data mode, removing per-tablet StarOS RPCs that previously stalled the planner critical section. [#72041](https://github.com/StarRocks/starrocks/pull/72041)
- Java UDAF instances are now loaded and initialized once per query and reused across pipeline driver instances, removing the linear driver-preparation overhead at high `pipeline_dop`. [#72038](https://github.com/StarRocks/starrocks/pull/72038)
- Added BE metrics `starrocks_be_staros_shard_info_fallback_total` and `starrocks_be_staros_shard_info_fallback_failed_total` to track when the StarOS worker falls back to fetching shard info from `starmgr` because the local cache missed. [#71620](https://github.com/StarRocks/starrocks/pull/71620)
- File-bundle writes now prefer a tablet-local aggregator so the bundled tablet metadata path does not require cross-node shard-info lookups. [#71613](https://github.com/StarRocks/starrocks/pull/71613)
- Audit log entries now include the queried tables and views referenced by each query. [#71596](https://github.com/StarRocks/starrocks/pull/71596)
- `INSERT INTO FILES` CSV export now supports `csv.enclose` and `csv.escape` properties for controlling field quoting and escaping. [#71589](https://github.com/StarRocks/starrocks/pull/71589)
- Added LDAP direct bind authentication via DN pattern, removing the requirement for an admin search account in single-tenant LDAP setups. [#71559](https://github.com/StarRocks/starrocks/pull/71559)
- Added the `starrocks_fe_tablet_num` metric for shared-data clusters to match the shared-nothing metric set. [#71444](https://github.com/StarRocks/starrocks/pull/71444)
- `star_mgr_meta_sync_interval_sec` is now runtime-mutable via `ADMIN SET FRONTEND CONFIG`; the new interval takes effect on the next sync cycle without an FE restart. [#71675](https://github.com/StarRocks/starrocks/pull/71675)

### Bug Fixes

The following issues have been fixed:

- A race in shared-data combined txn log mode where INSERT into per-partition coordinator dispatch could classify legitimate txn logs as orphan and drop them, leaving the transaction stuck in non-VISIBLE state. [#72237](https://github.com/StarRocks/starrocks/pull/72237)
- An issue where `_incremental_open_node_channel` channels in shared-data combined txn log mode silently dropped txn logs because the legacy "sender_id == 0 collects all logs" rule did not apply to incremental channels. [#71992](https://github.com/StarRocks/starrocks/pull/71992)
- An issue where `RuntimeProfile::to_thrift()` could crash BE with `std::bad_optional_access` when another thread reset counter min/max values during profile serialization. [#72904](https://github.com/StarRocks/starrocks/pull/72904)
- An inconsistency in flat JSON merge results when one side contributed empty values. [#72973](https://github.com/StarRocks/starrocks/pull/72973)
- An issue where `CREATE TABLE` for an Iceberg table failed with "Multiple entries with same key: format-version" when the user explicitly specified `format-version` in `PROPERTIES`. [#72828](https://github.com/StarRocks/starrocks/pull/72828)
- A `CompactionScheduler.startCompaction` lock scope that held a DB-wide READ lock across single-table critical work, blocking concurrent DDL on other tables in the same database. Switched to IS on DB plus READ on the target table. [#72178](https://github.com/StarRocks/starrocks/pull/72178)
- An issue where `StarMgrMetaSyncer.syncTableMetaInternal` and `syncTableColocationInfo` held DB READ/WRITE locks across external StarOS RPCs, freezing CREATE/DROP/ALTER/RENAME on every table in the database for the duration of each RPC. [#72108](https://github.com/StarRocks/starrocks/pull/72108)
- An issue where `StarMgrMetaSyncer.getAllPartitionShardGroupId` held the DB READ lock for full iteration over all cloud-native tables and physical partitions, stalling FE threads waiting for the DB write lock on large catalogs. [#71614](https://github.com/StarRocks/starrocks/pull/71614)
- A redundant DB READ lock in `getTableNamesViewWithLock`. The underlying `nameToTable` is a `ConcurrentHashMap`, so the enclosing lock added contention without correctness benefit. [#72042](https://github.com/StarRocks/starrocks/pull/72042)
- A DB WRITE lock in the read-only `/api/{db}/{table}/_count` REST endpoint that was unnecessary for computing `proximateRowCount()`. [#72053](https://github.com/StarRocks/starrocks/pull/72053)
- A batch publish deadlock caused by partition version gaps that operations like tablet split, schema change, and alter jobs reserved by advancing `nextVersion` without a matching publish. [#71483](https://github.com/StarRocks/starrocks/pull/71483)
- A deadlock in shared-nothing mode when warming up the LRU cache for rowset metadata while the cache was full. [#71459](https://github.com/StarRocks/starrocks/pull/71459)
- A `PipelineTimerTask` that could remain stuck in `waitUtilFinished` due to incorrect ordering between consumer registration and finished signaling. [#72058](https://github.com/StarRocks/starrocks/pull/72058)
- A condition race in `ConnectorSinkPassthroughExchanger::accept` that crashed BE with SIGSEGV via out-of-bounds vector access on `_writer_count`. [#71848](https://github.com/StarRocks/starrocks/pull/71848)
- A use-after-free in `LoadChannel::get_load_replica_status` caused by destruction of a temporary `shared_ptr`. [#71843](https://github.com/StarRocks/starrocks/pull/71843)
- A use-after-free in the information schema sink due to a missing reference count increment in async RPC closure handling. [#71513](https://github.com/StarRocks/starrocks/pull/71513)
- A BE crash in `reverse(DecimalV3)` caused by improper handling of decimal value width. [#71834](https://github.com/StarRocks/starrocks/pull/71834)
- A BE crash when `UNNEST` produced columns whose define-expression carried an ARRAY type, which was incompatible with global dictionary generation downstream. [#72027](https://github.com/StarRocks/starrocks/pull/72027)
- An NPE in FE when creating an Iceberg external table with invalid transform argument order such as `bucket(4, region)`; FE now returns a normal analyzer error. [#71917](https://github.com/StarRocks/starrocks/pull/71917)
- An issue where Iceberg manifest data file cache entries were missing column statistics when the first query against a table did not request stats (for example `SELECT *`). [#71913](https://github.com/StarRocks/starrocks/pull/71913)
- An issue where the Iceberg min/max optimization was silently skipped when the table was partitioned by `bucket(col, N)` because `PruneHDFSScanColumnRule` injected a placeholder materialized column. [#71863](https://github.com/StarRocks/starrocks/pull/71863)
- An issue where `AggregateJoinPushDownRule` failed to rewrite materialized views over Iceberg base tables because `Table.getId()` was compared instead of identity, and connector-table ids can shift across plan rebuilds. [#71856](https://github.com/StarRocks/starrocks/pull/71856)
- An issue where INSERT OVERWRITE into Hive dynamic partitions failed when the metastore listed a partition whose location no longer existed on the file system; the missing partition directory is now created before commit. [#71810](https://github.com/StarRocks/starrocks/pull/71810)
- A Parquet scanner failure (`Illegal converting from arrow type(dictionary) ...`) when Arrow returned dictionary-typed columns, including dictionaries nested inside arrays, structs, and maps. [#71855](https://github.com/StarRocks/starrocks/pull/71855)
- An issue where stale scan ranges from earlier batches persisted across `ColocatedBackendSelector.Assignment` incremental batches, causing files to be re-deployed and re-scanned. [#71789](https://github.com/StarRocks/starrocks/pull/71789)
- An issue where `PruneShuffleColumnRule` did not update the Join `outputProperty` after pruning Exchange shuffle columns, leading to incorrect downstream distribution. [#72003](https://github.com/StarRocks/starrocks/pull/72003)
- Incorrect shuffle distribution caused by a missing project node when `PushDownJoinOnExpressionToChildProject` was disabled during the first stage of multi-stage MV rewrite. [#71075](https://github.com/StarRocks/starrocks/pull/71075)
- Duplicate `Apply` attachments in `ReplaceSubqueryRewriteRule` when predicate normalization made the same scalar-subquery placeholder appear multiple times. [#71155](https://github.com/StarRocks/starrocks/pull/71155)
- A short-circuit issue in `EventScheduler` where a finished join probe could prevent the pipeline from transitioning to the finished state. [#71740](https://github.com/StarRocks/starrocks/pull/71740)
- An issue where AWS assume-role configured via `aws.s3.iam_role_arn` was not applied to JNI scanners (RCFile / Avro / SequenceFile / Hudi), causing S3 403 errors. [#71422](https://github.com/StarRocks/starrocks/pull/71422)
- An issue where Oracle JDBC predicate pushdown produced invalid SQL because date literals did not match the Oracle NLS format; literals are now emitted as `date '...'`. [#71412](https://github.com/StarRocks/starrocks/pull/71412)
- An issue in shared-data mode where a follower FE forwarded DDL to the leader and waited only for FE journal replay, missing the StarMgr journal and producing "no queryable replica" errors for queries that immediately followed table creation. [#71263](https://github.com/StarRocks/starrocks/pull/71263)
- An issue where `get_tablet_stats` for Primary Key tablets repeatedly reloaded the entire `TabletMetadata` for every segment via `get_del_vec_in_meta()`. [#71672](https://github.com/StarRocks/starrocks/pull/71672)
- An Arrow Flight issue where empty result sets returned column names of `r` because the placeholder name was emitted instead of the actual schema. [#71534](https://github.com/StarRocks/starrocks/pull/71534)
- An issue where `parallel_clone_task_per_path` updates did not include the store-path count when resizing the CLONE thread pool. [#71484](https://github.com/StarRocks/starrocks/pull/71484)
- An issue where the resource group user classifier rejected digit-leading usernames that `CREATE USER` allowed. The classifier now uses the same validation rule as `CREATE USER`. [#71470](https://github.com/StarRocks/starrocks/pull/71470)
- An issue where `HttpServerHandler.channelInactive` skipped `unregisterConnection` when `isRegistered()` was false, leaking connection-map entries for early-failing requests. [#72006](https://github.com/StarRocks/starrocks/pull/72006)
- An issue where Java UDF JNI calls (`NewObject`, `NewArray`, `NewStringUTF`, etc.) did not check for exceptions or null returns, leading to silent failures or undefined behavior. [#71734](https://github.com/StarRocks/starrocks/pull/71734)
- An issue where `be_tablets.DATA_SIZE` reported `total_disk_size` (including rowset-embedded indexes and the persistent PK index for lake PK tablets) instead of rowset column data bytes. [#70735](https://github.com/StarRocks/starrocks/pull/70735)
- A noisy "Failed to batch drop tablets" warning printed by `StarMgrMetaSyncer` even when there were no shards to delete. [#72209](https://github.com/StarRocks/starrocks/pull/72209)
- CVE-2026-42198 (pgjdbc) and CVE-2026-5598 (BouncyCastle): bumped `org.postgresql:postgresql` to 42.7.11 and BouncyCastle to 1.84. [#72797](https://github.com/StarRocks/starrocks/pull/72797)
- CVE in netty: upgraded netty to 4.1.133.Final. [#72905](https://github.com/StarRocks/starrocks/pull/72905)
- Cleaned broker CVEs by upgrading netty / jetty / awssdk / jackson dependencies in the broker. [#72184](https://github.com/StarRocks/starrocks/pull/72184)
- Upgraded jetty-http to 9.4.58.v20250814 to address known CVEs in the previous jetty-http version. [#71762](https://github.com/StarRocks/starrocks/pull/71762)
- Temporarily masked CVE-2026-2332 to unblock the build, since jetty 9.x is EOL and no upstream fix is published. [#71914](https://github.com/StarRocks/starrocks/pull/71914)

## 4.0.9

Release Date: April 16, 2026

### Behavior Changes

- When VARBINARY columns appear inside nested types (ARRAY, MAP, or STRUCT), StarRocks now correctly encodes the values in binary format in MySQL result sets. Previously, raw bytes were emitted directly, which could break text-protocol parsing for null bytes or non-printable characters. This change may affect downstream clients or tools that process VARBINARY data inside nested types. [#71346](https://github.com/StarRocks/starrocks/pull/71346)
- Routine Load jobs now automatically pause when a non-retryable error is encountered, such as a row causing the Primary Key size limit to be exceeded. Previously, the job would retry indefinitely because such errors were not recognized as non-retryable by the FE transaction status handler. [#71161](https://github.com/StarRocks/starrocks/pull/71161)
- `SHOW CREATE TABLE` and `DESC` statements now display the Primary Key columns for Paimon external tables. [#70535](https://github.com/StarRocks/starrocks/pull/70535)
- Cloud-native tablet metadata fetch operations (such as `get_tablet_stats` and `get_tablet_metadatas`) now use a dedicated thread pool instead of the shared `UPDATE_TABLET_META_INFO` pool. This prevents metadata fetch contention from impacting repair and other tasks. The new thread pool size is configurable via a new BE parameter. [#70492](https://github.com/StarRocks/starrocks/pull/70492)

### Improvements

- Added session variables to control the encoding behavior of VARBINARY values in MySQL protocol responses, providing fine-grained control over binary result encoding in client connections. [#71415](https://github.com/StarRocks/starrocks/pull/71415)
- Added a `snapshot_meta.json` marker file to cluster snapshots to support integrity validation before snapshot restoration. [#71209](https://github.com/StarRocks/starrocks/pull/71209)
- Added warning logs for silently swallowed exceptions in `WarehouseManager` to improve observability of silent failures. [#71215](https://github.com/StarRocks/starrocks/pull/71215)
- Added metrics for Iceberg metadata table queries to support performance monitoring and diagnosis. [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- The `regexp_replace()` function now supports constant folding during FE query planning, reducing planning overhead for queries with constant string arguments. [#70804](https://github.com/StarRocks/starrocks/pull/70804)
- Added categorized metrics for Iceberg time travel queries to improve monitoring and performance analysis. [#70788](https://github.com/StarRocks/starrocks/pull/70788)
- Added log output when update compaction is suspended, improving visibility into compaction lifecycle. [#70538](https://github.com/StarRocks/starrocks/pull/70538)
- `SHOW COLUMNS` now returns column comments for PostgreSQL external tables. [#70520](https://github.com/StarRocks/starrocks/pull/70520)
- Added support for dumping query execution plans when a query encounters an exception, improving diagnosability of runtime failures. [#70387](https://github.com/StarRocks/starrocks/pull/70387)
- Tablet deletion during DDL operations is now batched, reducing write lock contention on tablet metadata. [#70052](https://github.com/StarRocks/starrocks/pull/70052)
- Added a Force Drop recovery mechanism for synchronous materialized views that are stuck in an error state and cannot be dropped through normal means. [#70029](https://github.com/StarRocks/starrocks/pull/70029)

### Bug Fixes

The following issues have been fixed:

- An issue where the profile `START_TIME` and `END_TIME` were not displayed in the session timezone. [#71429](https://github.com/StarRocks/starrocks/pull/71429)
- A shared-object mutation bug in `PushDownAggregateRewriter` when processing CASE-WHEN/IF expressions, which could cause incorrect query results. [#71309](https://github.com/StarRocks/starrocks/pull/71309)
- A use-after-free bug in `ThreadPool::do_submit` triggered when thread creation fails. [#71276](https://github.com/StarRocks/starrocks/pull/71276)
- An issue where `information_schema.tables` did not properly escape special characters in equality predicates, causing incorrect results. [#71273](https://github.com/StarRocks/starrocks/pull/71273)
- An issue where the materialized view scheduler continued to run after the materialized view became inactive. [#71265](https://github.com/StarRocks/starrocks/pull/71265)
- Fixed a task signature collision in `UpdateTabletSchemaTask` across concurrent ALTER jobs that could cause schema update tasks to be skipped. [#71242](https://github.com/StarRocks/starrocks/pull/71242)
- An issue where row count estimation produced NaN values for histograms that contained only MCV (Most Common Values) entries. [#71241](https://github.com/StarRocks/starrocks/pull/71241)
- A missing dependency on the AWS S3 Transfer Manager in the AWS SDK integration. [#71230](https://github.com/StarRocks/starrocks/pull/71230)
- An issue where `TaskManager` scheduler callbacks did not verify whether the current node is the leader, potentially causing duplicate task execution on follower nodes. [#71156](https://github.com/StarRocks/starrocks/pull/71156)
- A thread-local context pollution issue where `ConnectContext` information was not cleared after a leader-forwarded request completed. [#71141](https://github.com/StarRocks/starrocks/pull/71141)
- An issue where the partition predicate was missing in short-circuit point lookups, causing incorrect query results. [#71124](https://github.com/StarRocks/starrocks/pull/71124)
- A NullPointerException when analyzing generated columns during Stream Load or Broker Load if a column referenced by the generated column expression was absent from the load schema. [#71116](https://github.com/StarRocks/starrocks/pull/71116)
- A use-after-free bug in the error handling path of parallel segment and rowset loading. [#71083](https://github.com/StarRocks/starrocks/pull/71083)
- An issue where delvec orphan entries were left behind when a write operation preceded compaction in the same publish batch. [#71049](https://github.com/StarRocks/starrocks/pull/71049)
- An issue where queries appeared in the `current_queries` result via HTTP loopback when checking query progress internally. [#71032](https://github.com/StarRocks/starrocks/pull/71032)
- CVE-2026-33870 and CVE-2026-33871. [#71017](https://github.com/StarRocks/starrocks/pull/71017)
- A read lock leak in `SharedDataStorageVolumeMgr`. [#70987](https://github.com/StarRocks/starrocks/pull/70987)
- An issue where the input and result columns of the `locate()` function shared the same NullColumn reference inside BinaryColumns, causing incorrect results. [#70957](https://github.com/StarRocks/starrocks/pull/70957)
- An issue where safe tablet deletion checks were incorrectly applied during ALTER operations in share-nothing mode. [#70934](https://github.com/StarRocks/starrocks/pull/70934)
- A race condition in `_all_global_rf_ready_or_timeout` that could prevent global runtime filters from being applied correctly. [#70920](https://github.com/StarRocks/starrocks/pull/70920)
- An int32 overflow in the `ACCUMULATED` metric macro that caused metric values to silently overflow. [#70889](https://github.com/StarRocks/starrocks/pull/70889)
- Incorrect aggregation results in dictionary-encoded merge GROUP BY queries. [#70866](https://github.com/StarRocks/starrocks/pull/70866)
- CVE-2025-54920. [#70862](https://github.com/StarRocks/starrocks/pull/70862)
- A potential data loss issue in aggregation spill caused by incorrect hash table state handling during `set_finishing`. [#70851](https://github.com/StarRocks/starrocks/pull/70851)
- An issue where the `content-length` header was not reset when `proxy_pass_request_body` is disabled. [#70821](https://github.com/StarRocks/starrocks/pull/70821)
- An issue where the spill directory for load operations was cleaned up in the object destructor rather than during `DeltaWriter::close()`, potentially causing premature deletion of spill data. [#70778](https://github.com/StarRocks/starrocks/pull/70778)
- An issue where `INSERT INTO ... BY NAME` from `FILES()` did not correctly push down the schema for partial column sets. [#70774](https://github.com/StarRocks/starrocks/pull/70774)
- An issue where connector scan nodes did not reset the scan range source on query retry, causing incorrect results upon retry. [#70762](https://github.com/StarRocks/starrocks/pull/70762)
- A potential rowset metadata loss for Primary Key model tablets caused by a GC race during disk re-migration of the form A→B→A. [#70727](https://github.com/StarRocks/starrocks/pull/70727)
- An issue where a query-scoped warehouse hint leaked the `ComputeResource` object in `ConnectContext`, potentially affecting subsequent queries on the same connection. [#70706](https://github.com/StarRocks/starrocks/pull/70706)
- An issue where redundant conjuncts in `MySqlScanNode` and `JDBCScanNode` caused BE errors related to `VectorizedInPredicate` type mismatches. [#70694](https://github.com/StarRocks/starrocks/pull/70694)
- A missing `libssl-dev` dependency in the Ubuntu runtime environment. [#70688](https://github.com/StarRocks/starrocks/pull/70688)
- An issue where Iceberg manifest cache completeness was not validated on read, leading to incorrect scan results when the cache was partially populated. [#70675](https://github.com/StarRocks/starrocks/pull/70675)
- A duplicate closure reference in `_tablet_multi_get_rpc` that could cause use-after-free. [#70657](https://github.com/StarRocks/starrocks/pull/70657)
- Partial manifest cache writes in the Iceberg `ManifestReader` that could result in incomplete cache entries and incorrect scan behavior. [#70652](https://github.com/StarRocks/starrocks/pull/70652)
- A crash in `array_map()` when processing arrays that contain null literal elements. [#70629](https://github.com/StarRocks/starrocks/pull/70629)
- A stack overflow in the `to_base64()` function when processing large inputs. [#70623](https://github.com/StarRocks/starrocks/pull/70623)
- An issue where `INSERT INTO ... BY NAME` from `FILES()` used positional column mapping instead of name-based mapping, causing data to be written to incorrect columns. [#70622](https://github.com/StarRocks/starrocks/pull/70622)
- An issue where `NOT NULL` constraints were incorrectly pushed down into the schema inferred from `FILES()`, causing load failures for nullable columns. [#70621](https://github.com/StarRocks/starrocks/pull/70621)
- An issue where precise external materialized view refresh did not fall back correctly for Iceberg-like connectors. [#70589](https://github.com/StarRocks/starrocks/pull/70589)
- A `num_short_key_columns` mismatch when constructing a partial tablet schema, which could cause data read errors. [#70586](https://github.com/StarRocks/starrocks/pull/70586)
- A BE crash that occurred when the child iterator was exhausted in `MaskMergeIterator`. [#70539](https://github.com/StarRocks/starrocks/pull/70539)
- An issue where materialized view refresh jobs repeatedly refreshed partitions whose corresponding Iceberg snapshots had expired. [#70523](https://github.com/StarRocks/starrocks/pull/70523)
- An issue where starlet configuration parameters could not be set. [#70482](https://github.com/StarRocks/starrocks/pull/70482)
- An issue where the lock-free materialized view rewrite path incorrectly fell back to live metadata, causing inconsistent rewrite behavior. [#70475](https://github.com/StarRocks/starrocks/pull/70475)
- An issue in `JoinHashTable::merge_ht` where dummy rows were not skipped for expression-based join key columns, causing incorrect join results. [#70465](https://github.com/StarRocks/starrocks/pull/70465)
- An incorrect equality comparison in `InformationFunction` that could produce wrong results in certain queries. [#70464](https://github.com/StarRocks/starrocks/pull/70464)
- A column type mismatch in the `__iceberg_transform_bucket` internal function. [#70443](https://github.com/StarRocks/starrocks/pull/70443)
- An issue where Iceberg materialized view refresh failed when Iceberg snapshot timestamps were non-monotonic. [#70382](https://github.com/StarRocks/starrocks/pull/70382)
- An issue where user authentication credentials were exposed in audit logs and SQL redaction output. [#70360](https://github.com/StarRocks/starrocks/pull/70360)
- A CN crash that occurred when scanning an empty tablet with physical split enabled. [#70281](https://github.com/StarRocks/starrocks/pull/70281)
- An issue where the VARCHAR column length was not preserved after a redundant CAST was eliminated during query optimization. [#70269](https://github.com/StarRocks/starrocks/pull/70269)
- An issue where brpc connection retry logic did not correctly handle a wrapped `NoSuchElementException`, causing connection failures after the retry attempt. [#70203](https://github.com/StarRocks/starrocks/pull/70203)
- An issue where null fractions for outer join columns were not preserved during statistics estimation, leading to suboptimal query plans. [#70144](https://github.com/StarRocks/starrocks/pull/70144)
- A memory tracker leak in connector sink operations running on poller threads. [#70121](https://github.com/StarRocks/starrocks/pull/70121)

## 4.0.8

Release Date: March 25, 2026

### Behavior Changes

- Improved `sql_mode` handling: when `DIVISION_BY_ZERO` or `FAIL_PARSE_DATE` mode is set, division by zero and date parse failures in `str_to_date`/`str2date` now return an error instead of being silently ignored. [#70004](https://github.com/StarRocks/starrocks/pull/70004)
- When `sql_mode` is set to `FORBID_INVALID_DATE`, invalid dates in `INSERT VALUES` clauses are now correctly rejected instead of being bypassed. [#69803](https://github.com/StarRocks/starrocks/pull/69803)
- Expression partition generated columns are now hidden from `DESC` and `SHOW CREATE TABLE` output. [#69793](https://github.com/StarRocks/starrocks/pull/69793)
- Client ID is no longer included in audit logs. [#69383](https://github.com/StarRocks/starrocks/pull/69383)

### Improvements

- Added a configuration item `local_exchange_buffer_mem_limit_per_driver` to limit the local exchange buffer size to `dop * local_exchange_buffer_mem_limit_per_driver`. [#70393](https://github.com/StarRocks/starrocks/pull/70393)
- Cached file existence check results across versions in `check_missing_files` to reduce redundant storage I/O. [#70364](https://github.com/StarRocks/starrocks/pull/70364)
- Allowed disabling split and reverse scan ranges for descending TopN runtime filters when `desc_hint_split_range` is set to ≤ 0. [#70307](https://github.com/StarRocks/starrocks/pull/70307)
- Added `EXPLAIN` and `EXPLAIN ANALYZE` support for `INSERT` statements in the Trino dialect. [#70174](https://github.com/StarRocks/starrocks/pull/70174)
- Optimized Iceberg read performance when position deletes are present. [#69717](https://github.com/StarRocks/starrocks/pull/69717)
- Optimized materialized view best-selector strategy based on distributed keys to improve materialized view selection accuracy. [#69679](https://github.com/StarRocks/starrocks/pull/69679)

### Bug Fixes

The following issues have been fixed:

- JDBC MySQL pushdown failing for unsupported cast operations. [#70415](https://github.com/StarRocks/starrocks/pull/70415)
- Type mismatch issues in materialized view refresh. Added `mv_refresh_force_partition_type` configuration to force partition type in materialized view refresh. [#70381](https://github.com/StarRocks/starrocks/pull/70381)
- `dataVersion` not set correctly when restoring from backup. [#70373](https://github.com/StarRocks/starrocks/pull/70373)
- Duplicated partition names in materialized view refresh tasks. [#70354](https://github.com/StarRocks/starrocks/pull/70354)
- Incorrect SLF4J parameterized logging using string concatenation instead of placeholder arguments. [#70330](https://github.com/StarRocks/starrocks/pull/70330)
- Comment not set when creating Hive tables. [#70318](https://github.com/StarRocks/starrocks/pull/70318)
- `FileSystemExpirationChecker` blocking on slow HDFS close operations. [#70311](https://github.com/StarRocks/starrocks/pull/70311)
- Distribute column validation not applied across different partitions in `OlapTableSink`. [#70310](https://github.com/StarRocks/starrocks/pull/70310)
- Constant folding producing INF instead of an error when double addition overflows. [#70309](https://github.com/StarRocks/starrocks/pull/70309)
- Typo in Iceberg table creation: field `common` was used instead of `comment`. [#70267](https://github.com/StarRocks/starrocks/pull/70267)
- Root user not bypassing all Ranger permission checks in some scenarios. [#70254](https://github.com/StarRocks/starrocks/pull/70254)
- `query_pool` memory tracker going negative during data ingestion. [#70228](https://github.com/StarRocks/starrocks/pull/70228)
- `AuditEventProcessor` thread exiting due to `OutOfMemoryException`. [#70206](https://github.com/StarRocks/starrocks/pull/70206)
- `SplitTopNRule` not applying partition pruning correctly. [#70154](https://github.com/StarRocks/starrocks/pull/70154)
- Out-of-bounds access in `cal_new_base_version` during schema change publish. [#70132](https://github.com/StarRocks/starrocks/pull/70132)
- Materialied view rewrite ignoring dropped partitions from the base table. [#70130](https://github.com/StarRocks/starrocks/pull/70130)
- Unexpected partition predicate pruning due to type mismatch in boundary comparisons. [#70097](https://github.com/StarRocks/starrocks/pull/70097)
- `str_to_date` losing microsecond precision in BE runtime. [#70068](https://github.com/StarRocks/starrocks/pull/70068)
- Join spill process crashing in `set_callback_function`. [#70030](https://github.com/StarRocks/starrocks/pull/70030)
- Broker Load failing GCS authentication after `gcs-connector` upgrade to version 3.0.13. [#70012](https://github.com/StarRocks/starrocks/pull/70012)
- DCHECK failure in `DeltaWriter::close()` when called from a bthread context. [#69960](https://github.com/StarRocks/starrocks/pull/69960)
- Use-after-free race condition in `AsyncDeltaWriter` close/finish lifecycle. [#69940](https://github.com/StarRocks/starrocks/pull/69940)
- Race condition causing write transaction edit log entry to be missed. [#69899](https://github.com/StarRocks/starrocks/pull/69899)
- Known CVE vulnerabilities. [#69863](https://github.com/StarRocks/starrocks/pull/69863)
- Follower FE not waiting for journal replay in `changeCatalogDb`. [#69834](https://github.com/StarRocks/starrocks/pull/69834)
- Incorrect `LIKE` pattern matching with backslash escape sequences. [#69775](https://github.com/StarRocks/starrocks/pull/69775)
- Expression analysis failure after renaming a partition column. [#69771](https://github.com/StarRocks/starrocks/pull/69771)
- Use-after-free crash in `AsyncDeltaWriter::close`. [#69770](https://github.com/StarRocks/starrocks/pull/69770)
- Crash in local partition TopN execution. [#69752](https://github.com/StarRocks/starrocks/pull/69752)
- Incorrect behavior in `PartitionColumnMinMaxRewriteRule` caused by `Partition.hasStorageData`. [#69751](https://github.com/StarRocks/starrocks/pull/69751)
- Duplicated CSV compression suffix in file sink output filenames. [#69749](https://github.com/StarRocks/starrocks/pull/69749)
- `lake_capture_tablet_and_rowsets` not gated behind an experimental configuration flag. [#69748](https://github.com/StarRocks/starrocks/pull/69748)
- Incorrect partition min pruning with shadow partitions. [#69641](https://github.com/StarRocks/starrocks/pull/69641)
- Java UDTF/UDAF crashing when method parameters use generic types. [#69197](https://github.com/StarRocks/starrocks/pull/69197)
- Per-query metadata not released after query planning, causing FE OOM during concurrent query execution. [#68444](https://github.com/StarRocks/starrocks/pull/68444)
- Query-scope warehouse hint leaking `ComputeResource` in `ConnectContext`. [#70706](https://github.com/StarRocks/starrocks/pull/70706)
- Lock-free materialized view rewrite incorrectly falling back to live metadata. [#70475](https://github.com/StarRocks/starrocks/pull/70475)
- Duplicate closure reference in `_tablet_multi_get_rpc`. [#70657](https://github.com/StarRocks/starrocks/pull/70657)
- Infinite recursion in `ReplaceColumnRefRewriter`. [#66974](https://github.com/StarRocks/starrocks/pull/66974)
- `NOT NULL` constraint incorrectly pushed down to `FILES()` table function schema. [#70621](https://github.com/StarRocks/starrocks/pull/70621)
- `num_short_key_columns` mismatch in partial tablet schema. [#70586](https://github.com/StarRocks/starrocks/pull/70586)
- `COLUMN_UPSERT_MODE` checksum error in shared-data clusters. [#65320](https://github.com/StarRocks/starrocks/pull/65320)
- Column type mismatch for `__iceberg_transform_bucket`. [#70443](https://github.com/StarRocks/starrocks/pull/70443)
- Starlet configuration items not taking effect. [#70482](https://github.com/StarRocks/starrocks/pull/70482)
- DCG data not read correctly when switching from column mode to row mode in partial update. [#61529](https://github.com/StarRocks/starrocks/pull/61529)

## 4.0.7

Release Date: March 12, 2026

### Behavior Change

- Disallowed creating materialized views based on Iceberg views. [#69471](https://github.com/StarRocks/starrocks/pull/69471)
- Fixed inconsistencies in multi-statement Stream Load transaction behavior. [#68542](https://github.com/StarRocks/starrocks/pull/68542)

### Improvements

- Added fine-grained trace counters for `LakePersistentIndex` in the Publish phase. [#69640](https://github.com/StarRocks/starrocks/pull/69640)
- Triggered early flush in `LakePersistentIndex` when rebuild row count exceeds the threshold. [#69698](https://github.com/StarRocks/starrocks/pull/69698)
- Added `dump_lake_persistent_index_sst` operation to `meta_tool`. [#69682](https://github.com/StarRocks/starrocks/pull/69682)
- Improved `REPAIR TABLE` functionality and `SHOW TABLET` status display. [#69656](https://github.com/StarRocks/starrocks/pull/69656)
- Supports `ADMIN SHOW TABLET STATUS` for cloud-native tables. [#69616](https://github.com/StarRocks/starrocks/pull/69616)
- Upgraded `hadoop-client` from 3.4.2 to 3.4.3. [#69503](https://github.com/StarRocks/starrocks/pull/69503)
- Prevented crashes when deserialization mismatches occur. [#69481](https://github.com/StarRocks/starrocks/pull/69481)
- Pushed down predicates to FE when querying `information_schema.loads`. [#69472](https://github.com/StarRocks/starrocks/pull/69472)
- Optimized the SQL displayed for materialized view refresh TaskRuns. [#69437](https://github.com/StarRocks/starrocks/pull/69437)
- Bypassed caching in `CachingIcebergCatalog` when vended credentials are enabled. [#69434](https://github.com/StarRocks/starrocks/pull/69434)
- Uses `tryLock` with timeout in `canTxnFinished` to reduce lock contention. [#69427](https://github.com/StarRocks/starrocks/pull/69427)
- Added a global readonly variable `@@run_mode`. [#69247](https://github.com/StarRocks/starrocks/pull/69247)
- Uses Estimator to estimate cache entry weight in `DeltaLakeMetastore`. [#69244](https://github.com/StarRocks/starrocks/pull/69244)
- Added resource share type support for AWS Glue `GetDatabases` API. [#69056](https://github.com/StarRocks/starrocks/pull/69056)
- Extracted range predicates from scalar-subqueries containing `convert_tz`. [#69055](https://github.com/StarRocks/starrocks/pull/69055)
- Added ByteBuffer Estimator. [#69042](https://github.com/StarRocks/starrocks/pull/69042)
- Supports fast cancel for Lake DeltaWriter in shared-data clusters. [#68877](https://github.com/StarRocks/starrocks/pull/68877)
- Supports an interface to add physical partitions for random distribution tables. [#68503](https://github.com/StarRocks/starrocks/pull/68503)
- Gated SQL transactions behind the session variable `enable_sql_transaction` (default: true). [#63535](https://github.com/StarRocks/starrocks/pull/63535)
- Added partition scan number limit when querying external tables. [#68480](https://github.com/StarRocks/starrocks/pull/68480)

### Bug Fixes

The following issues have been fixed:

- Incorrect value for the metric `g_publish_version_failed_tasks` when the resource is busy. [#69526](https://github.com/StarRocks/starrocks/pull/69526)
- NPE in `IcebergCatalog.getPartitionLastUpdatedTime` when the snapshot has expired. [#68925](https://github.com/StarRocks/starrocks/pull/68925)
- DCHECK failure in `DeltaWriter::close()` when called from bthread context. [#70057](https://github.com/StarRocks/starrocks/pull/70057)
- Several use-after-free issues. [#69968](https://github.com/StarRocks/starrocks/pull/69968)
- Use-after-free race in AsyncDeltaWriter close/finish lifecycle. [#69961](https://github.com/StarRocks/starrocks/pull/69961)
- Corrupted cache for PK SST tables is not clraered. [#69693](https://github.com/StarRocks/starrocks/pull/69693)
- `AsyncFlushOutputStream` use-after-free issue. [#69688](https://github.com/StarRocks/starrocks/pull/69688)
- Retention clock reset issue and incomplete scan in `disableRecoverPartitionWithSameName`. [#69677](https://github.com/StarRocks/starrocks/pull/69677)
- NPE in `StreamLoadMultiStmtTask.cancelAfterRestart` after deserialization. [#69662](https://github.com/StarRocks/starrocks/pull/69662)
- Unnecessary RPCs and metadata queries caused by the incorrect logic of `SchemaBeTabletsScanner`. [#69645](https://github.com/StarRocks/starrocks/pull/69645)
- Graceful exit caused different transactions to publish the same version. [#69639](https://github.com/StarRocks/starrocks/pull/69639)
- `TabletUpdates::get_column_values` crashes with SIGSEGV when the Primary Key Index contains stale entries that point to rowsets that have been compacted. [#69617](https://github.com/StarRocks/starrocks/pull/69617)
- `KILL ANALYZE` fails to stop `ANALYZE TABLE` tasks. [#69592](https://github.com/StarRocks/starrocks/pull/69592)
- Unexpected behavior because not all exceptions of RowGroupWriter are caught. [#69568](https://github.com/StarRocks/starrocks/pull/69568)
- TaskRun warehouse display issues after changing the warehouse for the materialized view. [#69567](https://github.com/StarRocks/starrocks/pull/69567)
- Sort key does not include newly added key columns after schema change on aggregate and unique tables. [#69529](https://github.com/StarRocks/starrocks/pull/69529)
- Issues caused by `isInternalCancelError` using `equals`. [#69523](https://github.com/StarRocks/starrocks/pull/69523)
- TaskManager scheduling bugs after `ALTER MATERIALIZED VIEW`. [#69504](https://github.com/StarRocks/starrocks/pull/69504)
- Pipeline will be blocked or crash because not all exceptions of `ParquetFileWriter::close` are caught. [#69492](https://github.com/StarRocks/starrocks/pull/69492)
- Materialized view force refresh bugs for partitioned tables. [#69488](https://github.com/StarRocks/starrocks/pull/69488)
- Incorrect status was returned when certain writers failed to flush data. [#69473](https://github.com/StarRocks/starrocks/pull/69473)
- Rowset files were deleted when Primary Key tablets were moved to trash. [#69438](https://github.com/StarRocks/starrocks/pull/69438)
- INSERT failure when the range of an automatic partition is enclosed by an existing merged partition. [#69429](https://github.com/StarRocks/starrocks/pull/69429)
- Materialized view tablet meta inconsistency between FE leader and follower. [#69428](https://github.com/StarRocks/starrocks/pull/69428)
- Concurrency bugs related to function fields. [#69315](https://github.com/StarRocks/starrocks/pull/69315)
- Premature deletion of data because rollup handler's active transaction ID is not considered in `computeMinActiveTxnId`. [#69285](https://github.com/StarRocks/starrocks/pull/69285)
- Lock leak in `addPartitions` caused by name-based table lookup after concurrent SWAP. [#69284](https://github.com/StarRocks/starrocks/pull/69284)
- `DROP FUNCTION IF EXISTS` ignored the `ifExists` flag. [#69216](https://github.com/StarRocks/starrocks/pull/69216)
- Inconsistent behavior between StarRocks and MySQL-compatible syntax when `CAST(... AS SIGNED)` in TypeParser. [#69181](https://github.com/StarRocks/starrocks/pull/69181)
- Issue with case-insensitive partition lookup in query table copy. [#69173](https://github.com/StarRocks/starrocks/pull/69173)
- Missing aggregate function when MIN/MAX stats rewrite failed. [#69149](https://github.com/StarRocks/starrocks/pull/69149)
- CVE-2025-67721. [#69138](https://github.com/StarRocks/starrocks/pull/69138)
- All-null value handling bug in synchronous materialized views. [#69136](https://github.com/StarRocks/starrocks/pull/69136)
- Projection loss in materialized view rewrite due to shared mutable state. [#69063](https://github.com/StarRocks/starrocks/pull/69063)
- Incorrect estimation of the Iceberg cache Weigher. [#69058](https://github.com/StarRocks/starrocks/pull/69058)
- `FULL OUTER JOIN USING` issue with constant subqueries. [#69028](https://github.com/StarRocks/starrocks/pull/69028)
- Issue with `DISTINCT ORDER BY` alias resolution for duplicated constants. [#69014](https://github.com/StarRocks/starrocks/pull/69014)
- Issue when materialized view visits external catalog on reload. [#68926](https://github.com/StarRocks/starrocks/pull/68926)
- NPE in Iceberg `getPartitions`. [#68907](https://github.com/StarRocks/starrocks/pull/68907)
- The container were not properly included in the Azure ABFS/WASB FileSystem cache key. [#68901](https://github.com/StarRocks/starrocks/pull/68901)
- Erroneous query results after modifying `CHAR` column length in shared-data clusters. [#68808](https://github.com/StarRocks/starrocks/pull/68808)
- Case-insensitive issue with username in LDAP authentication. [#67966](https://github.com/StarRocks/starrocks/pull/67966)
- Partitions could not be created after adding `storage_cooldown_ttl` to a table. [#60290](https://github.com/StarRocks/starrocks/pull/60290)

## 4.0.6

Release Date: February 14, 2026

### Improvements

- Support Partition Transforms with parentheses when creating Iceberg tables (for example, `PARTITION BY (bucket(k1, 3))`). [#68945](https://github.com/StarRocks/starrocks/pull/68945)
- Removed the restriction that partition columns in Iceberg tables must be at the end of the column list; they can now be defined at any position. [#68340](https://github.com/StarRocks/starrocks/pull/68340)
- Introduced host-level sorting for Iceberg table sink, controlled by the system variable `connector_sink_sort_scope` (Default: FILE), to organize data layout for better read performance. [#68121](https://github.com/StarRocks/starrocks/pull/68121)
- Improved error messages for Iceberg partition transform functions (for example, `bucket`, `truncate`) when the argument count is incorrect. [#68349](https://github.com/StarRocks/starrocks/pull/68349)
- Refactored table property handling to improve support for different file formats (ORC/Parquet) and compression codecs in Iceberg tables. [#68588](https://github.com/StarRocks/starrocks/pull/68588)
- Added table-level query timeout configuration `table_query_timeout` for fine-grained control (Priority: Session &gt; Table &gt; Cluster). [#67547](https://github.com/StarRocks/starrocks/pull/67547)
- Supports the `ADMIN SHOW AUTOMATED CLUSTER SNAPSHOT` statement to view automated snapshot status and schedule. [#68455](https://github.com/StarRocks/starrocks/pull/68455)
- Supports displaying the original user-defined SQL with comments in `SHOW CREATE VIEW`. [#68040](https://github.com/StarRocks/starrocks/pull/68040)
- Exposed Merge Commit-enabled Stream Load tasks in `information_schema.loads` for better observability. [#67879](https://github.com/StarRocks/starrocks/pull/67879)
- Introduced FE memory estimation utility API `/api/memory_usage`. [#68287](https://github.com/StarRocks/starrocks/pull/68287)
- Reduced unnecessary logging in `CatalogRecycleBin` during partition recycling. [#68533](https://github.com/StarRocks/starrocks/pull/68533)
- Triggered refresh of related asynchronous materialized views when the base table undergoes Swap/Drop/Replace Partition operations. [#68430](https://github.com/StarRocks/starrocks/pull/68430)
- Supports `VARBINARY` type for `count distinct`-like aggregate functions. [#68442](https://github.com/StarRocks/starrocks/pull/68442)
- Enhanced expression statistics to propagate histogram MCV for semantics-safe expressions (for example, `cast(k as bigint) + 10`) to improve skew detection. [#68292](https://github.com/StarRocks/starrocks/pull/68292)

### Bug Fixes

The following issues have been fixed:

- Potential crashes in Skew Join V2 runtime filters. [#67611](https://github.com/StarRocks/starrocks/pull/67611)
- Join predicate type mismatch (for example, INT = VARCHAR) caused by low-cardinality rewriting. [#68568](https://github.com/StarRocks/starrocks/pull/68568)
- Issues in query queue allocation time and pending timeout logic. [#65802](https://github.com/StarRocks/starrocks/pull/65802)
- `unique_id` conflict for Flat JSON extended columns after schema changes. [#68279](https://github.com/StarRocks/starrocks/pull/68279)
- Concurrent partition access issues in `OlapTableSink.complete()`. [#68853](https://github.com/StarRocks/starrocks/pull/68853)
- Incorrect metadata tracking when restoring manually downloaded cluster snapshots. [#68368](https://github.com/StarRocks/starrocks/pull/68368)
- Double slashes in backup paths when the repository location ends with `/`. [#68764](https://github.com/StarRocks/starrocks/pull/68764)
- OBS AK/SK credentials in the `SHOW CREATE CATALOG` output were not masked. [#65462](https://github.com/StarRocks/starrocks/pull/65462)

## 4.0.5

Release Date: February 3, 2026

### Improvements

- Bumped Paimon version to 1.3.1. [#67098](https://github.com/StarRocks/starrocks/pull/67098)
- Restored missing optimizations in DP statistics estimation to reduce redundant calculations. [#67852](https://github.com/StarRocks/starrocks/pull/67852)
- Improved pruning in DP Join reorder to skip expensive candidate plans earlier. [#67828](https://github.com/StarRocks/starrocks/pull/67828)
- Optimized JoinReorderDP partition enumeration to reduce object allocation and added an atom count cap (&le; 62). [#67643](https://github.com/StarRocks/starrocks/pull/67643)
- Optimized DP join reorder pruning and added checks to BitSet to reduce stream operation overhead. [#67644](https://github.com/StarRocks/starrocks/pull/67644)
- Skipped predicate column statistics collection during DP statistics estimation to reduce CPU overhead. [#67663](https://github.com/StarRocks/starrocks/pull/67663)
- Optimized correlated Join row count estimation to avoid repeatedly building `Statistics` objects. [#67773](https://github.com/StarRocks/starrocks/pull/67773)
- Reduced memory allocations in `Statistics.getUsedColumns`. [#67786](https://github.com/StarRocks/starrocks/pull/67786)
- Avoided redundant `Statistics` map copies when only row counts are updated. [#67777](https://github.com/StarRocks/starrocks/pull/67777)
- Skipped aggregate pushdown logic when no aggregation exists in the query to reduce overhead. [#67603](https://github.com/StarRocks/starrocks/pull/67603)
- Improved COUNT DISTINCT over windows, added support for fused multi-distinct aggregations, and optimized CTE generation. [#67453](https://github.com/StarRocks/starrocks/pull/67453)
- Supports `map_agg` function in the Trino dialect. [#66673](https://github.com/StarRocks/starrocks/pull/66673)
- Supports batching retrieval of LakeTablet location information during physical planning to reduce RPC calls in shared-data clusters. [#67325](https://github.com/StarRocks/starrocks/pull/67325)
- Added a thread pool for Publish Version transactions to shared-nothing clusters to improve concurrency. [#67797](https://github.com/StarRocks/starrocks/pull/67797)
- Optimized LocalMetastore locking granularity by replacing database-level locks with table-level locks. [#67658](https://github.com/StarRocks/starrocks/pull/67658)
- Refactored MergeCommitTask lifecycle management and added support for task cancellation. [#67425](https://github.com/StarRocks/starrocks/pull/67425)
- Supports intervals for automated cluster snapshots. [#67525](https://github.com/StarRocks/starrocks/pull/67525)
- Automatically cleaned up unused `mem_pool` entries in MemTrackerManager. [#67347](https://github.com/StarRocks/starrocks/pull/67347)
- Ignored `information_schema` queries during warehouse idle checks. [#67958](https://github.com/StarRocks/starrocks/pull/67958)
- Supports dynamically enabling global shuffle for Iceberg table sinks based on data distribution. [#67442](https://github.com/StarRocks/starrocks/pull/67442)
- Added Profile metrics for connector sink modules. [#67761](https://github.com/StarRocks/starrocks/pull/67761)
- Improved the collection and display of load spill metrics in Profiles, distinguishing between local and remote I/O. [#67527](https://github.com/StarRocks/starrocks/pull/67527)
- Changed Async-Profiler log level to Error to avoid repeating warning logs. [#67297](https://github.com/StarRocks/starrocks/pull/67297)
- Notified Starlet during BE shutdown to report SHUTDOWN status to StarMgr. [#67461](https://github.com/StarRocks/starrocks/pull/67461)

### Bug Fixes

The following issues have been fixed:

- Lacking support for legal simple paths containing hyphens (`-`). [#67988](https://github.com/StarRocks/starrocks/pull/67988)
- Runtime error when aggregate pushdown occurred on grouping keys involving JSON types. [#68142](https://github.com/StarRocks/starrocks/pull/68142)
- Issue where JSON path rewrite rules incorrectly pruned partition columns referenced in partition predicates. [#67986](https://github.com/StarRocks/starrocks/pull/67986)
- Type mismatch issue when rewriting simple aggregation using statistics. [#67829](https://github.com/StarRocks/starrocks/pull/67829)
- Potential heap-buffer-overflow in partition Joins. [#67435](https://github.com/StarRocks/starrocks/pull/67435)
- Duplicate `slot_ids` introduced when pushing down heavy expressions. [#67477](https://github.com/StarRocks/starrocks/pull/67477)
- Division-by-zero error in ExecutionDAG fragment connection for lacking precondition checks. [#67918](https://github.com/StarRocks/starrocks/pull/67918)
- Potential issues caused by fragment parallel prepare for single BE. [#67798](https://github.com/StarRocks/starrocks/pull/67798)
- Operator terminates incorrectly for lacking `set_finished` method for RawValuesSourceOperator. [#67609](https://github.com/StarRocks/starrocks/pull/67609)
- BE crash caused by unsupported DECIMAL256 type (precision > 38) in column aggregators. [#68134](https://github.com/StarRocks/starrocks/pull/68134)
- Shared-data clusters lack support for Fast Schema Evolution v2 over DELETE operations by carrying `schema_key` in requests. [#67456](https://github.com/StarRocks/starrocks/pull/67456)
- Shared-data clusters lack support for Fast Schema Evolution v2 over synchronous materialized views and traditional schema changes. [#67443](https://github.com/StarRocks/starrocks/pull/67443)
- Vacuum might accidentally delete files when file bundling is disabled during FE downgrade. [#67849](https://github.com/StarRocks/starrocks/pull/67849)
- Incorrect graceful exit handling in MySQLReadListener. [#67917](https://github.com/StarRocks/starrocks/pull/67917)

## 4.0.4

Release Date: January 16, 2026

### Improvements

- Supports Parallel Prepare for Operators and Drivers, and single-node batch fragment deployment to improve query scheduling performance. [#63956](https://github.com/StarRocks/starrocks/pull/63956)
- Optimized `deltaRows` calculation with lazy evaluation for large partition tables. [#66381](https://github.com/StarRocks/starrocks/pull/66381)
- Optimized Flat JSON processing with sequential iteration and improved path derivation. [#66941](https://github.com/StarRocks/starrocks/pull/66941) [#66850](https://github.com/StarRocks/starrocks/pull/66850)
- Supports releasing Spill Operator memory earlier to reduce memory usage in group execution. [#66669](https://github.com/StarRocks/starrocks/pull/66669)
- Optimized the logic to reduce string comparison overhead. [#66570](https://github.com/StarRocks/starrocks/pull/66570)
- Improved skew detection in `GroupByCountDistinctDataSkewEliminateRule` and `SkewJoinOptimizeRule` to support histogram and NULL-based strategies. [#66640](https://github.com/StarRocks/starrocks/pull/66640) [#67100](https://github.com/StarRocks/starrocks/pull/67100)
- Enhanced Column ownership management in Chunk using Move semantics to reduce Copy-On-Write overhead. [#66805](https://github.com/StarRocks/starrocks/pull/66805)
- For shared-data clusters, added FE `TableSchemaService` and updated `MetaScanNode` to support Fast Schema Evolution v2 schema retrieval. [#66142](https://github.com/StarRocks/starrocks/pull/66142) [#66970](https://github.com/StarRocks/starrocks/pull/66970)
- Supports multi-warehouse Backend resource statistics and parallelism (DOP) calculation for better resource isolation. [#66632](https://github.com/StarRocks/starrocks/pull/66632)
- Supports configuring Iceberg split size via StarRocks session variable `connector_huge_file_size`. [#67044](https://github.com/StarRocks/starrocks/pull/67044)
- Supports label-formatted statistics in `QueryDumpDeserializer`. [#66656](https://github.com/StarRocks/starrocks/pull/66656)
- Added an FE configuration `lake_enable_fullvacuum` (Default: `false`) to allow disabling Full Vacuum in shared-data clusters. [#63859](https://github.com/StarRocks/starrocks/pull/63859)
- Upgraded lz4 dependency to v1.10.0. [#67045](https://github.com/StarRocks/starrocks/pull/67045)
- Added fallback logic for sample-type cardinality estimation when row count is 0. [#65599](https://github.com/StarRocks/starrocks/pull/65599)
- Validated Strict Weak Ordering property for lambda comparator in `array_sort`. [#66951](https://github.com/StarRocks/starrocks/pull/66951)
- Optimized error messages when fetching external table metadata (Delta/Hive/Hudi/Iceberg) fails, showing root causes. [#66916](https://github.com/StarRocks/starrocks/pull/66916)
- Supports dumping pipeline status on query timeout and cancelling with `TIMEOUT` state in FE. [#66540](https://github.com/StarRocks/starrocks/pull/66540)
- Displays matched rule index in SQL blacklist error messages. [#66618](https://github.com/StarRocks/starrocks/pull/66618)
- Added labels to column statistics in `EXPLAIN` output. [#65899](https://github.com/StarRocks/starrocks/pull/65899)
- Filtered out "cancel fragment" logs for normal query completions (for example, LIMIT reached). [#66506](https://github.com/StarRocks/starrocks/pull/66506)
- Reduced Backend heartbeat failure logs when the warehouse is suspended. [#66733](https://github.com/StarRocks/starrocks/pull/66733)
- Supports `IF EXISTS` in the `ALTER STORAGE VOLUME` syntax. [#66691](https://github.com/StarRocks/starrocks/pull/66691)

### Bug Fix

The following issues have been fixed:

- Incorrect `DISTINCT` and `GROUP BY` results under Low Cardinality optimization due to missing `withLocalShuffle`. [#66768](https://github.com/StarRocks/starrocks/pull/66768)
- Rewrite error for JSON v2 functions with Lambda expressions. [#66550](https://github.com/StarRocks/starrocks/pull/66550)
- Incorrect application of Partition Join in Null-aware Left Anti Join within correlated subqueries. [#67038](https://github.com/StarRocks/starrocks/pull/67038)
- Incorrect row count calculation in the Meta Scan rewrite rule. [#66852](https://github.com/StarRocks/starrocks/pull/66852)
- Nullable property mismatched in Union Node when rewriting Meta Scan by statistics. [#67051](https://github.com/StarRocks/starrocks/pull/67051)
- BE crash caused by optimization logic for Ranking window functions when `PARTITION BY` and `ORDER BY` are missing. [#67094](https://github.com/StarRocks/starrocks/pull/67094)
- Potential wrong results in Group Execution Join with window functions. [#66441](https://github.com/StarRocks/starrocks/pull/66441)
- Incorrect results from `PartitionColumnMinMaxRewriteRule` under specific filter conditions. [#66356](https://github.com/StarRocks/starrocks/pull/66356)
- Incorrect Nullable property deduction in Union operations after aggregation. [#65429](https://github.com/StarRocks/starrocks/pull/65429)
- Crash in `percentile_approx_weighted` when handling compression parameters. [#64838](https://github.com/StarRocks/starrocks/pull/64838)
- Crash when spilling with large string encoding. [#61495](https://github.com/StarRocks/starrocks/pull/61495)
- Crash triggered by multiple calls to `set_collector` when pushing down local TopN. [#66199](https://github.com/StarRocks/starrocks/pull/66199)
- Dependency deduction error in LowCardinality rewrite logic. [#66795](https://github.com/StarRocks/starrocks/pull/66795)
- Rowset ID leak when rowset commit fails. [#66301](https://github.com/StarRocks/starrocks/pull/66301)
- Metacache lock contention. [#66637](https://github.com/StarRocks/starrocks/pull/66637)
- Ingestion failure when column-mode partial update is used with conditional update. [#66139](https://github.com/StarRocks/starrocks/pull/66139)
- Concurrent import failure caused by Tablet deletion during the ALTER operation. [#65396](https://github.com/StarRocks/starrocks/pull/65396)
- Tablet metadata load error due to RocksDB iteration timeout. [#65146](https://github.com/StarRocks/starrocks/pull/65146)
- Compression settings were not applied during table creation and Schema Change in shared-data clusters. [#65673](https://github.com/StarRocks/starrocks/pull/65673)
- Delete Vector CRC32 compatibility issue during upgrade. [#65442](https://github.com/StarRocks/starrocks/pull/65442)
- Status check logic error in file cleanup after clone task failure. [#65709](https://github.com/StarRocks/starrocks/pull/65709)
- Abnormal statistics collection logic after `INSERT OVERWRITE`. [#65327](https://github.com/StarRocks/starrocks/pull/65327) [#65298](https://github.com/StarRocks/starrocks/pull/65298) [#65225](https://github.com/StarRocks/starrocks/pull/65225)
- Foreign Key constraints were lost after FE restart. [#66474](https://github.com/StarRocks/starrocks/pull/66474)
- Metadata retrieval error after Warehouse deletion. [#66436](https://github.com/StarRocks/starrocks/pull/66436)
- Inaccurate Audit Log scan statistics under high selectivity filters. [#66280](https://github.com/StarRocks/starrocks/pull/66280)
- Incorrect query error rate metrics calculation logic. [#65891](https://github.com/StarRocks/starrocks/pull/65891)
- Potential MySQL connection leaks when tasks exit. [#66829](https://github.com/StarRocks/starrocks/pull/66829)
- BE status was not updated immediately on the SIGSEGV crash. [#66212](https://github.com/StarRocks/starrocks/pull/66212)
- NPE during LDAP user login. [#65843](https://github.com/StarRocks/starrocks/pull/65843)
- Inaccurate error log when switching users in HTTP SQL requests. [#65371](https://github.com/StarRocks/starrocks/pull/65371)
- HTTP context leaks during TCP connection reuse. [#65203](https://github.com/StarRocks/starrocks/pull/65203)
- Missing QueryDetail in Profile logs for queries forwarded from Follower. [#64395](https://github.com/StarRocks/starrocks/pull/64395)
- Missing Prepare/Execute details in Audit logs. [#65448](https://github.com/StarRocks/starrocks/pull/65448)
- Crash caused by HyperLogLog memory allocation failures. [#66747](https://github.com/StarRocks/starrocks/pull/66747)
- Issue with the `trim` function memory reservation. [#66477](https://github.com/StarRocks/starrocks/pull/66477) [#66428](https://github.com/StarRocks/starrocks/pull/66428)
- CVE-2025-66566 and CVE-2025-12183. [#66453](https://github.com/StarRocks/starrocks/pull/66453) [#66362](https://github.com/StarRocks/starrocks/pull/66362) [#67053](https://github.com/StarRocks/starrocks/pull/67053) 
- Race condition in Exec Group driver submission. [#66099](https://github.com/StarRocks/starrocks/pull/66099)
- Use-after-free risk in Pipeline countdown. [#65940](https://github.com/StarRocks/starrocks/pull/65940)
- `MemoryScratchSinkOperator` hangs when the queue closes. [#66041](https://github.com/StarRocks/starrocks/pull/66041)
- Filesystem cache key collision issue. [#65823](https://github.com/StarRocks/starrocks/pull/65823)
- Wrong subtask count in `SHOW PROC '/compactions'`. [#67209](https://github.com/StarRocks/starrocks/pull/67209)
- A unified JSON format is not returned in the Query Profile API. [#67077](https://github.com/StarRocks/starrocks/pull/67077)
- Improper `getTable` exception handling that affects the materialized view check. [#67224](https://github.com/StarRocks/starrocks/pull/67224)
- Inconsistent output of the `Extra` column from the `DESC` statement for native and cloud-native tables. [#67238](https://github.com/StarRocks/starrocks/pull/67238)
- Race condition in single-node deployments. [#67215](https://github.com/StarRocks/starrocks/pull/67215)
- Log leakage from third-party libraries. [#67129](https://github.com/StarRocks/starrocks/pull/67129)
- Incorrect REST Catalog authentication logic that causes authentication failures. [#66861](https://github.com/StarRocks/starrocks/pull/66861)

## 4.0.3

Release Date: December 25, 2025

### Improvements

- Supports `ORDER BY` clauses for STRUCT data types [#66035](https://github.com/StarRocks/starrocks/pull/66035)
- Supports creating Iceberg views with properties and displaying properties in the output of `SHOW CREATE VIEW`. [#65938](https://github.com/StarRocks/starrocks/pull/65938)
- Supports altering Iceberg table partition specs using `ALTER TABLE ADD/DROP PARTITION COLUMN`. [#65922](https://github.com/StarRocks/starrocks/pull/65922)
- Supports `COUNT/SUM/AVG(DISTINCT)` aggregation over framed windows (for example, `ORDER BY`/`PARTITION BY`) with optimization options. [#65815](https://github.com/StarRocks/starrocks/pull/65815)
- Optimized CSV parsing performance by using `memchr` for single-character delimiters. [#63715](https://github.com/StarRocks/starrocks/pull/63715)
- Added an optimizer rule to push down Partial TopN to the Pre-Aggregation phase to reduce network overhead. [#61497](https://github.com/StarRocks/starrocks/pull/61497)
- Enhanced Data Cache monitoring
  - Added new metrics for memory/disk quota and usage. [#66168](https://github.com/StarRocks/starrocks/pull/66168)
  - Added Page Cache statistics to the `api/datacache/stat` HTTP endpoint. [#66240](https://github.com/StarRocks/starrocks/pull/66240)
  - Added hit rate statistics for native tables. [#66198](https://github.com/StarRocks/starrocks/pull/66198)
- Optimized Sort and Aggregation operators to support rapid memory release in OOM scenarios. [#66157](https://github.com/StarRocks/starrocks/pull/66157)
- Added `TableSchemaService` in FE for shared-data clusters to allow CNs to fetch specific schemas on demand. [#66142](https://github.com/StarRocks/starrocks/pull/66142)
- Optimized Fast Schema Evolution to retain history schemas until all dependent ingestion jobs are finished. [#65799](https://github.com/StarRocks/starrocks/pull/65799)
- Enhanced `filterPartitionsByTTL` to properly handle NULL partition values to prevent all partitions from being filtered. [#65923](https://github.com/StarRocks/starrocks/pull/65923)
- Optimized `FusedMultiDistinctState` to clear the associated MemPool upon reset. [#66073](https://github.com/StarRocks/starrocks/pull/66073)
- Made `ICEBERG_CATALOG_SECURITY` property check case-insensitive in Iceberg REST Catalog. [#66028](https://github.com/StarRocks/starrocks/pull/66028)
- Added HTTP endpoint `GET /service_id` to retrieve StarOS Service ID in shared-data clusters. [#65816](https://github.com/StarRocks/starrocks/pull/65816)
- Replaced deprecated `metadata.broker.list` with `bootstrap.servers` in Kafka consumer configurations. [#65437](https://github.com/StarRocks/starrocks/pull/65437)
- Added FE configuration `lake_enable_fullvacuum` (Default: false) to allow disabling the Full Vacuum Daemon. [#66685](https://github.com/StarRocks/starrocks/pull/66685)
- Updated lz4 library to v1.10.0. [#67080](https://github.com/StarRocks/starrocks/pull/67080)

### Bug Fixes

The following issues have been fixed:

- `latest_cached_tablet_metadata` could cause versions to be incorrectly skipped during batch Publish. [#66558](https://github.com/StarRocks/starrocks/pull/66558)
- Potential issues caused by `ClusterSnapshot` relative checks in `CatalogRecycleBin` when running in shared-nothing clusters. [#66501](https://github.com/StarRocks/starrocks/pull/66501)
- BE crash when writing complex data types (ARRAY/MAP/STRUCT) to Iceberg tables during Spill operations. [#66209](https://github.com/StarRocks/starrocks/pull/66209)
- Potential hang in Connector Chunk Sink when the writer's initialization or initial write fails. [#65951](https://github.com/StarRocks/starrocks/pull/65951)
- Connector Chunk Sink bug where `PartitionChunkWriter` initialization failure caused a null pointer dereference during close. [#66097](https://github.com/StarRocks/starrocks/pull/66097)
- Setting a non-existent system variable would silently succeed instead of reporting an error. [#66022](https://github.com/StarRocks/starrocks/pull/66022)
- Bundle metadata parsing failure when Data Cache is corrupted. [#66021](https://github.com/StarRocks/starrocks/pull/66021)
- MetaScan returned NULL instead of 0 for count columns when the result is empty. [#66010](https://github.com/StarRocks/starrocks/pull/66010)
- `SHOW VERBOSE RESOURCE GROUP ALL` displays NULL instead of `default_mem_pool` for resource groups created in earlier versions. [#65982](https://github.com/StarRocks/starrocks/pull/65982)
- A `RuntimeException` during query execution after disabling the `flat_json` table configuration. [#65921](https://github.com/StarRocks/starrocks/pull/65921)
- Type mismatch issue in shared-data clusters caused by rewriting `min`/`max` statistics to MetaScan after Schema Change. [#65911](https://github.com/StarRocks/starrocks/pull/65911)
- BE crash caused by ranking window optimization when `PARTITION BY` and `ORDER BY` are missing. [#67093](https://github.com/StarRocks/starrocks/pull/67093)
- Incorrect `can_use_bf` check when merging runtime filters, which could lead to wrong results or crashes. [#67062](https://github.com/StarRocks/starrocks/pull/67062)
- Pushing down runtime bitset filters into nested OR predicates causes incorrect results. [#67061](https://github.com/StarRocks/starrocks/pull/67061)
- Potential data race and data loss issues caused by write or flush operations after the DeltaWriter has finished. [#66966](https://github.com/StarRocks/starrocks/pull/66966)
- Execution error caused by mismatched nullable properties when rewriting simple aggregation to MetaScan. [#67068](https://github.com/StarRocks/starrocks/pull/67068)
- Incorrect row count calculation in the MetaScan rewrite rule. [#66967](https://github.com/StarRocks/starrocks/pull/66967)
- Versions might be incorrectly skipped during batch Publish due to inconsistent cached tablet metadata. [#66575](https://github.com/StarRocks/starrocks/pull/66575)
- Improper error handling for memory allocation failures in HyperLogLog operations. [#66827](https://github.com/StarRocks/starrocks/pull/66827)

## 4.0.2

Release Date: December 4, 2025

### New Features

- Introduced a new resource group attribute, `mem_pool`, allowing multiple resource groups to share the same memory pool and enforce a joint memory limit for the pool. This feature is backward compatible. `default_mem_pool` is used if `mem_pool` is not specified. [#64112](https://github.com/StarRocks/starrocks/pull/64112)

### Improvements

- Reduced remote storage access during Vacuum after File Bundling is enabled. [#65793](https://github.com/StarRocks/starrocks/pull/65793)
- The File Bundling feature caches the latest tablet metadata. [#65640](https://github.com/StarRocks/starrocks/pull/65640)
- Improved safety and stability for long-string scenarios. [#65433](https://github.com/StarRocks/starrocks/pull/65433) [#65148](https://github.com/StarRocks/starrocks/pull/65148)  
- Optimized the `SplitTopNAggregateRule` logic to avoid performance regression. [#65478](https://github.com/StarRocks/starrocks/pull/65478)  
- Applied the Iceberg/DeltaLake table statistics collection strategy to other external data sources to avoid collecting statistics when the table is a single table. [#65430](https://github.com/StarRocks/starrocks/pull/65430)  
- Added Page Cache metrics to the Data Cache HTTP API `api/datacache/app_stat`. [#65341](https://github.com/StarRocks/starrocks/pull/65341)  
- Supports ORC file splitting to enable parallel scanning of a single large ORC file. [#65188](https://github.com/StarRocks/starrocks/pull/65188)  
- Added selectivity estimation for IF predicates in the optimizer. [#64962](https://github.com/StarRocks/starrocks/pull/64962)  
- Supports constant evaluation of `hour`, `minute`, and `second` for `DATE` and `DATETIME` types in the FE. [#64953](https://github.com/StarRocks/starrocks/pull/64953)  
- Enabled rewrite of simple aggregation to MetaScan by default. [#64698](https://github.com/StarRocks/starrocks/pull/64698)  
- Improved multiple-replica assignment handling in shared-data clusters for enhanced reliability. [#64245](https://github.com/StarRocks/starrocks/pull/64245)  
- Exposes cache hit ratio in audit logs and metrics. [#63964](https://github.com/StarRocks/starrocks/pull/63964)  
- Estimates per-bucket distinct counts for histograms using HyperLogLog or sampling to provide more accurate NDV for predicates and joins. [#58516](https://github.com/StarRocks/starrocks/pull/58516)  
- Supports FULL OUTER JOIN USING with SQL-standard semantics. [#65122](https://github.com/StarRocks/starrocks/pull/65122)  
- Prints memory information when Optimizer times out for diagnostics. [#65206](https://github.com/StarRocks/starrocks/pull/65206)

### Bug Fixes

The following issues have been fixed:

- DECIMAL56 `mod`-related issue. [#65795](https://github.com/StarRocks/starrocks/pull/65795)  
- Issue related to Iceberg scan range handling. [#65658](https://github.com/StarRocks/starrocks/pull/65658)  
- MetaScan rewrite issues on temporary partitions and random buckets. [#65617](https://github.com/StarRocks/starrocks/pull/65617)  
- `JsonPathRewriteRule` uses the wrong table after transparent materialized view rewrite. [#65597](https://github.com/StarRocks/starrocks/pull/65597)  
- Materialized view refresh failures when `partition_retention_condition` referenced generated columns. [#65575](https://github.com/StarRocks/starrocks/pull/65575)
- Iceberg min/max value typing issue. [#65551](https://github.com/StarRocks/starrocks/pull/65551)  
- Issue with queries against `information_schema.tables` and `views` across different databases when `enable_evaluate_schema_scan_rule` is set to `true`. [#65533](https://github.com/StarRocks/starrocks/pull/65533)  
- Integer overflow in JSON array comparison. [#64981](https://github.com/StarRocks/starrocks/pull/64981)  
- MySQL Reader does not support SSL. [#65291](https://github.com/StarRocks/starrocks/pull/65291)  
- ARM build issue caused by SVE build incompatibility. [#65268](https://github.com/StarRocks/starrocks/pull/65268)  
- Queries based on bucket-aware execution may get stuck for bucketed Iceberg tables. [#65261](https://github.com/StarRocks/starrocks/pull/65261)  
- Robust error propagation and memory safety issues for the lack of memory limit checks in OLAP table scan. [#65131](https://github.com/StarRocks/starrocks/pull/65131)

### Behavior Changes

- When a materialized view is inactivated, the system recursively inactivates its dependent materialized views. [#65317](https://github.com/StarRocks/starrocks/pull/65317)  
- Uses the original materialized view query SQL (including comments/formatting) when generating SHOW CREATE output. [#64318](https://github.com/StarRocks/starrocks/pull/64318)

## 4.0.1

Release Date: November 17, 2025

### Improvements

- Optimized TaskRun session variable handling to process known variables only. [#64150](https://github.com/StarRocks/starrocks/pull/64150)
- Supports collecting statistics of Iceberg and Delta Lake tables from metadata by default. [#64140](https://github.com/StarRocks/starrocks/pull/64140)
- Supports collecting statistics of Iceberg tables with bucket and truncate partition transform. [#64122](https://github.com/StarRocks/starrocks/pull/64122)
- Supports inspecting FE `/proc` profile for debugging. [#63954](https://github.com/StarRocks/starrocks/pull/63954)
- Enhanced OAuth2 and JWT authentication support for Iceberg REST catalogs. [#63882](https://github.com/StarRocks/starrocks/pull/63882)
- Improved bundle tablet metadata validation and recovery handling. [#63949](https://github.com/StarRocks/starrocks/pull/63949)
- Improved scan-range memory estimation logic. [#64158](https://github.com/StarRocks/starrocks/pull/64158)

### Bug Fixes

The following issues have been fixed:

- Transaction logs were deleted when publishing bundle tablets. [#64030](https://github.com/StarRocks/starrocks/pull/64030)
- The join algorithm cannot guarantee the sort property because, after joining, the sort property is not reset. [#64086](https://github.com/StarRocks/starrocks/pull/64086)
- Issues related to transparent materialized view rewrite. [#63962](https://github.com/StarRocks/starrocks/pull/63962)

### Behavior Changes

- Added the property `enable_iceberg_table_cache` to Iceberg Catalogs to optionally disable Iceberg table cache and allow it always to read the latest data. [#64082](https://github.com/StarRocks/starrocks/pull/64082)
- Ensured `INSERT ... SELECT` reads the freshest metadata by refreshing external tables before planning. [#64026](https://github.com/StarRocks/starrocks/pull/64026)
- Increased lock table slots to 256 and added `rid` to slow-lock logs. [#63945](https://github.com/StarRocks/starrocks/pull/63945)
- Temporarily disabled `shared_scan` due to incompatibility with event-based scheduling. [#63543](https://github.com/StarRocks/starrocks/pull/63543)
- Changed the default Hive Catalog cache TTL to 24 hours and removed unused parameters. [#63459](https://github.com/StarRocks/starrocks/pull/63459)
- Automatically determine the Partial Update mode based on the session variable and the number of inserted columns. [#62091](https://github.com/StarRocks/starrocks/pull/62091)

## 4.0.0

Release date: October 17, 2025

### Data Lake Analytics

- Unified Page Cache and Data Cache for BE metadata, and adopted an adaptive strategy for scaling. [#61640](https://github.com/StarRocks/starrocks/issues/61640)
- Optimized metadata file parsing for Iceberg statistics to avoid repetitive parsing. [#59955](https://github.com/StarRocks/starrocks/pull/59955)
- Optimized COUNT/MIN/MAX queries against Iceberg metadata by efficiently skipping over data file scans, significantly improving aggregation query performance on large partitioned tables and reducing resource consumption. [#60385](https://github.com/StarRocks/starrocks/pull/60385)
- Supports compaction for Iceberg tables via procedure `rewrite_data_files`. 
- Supports Iceberg tables with hidden partitions, including creating, writing, and reading the tables. [#58914](https://github.com/StarRocks/starrocks/issues/58914)
- Supports setting sort keys when creating Iceberg tables.
- Optimizes sink performance for Iceberg tables.
  - Iceberg Sink supports spilling large operators, global shuffle, and local sorting to optimize memory usage and address small file issues. [#61963](https://github.com/StarRocks/starrocks/pull/61963)
  - Iceberg Sink optimizes local sorting based on Spill Partition Writer to improve write efficiency. [#62096](https://github.com/StarRocks/starrocks/pull/62096)
  - Iceberg Sink supports global shuffle for partitions to further reduce small files. [#62123](https://github.com/StarRocks/starrocks/pull/62123)
- Enhanced bucket-aware execution for Iceberg tables to improve concurrency and distribution capabilities of bucketed tables. [#61756](https://github.com/StarRocks/starrocks/pull/61756)
- Supports the TIME data type in the Paimon catalog. [#58292](https://github.com/StarRocks/starrocks/pull/58292)
- Upgraded Iceberg version to 1.10.0. [#63667](https://github.com/StarRocks/starrocks/pull/63667)

### Security and Authentication

- In scenarios where JWT authentication and the Iceberg REST Catalog are used, StarRocks supports the passthrough of user login information to Iceberg via the REST Session Catalog for subsequent data access authentication. [#59611](https://github.com/StarRocks/starrocks/pull/59611) [#58850](https://github.com/StarRocks/starrocks/pull/58850)
- Supports vended credentials for the Iceberg catalog.
- Supports granting StarRocks internal roles to external groups obtained via Group Provider. [#63385](https://github.com/StarRocks/starrocks/pull/63385) [#63258](https://github.com/StarRocks/starrocks/pull/63258)
- Added REFRESH privilege to external tables to control the permission to refresh them. [#63385](https://github.com/StarRocks/starrocks/pull/62636)

### Storage Optimization and Cluster Management

- Introduced  the File Bundling optimization for the cloud-native table in shared-data clusters to automatically bundle the data files generated by loading, Compaction, or Publish operations, thereby reducing the API cost caused by high-frequency access to the external storage system. File Bundling is enabled by default for tables created in v4.0 or later. [#58316](https://github.com/StarRocks/starrocks/issues/58316)
- Supports Multi-Table Write-Write Transaction to allow users to control the atomic submission of INSERT, UPDATE, and DELETE operations. The transaction supports Stream Load and INSERT INTO interfaces, effectively guaranteeing cross-table consistency in ETL and real-time write scenarios. [#61362](https://github.com/StarRocks/starrocks/issues/61362)
- Supports Kafka 4.0 for Routine Load.
- Supports full-text inverted indexes on Primary Key tables in shared-nothing clusters.
- Supports modifying aggregate keys of Aggregate tables. [#62253](https://github.com/StarRocks/starrocks/issues/62253)
- Supports enabling case-insensitive processing on names of catalogs, databases, tables, views, and materialized views. [#61136](https://github.com/StarRocks/starrocks/pull/61136)
- Supports blacklisting Compute Nodes in shared-data clusters. [#60830](https://github.com/StarRocks/starrocks/pull/60830)
- Supports global connection ID. [#57256](https://github.com/StarRocks/starrocks/pull/57276)
- Added the `recyclebin_catalogs` metadata view to Information Schema to display recoverable deleted metadata. [#51007](https://github.com/StarRocks/starrocks/pull/51007)

### Query and Performance Improvement

- Supports DECIMAL256 data type, expanding the upper limit of precision from 38 to 76 bits. Its 256-bit storage provides better adaptability to high-precision financial and scientific computing scenarios, effectively mitigating DECIMAL128's precision overflow problem in very large aggregations and high-order operations. [#59645](https://github.com/StarRocks/starrocks/issues/59645)
- Improved the performance for basic operators.[#61691](https://github.com/StarRocks/starrocks/issues/61691) [#61632](https://github.com/StarRocks/starrocks/pull/61632) [#62585](https://github.com/StarRocks/starrocks/pull/62585) [#61405](https://github.com/StarRocks/starrocks/pull/61405)  [#61429](https://github.com/StarRocks/starrocks/pull/61429)
- Optimized the performance of the JOIN and AGG operators. [#61691](https://github.com/StarRocks/starrocks/issues/61691)
- [Preview] Introduced SQL Plan Manager to allow users to bind a query plan to a query, thereby preventing the query plan from changing due to system state changes (mainly data updates and statistics updates), thus stabilizing query performance. [#56310](https://github.com/StarRocks/starrocks/issues/56310)
- Introduced Partition-wise Spillable Aggregate/Distinct operators to replace the original Spill implementation based on sorted aggregation, significantly improving aggregation performance and reducing read/write overhead in complex and high-cardinality GROUP BY scenarios. [#60216](https://github.com/StarRocks/starrocks/pull/60216)
- Flat JSON V2:
  - Supports configuring Flat JSON on the table level. [#57379](https://github.com/StarRocks/starrocks/pull/57379)
  - Enhance JSON columnar storage by retaining the V1 mechanism while adding page- and segment-level indexes (ZoneMaps, Bloom filters), predicate pushdown with late materialization, dictionary encoding, and integration of a low-cardinality global dictionary to significantly boost execution efficiency. [#60953](https://github.com/StarRocks/starrocks/issues/60953)
- Supports an adaptive ZoneMap index creation strategy for the STRING data type. [#61960](https://github.com/StarRocks/starrocks/issues/61960)
- Enhanced query observability:
  - Optimized EXPLAIN ANALYZE output to display the execution metrics by group and by operator for better readability. [#63326](https://github.com/StarRocks/starrocks/pull/63326)
  - `QueryDetailActionV2` and `QueryProfileActionV2` now support JSON format, enhancing cross-FE query capabilities. [#63235](https://github.com/StarRocks/starrocks/pull/63235)
  - Supports retrieving Query Profile information across all FEs. [#61345](https://github.com/StarRocks/starrocks/pull/61345)
  - SHOW PROCESSLIST statements display Catalog, Query ID, and other information. [#62552](https://github.com/StarRocks/starrocks/pull/62552)
  - Enhanced query queue and process monitoring, supporting display of Running/Pending statuses.[#62261](https://github.com/StarRocks/starrocks/pull/62261)
- Materialized view rewrites consider the distribution and sort keys of the original table, improving the selection of optimal materialized views. [#62830](https://github.com/StarRocks/starrocks/pull/62830)

### Functions and SQL Syntax

- Added the following functions:
  - `bitmap_hash64` [#56913](https://github.com/StarRocks/starrocks/pull/56913)
  - `bool_or` [#57414 ](https://github.com/StarRocks/starrocks/pull/57414)
  - `strpos` [#57278](https://github.com/StarRocks/starrocks/pull/57287)
  - `to_datetime` and `to_datetime_ntz` [#60637](https://github.com/StarRocks/starrocks/pull/60637)
  - `regexp_count` [#57182](https://github.com/StarRocks/starrocks/pull/57182)
  - `tokenize` [#58965](https://github.com/StarRocks/starrocks/pull/58965)
  - `format_bytes` [#61535](https://github.com/StarRocks/starrocks/pull/61535)
  - `encode_sort_key` [#61781](https://github.com/StarRocks/starrocks/pull/61781)
  - `column_size` and `column_compressed_size`  [#62481](https://github.com/StarRocks/starrocks/pull/62481)
- Provides the following syntactic extensions:
  - Supports IF NOT EXISTS keywords in CREATE ANALYZE FULL TABLE. [#59789](https://github.com/StarRocks/starrocks/pull/59789)
  - Supports EXCLUDE clauses in SELECT.  [#57411](https://github.com/StarRocks/starrocks/pull/57411/files)
  - Supports FILTER clauses in aggregate functions, improving readability and execution efficiency of conditional aggregations. [#58937](https://github.com/StarRocks/starrocks/pull/58937)

### Behavior Changes

- Adjust the logic of the materialized view parameter `auto_partition_refresh_number` to limit the number of partitions to refresh regardless of auto refresh or manual refresh. [#62301](https://github.com/StarRocks/starrocks/pull/62301)
- Flat JSON is enabled by default. [#62097](https://github.com/StarRocks/starrocks/pull/62097)
- The default value of the system variable `enable_materialized_view_agg_pushdown_rewrite` is set to `true`, indicating that aggregation pushdown for materialized view query rewrite is enabled by default. [#60976](https://github.com/StarRocks/starrocks/pull/60976)
- Changed the type of some columns in `information_schema.materialized_views` to better align with the corresponding data. [#60054](https://github.com/StarRocks/starrocks/pull/60054)
- The `split_part` function returns NULL when the delimiter is not matched. [#56967](https://github.com/StarRocks/starrocks/pull/56967)
- Use STRING to replace fixed-length CHAR in CTAS/CREATE MATERIALIZED VIEW to avoid deducing the wrong column length, which may cause materialized view refresh failures. [#63114](https://github.com/StarRocks/starrocks/pull/63114) [#62476](https://github.com/StarRocks/starrocks/pull/62476)
- Data Cache-related configurations are simplified. [#61640](https://github.com/StarRocks/starrocks/issues/61640)
  - `datacache_mem_size` and `datacache_disk_size` are now effective.
  - `storage_page_cache_limit`, `block_cache_mem_size`, `block_cache_disk_size` are deprecated.
- Added new catalog properties (`remote_file_cache_memory_ratio` for Hive, and `iceberg_data_file_cache_memory_usage_ratio` and `iceberg_delete_file_cache_memory_usage_ratio` for Iceberg) to limit the memory resources used for Hive and Iceberg metadata cache, and set the default values to `0.1` (10%). Adjust the metadata cache TTL to 24 hours. [#63459](https://github.com/StarRocks/starrocks/pull/63459) [#63373](https://github.com/StarRocks/starrocks/pull/63373) [#61966](https://github.com/StarRocks/starrocks/pull/61966) [#62288](https://github.com/StarRocks/starrocks/pull/62288)
- SHOW DATA DISTRIBUTION now will not merge the statistics of all materialized indexes with the same bucket sequence number. It only shows data distribution at the materialized index level. [#59656](https://github.com/StarRocks/starrocks/pull/59656)
- The default bucket size for automatic bucket tables is changed from 4GB to 1GB to improve performance and resource utilization. [#63168](https://github.com/StarRocks/starrocks/pull/63168)
-  The system determines the Partial Update mode based on the corresponding session variable and the number of columns in the INSERT statement. [#62091](https://github.com/StarRocks/starrocks/pull/62091)
- Optimized the `fe_tablet_schedules` view in the Information Schema. [#62073](https://github.com/StarRocks/starrocks/pull/62073) [#59813](https://github.com/StarRocks/starrocks/pull/59813)
  - Renamed the `TABLET_STATUS` column to `SCHEDULE_REASON`, the `CLONE_SRC` column to `SRC_BE_ID`, and the `CLONE_DEST` column to `DEST_BE_ID`.
  - The data types of the `CREATE_TIME`, `SCHEDULE_TIME` and `FINISH_TIME` columns have been changed from `DOUBLE` to `DATETIME`.
- The `is_leader` label has been added to some FE metrics. [#63004](https://github.com/StarRocks/starrocks/pull/63004)
- Shared-data clusters using Microsoft Azure Blob Storage and Data Lake Storage Gen 2 as object storage will experience Data Cache failure after being upgraded to v4.0. The system will automatically reload the cache.
