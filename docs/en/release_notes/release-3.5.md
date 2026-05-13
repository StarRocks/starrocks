---
displayed_sidebar: docs
---

# StarRocks version 3.5

:::warning

**Upgrade Notes**

- JDK 17 or later is required from StarRocks v3.5.0 onwards.
  - To upgrade a cluster from v3.4 or earlier, you must upgrade the version of JDK that StarRocks depends, and remove the options that are incompatible with JDK 17 in the configuration item `JAVA_OPTS` in the FE configuration file **fe.conf**, for example, options that involve CMS and GC. The default value of `JAVA_OPTS` in the v3.5 configuration file is recommended.
  - For clusters using external catalogs, you need to add `--add-opens=java.base/java.util=ALL-UNNAMED` to the `JAVA_OPTS` configuration item in the BE configuration file **be.conf**.
  - For clusters using Java UDFs, you need to add `--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED` to the `JAVA_OPTS` configuration item in the BE configuration file **be.conf**.
  - In addition, as of v3.5.0, StarRocks no longer provides JVM configurations for specific JDK versions. All versions of JDK use `JAVA_OPTS`.
- It is recommended to upgrade the cluster to v3.4.10 or later before upgrading it to v3.5. Otherwise, you must manually disable low cardinality optimization during the gray-scale upgrade by executing the following statement:

  ```SQL
  SET GLOBAL cbo_enable_low_cardinality_optimize=false;
  ```

**Downgrade Notes**

- After upgrading StarRocks to v3.5, DO NOT downgrade it directly to v3.4.0 ～ v3.4.5, otherwise it will cause metadata incompatibility. You must downgrade the cluster to v3.4.6 or later to prevent the issue.
- After upgrading StarRocks to v3.5.2 or later, DO NOT downgrade it to v3.5.0 & v3.5.1, otherwise it will cause FE crash.

:::

## 3.5.17

Release date: May 13, 2026

### Behavior Changes

- `SHOW CREATE TABLE` and `DESC` now show Primary Keys for Paimon tables. [#70535](https://github.com/StarRocks/starrocks/pull/70535)
- Disallowed INSERT into insert-only ACID Hive tables in Hive catalogs. [#71460](https://github.com/StarRocks/starrocks/pull/71460)
- `START_TIME` and `END_TIME` in Profile are now displayed using the session time zone. [#71429](https://github.com/StarRocks/starrocks/pull/71429)

### Improvements

- Supports `csv.enclose` and `csv.escape` in `INSERT INTO FILES` CSV export. [#71589](https://github.com/StarRocks/starrocks/pull/71589)
- Added query relation information (directly queried tables and viewa) to audit logs. [#71596](https://github.com/StarRocks/starrocks/pull/71596)
- Made the FE configuration `star_mgr_meta_sync_interval_sec` runtime mutable. [#71675](https://github.com/StarRocks/starrocks/pull/71675)
- Reduced metadata and lock overhead in table metadata and row-count paths. [#72053](https://github.com/StarRocks/starrocks/pull/72053) [#72042](https://github.com/StarRocks/starrocks/pull/72042) [#71672](https://github.com/StarRocks/starrocks/pull/71672)
- Improved build and dependency hygiene by merging the broker builder into the FE build and removing WildFly OpenSSL. [#71823](https://github.com/StarRocks/starrocks/pull/71823) [#71908](https://github.com/StarRocks/starrocks/pull/71908)

### Bug fixes

The following issues have been fixed:

- Wrong results for local-shuffle aggregate queries with OFFSET. [#71997](https://github.com/StarRocks/starrocks/pull/71997)
- Incorrect Join output properties after Exchange shuffle columns are pruned. [#72003](https://github.com/StarRocks/starrocks/pull/72003)
- Several dependency CVE issues. [#71762](https://github.com/StarRocks/starrocks/pull/71762) [#71914](https://github.com/StarRocks/starrocks/pull/71914)
- Oracle JDBC NLS format handling issue. [#71412](https://github.com/StarRocks/starrocks/pull/71412)
- Missing Iceberg column statistics in manifest data file cache. [#71913](https://github.com/StarRocks/starrocks/pull/71913)
- Missing Hive partition directory before INSERT OVERWRITE commit. [#71810](https://github.com/StarRocks/starrocks/pull/71810)
- Aggregate-join-pushdown materialized view rewrite and min/max optimization issues on Iceberg base tables. [#71856](https://github.com/StarRocks/starrocks/pull/71856) [#71863](https://github.com/StarRocks/starrocks/pull/71863)
- Race conditions in `ConnectorSinkPassthroughExchanger` and `LoadChannel::get_load_replica_status`. [#71848](https://github.com/StarRocks/starrocks/pull/71848) [#71843](https://github.com/StarRocks/starrocks/pull/71843)
- Credential redaction issue in INSERT FILES operations. [#71245](https://github.com/StarRocks/starrocks/pull/71245)
- Incorrect `reverse(DecimalV3)` results. [#71834](https://github.com/StarRocks/starrocks/pull/71834)
- Missing JNI exception handling checks in Java UDF code. [#71734](https://github.com/StarRocks/starrocks/pull/71734)
- Incorrect short-circuit checks in `EventScheduler`. [#71740](https://github.com/StarRocks/starrocks/pull/71740)
- Incorrect Arrow Flight column name for empty result sets. [#71534](https://github.com/StarRocks/starrocks/pull/71534)
- Batch publish deadlock caused by partition version gaps. [#71483](https://github.com/StarRocks/starrocks/pull/71483)
- Repeated Apply attachments in scalar-subquery plans. [#71155](https://github.com/StarRocks/starrocks/pull/71155)

## 3.5.16

Release date: April 20, 2026

### Improvements

- Added clearer warning logs for swallowed exceptions in `WarehouseManager`. [#71215](https://github.com/StarRocks/starrocks/pull/71215)
- Supports pausing Routine Load jobs on non-retryable errors. [#71161](https://github.com/StarRocks/starrocks/pull/71161)
- Added thread names to the utility that prints all thread stacks. [#69366](https://github.com/StarRocks/starrocks/pull/69366)
- Supports constant folding for `regexp_replace` in FE. [#70804](https://github.com/StarRocks/starrocks/pull/70804)
- Supports showing column comments for PostgreSQL external tables, and added an option to let `information_schema.tables` fetch full metadata such as comments from external catalogs. [#70520](https://github.com/StarRocks/starrocks/pull/70520) [#70197](https://github.com/StarRocks/starrocks/pull/70197)
- Added automatic query plan dumping on query exceptions. [#70387](https://github.com/StarRocks/starrocks/pull/70387)
- Improved cloud-native tablet metadata fetch and repair efficiency. [#70492](https://github.com/StarRocks/starrocks/pull/70492) [#70386](https://github.com/StarRocks/starrocks/pull/70386)
- Added batch tablet deletion in FE to reduce write lock contention. [#70052](https://github.com/StarRocks/starrocks/pull/70052)
- Added logs for update compaction suspension, and added Iceberg metadata-table and time-travel query metrics. [#70538](https://github.com/StarRocks/starrocks/pull/70538) [#70825](https://github.com/StarRocks/starrocks/pull/70825) [#70788](https://github.com/StarRocks/starrocks/pull/70788)

### Bug fixes

The following issues have been fixed:

- `be_tablets.DATA_SIZE` reports rowset column data bytes inaccurately. [#70735](https://github.com/StarRocks/starrocks/pull/70735)
- An outdated Maven repository for broker builds. [#71533](https://github.com/StarRocks/starrocks/pull/71533)
- Incorrect thread-pool resizing when updating `parallel_clone_task_per_path`. [#71484](https://github.com/StarRocks/starrocks/pull/71484)
- Several use-after-free issues. [#71513](https://github.com/StarRocks/starrocks/pull/71513) [#71276](https://github.com/StarRocks/starrocks/pull/71276) [#71083](https://github.com/StarRocks/starrocks/pull/71083) [#62917](https://github.com/StarRocks/starrocks/pull/62917) [#69926](https://github.com/StarRocks/starrocks/pull/69926) [#69968](https://github.com/StarRocks/starrocks/pull/69968)
- Resource group user classifier validation is not aligned with `CREATE USER`. [#71470](https://github.com/StarRocks/starrocks/pull/71470)
- “no queryable replica” issues on follower FEs by syncing StarMgr journal replay. [#71263](https://github.com/StarRocks/starrocks/pull/71263)
- Multiple dependency CVEs. [#71256](https://github.com/StarRocks/starrocks/pull/71256) [#71017](https://github.com/StarRocks/starrocks/pull/71017) [#70862](https://github.com/StarRocks/starrocks/pull/70862)
- `VARCHAR` length is not preserved after reduce-cast with global variables. [#70269](https://github.com/StarRocks/starrocks/pull/70269)
- Special-character escaping in equality predicates on `information_schema.tables`. [#71273](https://github.com/StarRocks/starrocks/pull/71273)
- `UpdateTabletSchemaTask` signature collisions across alter jobs. [#71242](https://github.com/StarRocks/starrocks/pull/71242)
- Issue with shared-object mutation in `PushDownAggregateRewriter` for `CASE WHEN` and `IF` expressions. [#71309](https://github.com/StarRocks/starrocks/pull/71309)
- Stopped inactive materialized view schedulers correctly and added missing leader checks in TaskManager scheduler callbacks. [#71265](https://github.com/StarRocks/starrocks/pull/71265) [#71156](https://github.com/StarRocks/starrocks/pull/71156)
- `NaN` row-count estimation for MCV-only histograms. [#71241](https://github.com/StarRocks/starrocks/pull/71241)
- Packaging issues caused by a missing `s3-transfer-manager` dependency in the AWS SDK. [#71230](https://github.com/StarRocks/starrocks/pull/71230)
- Thread-local `ConnectContext` pollution after leader forwarding. [#71141](https://github.com/StarRocks/starrocks/pull/71141)
- Orphaned delvec entries when write and compaction transactions are published in the same batch. [#71001](https://github.com/StarRocks/starrocks/pull/71001) [#71049](https://github.com/StarRocks/starrocks/pull/71049) [#71107](https://github.com/StarRocks/starrocks/pull/71107)
- Missing partition predicates in short-circuit point lookups. [#71124](https://github.com/StarRocks/starrocks/pull/71124)
- Potential hash-table data loss during aggregation spill `set_finishing`. [#70851](https://github.com/StarRocks/starrocks/pull/70851)
- Query-progress HTTP loopback records from `current_queries`. [#71032](https://github.com/StarRocks/starrocks/pull/71032)
- Primary Key tablet rowset metadata loss caused by a GC race during disk re-migration (A→B→A). [#70727](https://github.com/StarRocks/starrocks/pull/70727)
- DB read-lock leaks in `SharedDataStorageVolumeMgr`. [#70987](https://github.com/StarRocks/starrocks/pull/70987)
- Incorrect `NullColumn` sharing in `NullableColumn`, `BinaryColumn`, and `locate()`. [#66037](https://github.com/StarRocks/starrocks/pull/66037) [#70957](https://github.com/StarRocks/starrocks/pull/70957)
- Race conditions in global runtime-filter readiness checks and corrected metric overflow caused by `ACCUMULATED` macro truncation. [#70920](https://github.com/StarRocks/starrocks/pull/70920) [#70889](https://github.com/StarRocks/starrocks/pull/70889)
- Generated-column is not displayed in `DESC` and `SHOW CREATE TABLE`. [#70037](https://github.com/StarRocks/starrocks/pull/70037)
- An issue with load spill directory cleanup timing, an ASAN crash in memory table spiller workgroup handling, and CN crashes when scanning empty tablets with physical split enabled. [#70778](https://github.com/StarRocks/starrocks/pull/70778) [#64379](https://github.com/StarRocks/starrocks/pull/64379) [#70281](https://github.com/StarRocks/starrocks/pull/70281)
- Incorrect `Content-Length` handling when `proxy_pass_request_body` is off. [#70821](https://github.com/StarRocks/starrocks/pull/70821)
- Issues with connector scan retry state handling and multiple schema pushdown issues for `INSERT INTO BY NAME ... FROM FILES()`. [#70762](https://github.com/StarRocks/starrocks/pull/70762) [#70774](https://github.com/StarRocks/starrocks/pull/70774) [#70622](https://github.com/StarRocks/starrocks/pull/70622) [#70621](https://github.com/StarRocks/starrocks/pull/70621)
- Invalid conjunct pushdown in MySQL and JDBC scan nodes that caused BE predicate type errors. [#70694](https://github.com/StarRocks/starrocks/pull/70694)
- Incomplete and partially written Iceberg manifest cache entries, and bypassed catalog caching when vended credentials are enabled. [#70675](https://github.com/StarRocks/starrocks/pull/70675) [#70652](https://github.com/StarRocks/starrocks/pull/70652) [#69434](https://github.com/StarRocks/starrocks/pull/69434)
- Ubuntu runtime dependency issues by installing `libssl-dev`. [#70688](https://github.com/StarRocks/starrocks/pull/70688)
- User authentication strings are not masked in audit logs and SQL redaction. [#70360](https://github.com/StarRocks/starrocks/pull/70360)
- External materialized view refresh issues for Iceberg-like connectors. [#70589](https://github.com/StarRocks/starrocks/pull/70589) [#70523](https://github.com/StarRocks/starrocks/pull/70523)
- `array_map` crashes on null literal arrays and BE crashes when a child iterator is exhausted in `MaskMergeIterator`. [#70629](https://github.com/StarRocks/starrocks/pull/70629) [#70539](https://github.com/StarRocks/starrocks/pull/70539)
- `starlet` configuration updates were incorrectly captured through `std::call_once`. [#70482](https://github.com/StarRocks/starrocks/pull/70482)
- Robustness issue with Iceberg materialized view refresh when snapshot timestamps are non-monotonic. [#70382](https://github.com/StarRocks/starrocks/pull/70382)
- Issues that forced materialized view refresh is not supported, and duplicated partition names during materialized view refresh. [#70381](https://github.com/StarRocks/starrocks/pull/70381) [#70354](https://github.com/StarRocks/starrocks/pull/70354)
- Incorrect distribution-column handling across partitions in `OlapTableSink`. [#70310](https://github.com/StarRocks/starrocks/pull/70310)
- File-existence checks are not cached across tablet metadata versions during missing-file validation. [#70364](https://github.com/StarRocks/starrocks/pull/70364)
- Issue with `dataVersion` handling during RESTORE, and incorrect materialized view rewrite logic. [#70373](https://github.com/StarRocks/starrocks/pull/70373) [#69751](https://github.com/StarRocks/starrocks/pull/69751)
- Negative `query_pool` memory accounting during ingestion, and high FE OOM risk under high concurrency. [#70228](https://github.com/StarRocks/starrocks/pull/70228) [#68444](https://github.com/StarRocks/starrocks/pull/68444)
- Incorrect SLF4J parameterized logging. [#70330](https://github.com/StarRocks/starrocks/pull/70330)
- `AuditEventProcessor` exiting on `OutOfMemoryException`. [#70206](https://github.com/StarRocks/starrocks/pull/70206)
- Adjusted handling of column-mode partial updates for Primary Key tables; the initial corruption fix was reverted in this release cycle for follow-up work. [#69652](https://github.com/StarRocks/starrocks/pull/69652)
- Equality and deduplication issues in `InformationFunction`. [#70464](https://github.com/StarRocks/starrocks/pull/70464)
- `brpc` connection retries when exceptions are wrapped in `NoSuchElementException`. [#70203](https://github.com/StarRocks/starrocks/pull/70203)
- Lock-free materialized view rewrite does not fallback to live metadata. [#70475](https://github.com/StarRocks/starrocks/pull/70475)
- Issue with `JoinHashTable::merge_ht()` that it does not skip dummy rows for expression-based join-key columns. [#70465](https://github.com/StarRocks/starrocks/pull/70465)

## 3.5.15

Release Date: March 26, 2026

### Behavior Changes

- Improved `sql_mode` handling: when `DIVISION_BY_ZERO` or `FAIL_PARSE_DATE` mode is set, division by zero and date parse failures in `str_to_date`/`str2date` now return an error instead of being silently ignored. [#70004](https://github.com/StarRocks/starrocks/pull/70004)
- When `sql_mode` is set to `FORBID_INVALID_DATE`, invalid dates in `INSERT VALUES` clauses are now correctly rejected instead of being bypassed. [#69803](https://github.com/StarRocks/starrocks/pull/69803)
- Expression partition generated columns are now hidden from `DESC` and `SHOW CREATE TABLE` output. [#69793](https://github.com/StarRocks/starrocks/pull/69793)
- Client ID is no longer included in audit logs. [#69383](https://github.com/StarRocks/starrocks/pull/69383)
- The `FORCE` option for `REFRESH EXTERNAL TABLE` has been reverted and is no longer supported. [#70428](https://github.com/StarRocks/starrocks/pull/70428)

### Improvements

- Allowed disabling split and reverse scan ranges for descending TopN by setting `desc_hint_split_range` to `0` or less. [#70307](https://github.com/StarRocks/starrocks/pull/70307)
- `information_schema` now shows comments for external catalog tables. [#70197](https://github.com/StarRocks/starrocks/pull/70197)
- Added `EXPLAIN` and `EXPLAIN ANALYZE` support for `INSERT` statements in Trino dialect. [#70174](https://github.com/StarRocks/starrocks/pull/70174)
- Added configurable parameters for `CatalogRecycleBin` to control recycle bin behavior. [#69838](https://github.com/StarRocks/starrocks/pull/69838)
- Improved `ADMIN REPAIR TABLE` and `SHOW TABLET STATUS` to provide better repair and status information. [#69656](https://github.com/StarRocks/starrocks/pull/69656)
- Blacklisted queries are now excluded from error metrics. [#69621](https://github.com/StarRocks/starrocks/pull/69621)
- Added support for `SHOW TABLET STATUS` for cloud-native tablets in shared-data deployments. [#69616](https://github.com/StarRocks/starrocks/pull/69616)
- Reduced overhead of Primary Key tablet statistics collection in shared-data clusters. [#69548](https://github.com/StarRocks/starrocks/pull/69548)
- Added support for dynamic configuration of the execution state report thread pool size. [#69142](https://github.com/StarRocks/starrocks/pull/69142)

### Bug Fixes

Fixed the following bugs:

- Data version not set when restoring a tablet. [#70373](https://github.com/StarRocks/starrocks/pull/70373)
- Table comment not set when creating a Hive table. [#70318](https://github.com/StarRocks/starrocks/pull/70318)
- Constant folding with double precision arithmetic producing `INF` instead of returning an error. [#70309](https://github.com/StarRocks/starrocks/pull/70309)
- Iceberg materialized view refresh failing when snapshot timestamps are non-monotonic. [#70382](https://github.com/StarRocks/starrocks/pull/70382)
- `toIcebergTable` function using `common` instead of `comment` in property mapping. [#70267](https://github.com/StarRocks/starrocks/pull/70267)
- Root user not correctly bypassing Ranger permission checks in all scenarios. [#70254](https://github.com/StarRocks/starrocks/pull/70254)
- `AuditEventProcessor` thread exiting unexpectedly when an `OutOfMemoryException` occurs. [#70206](https://github.com/StarRocks/starrocks/pull/70206)
- Out-of-bounds access in `cal_new_base_version` during schema change publish. [#70132](https://github.com/StarRocks/starrocks/pull/70132)
- Partition predicates pruned unexpectedly due to type mismatch in boundary comparison. [#70097](https://github.com/StarRocks/starrocks/pull/70097)
- `str_to_date` losing microsecond precision in BE runtime. [#70068](https://github.com/StarRocks/starrocks/pull/70068)
- Crash in join spill process when `set_callback_function` is called. [#70030](https://github.com/StarRocks/starrocks/pull/70030)
- DCHECK failure in `DeltaWriter::close()` when called from a bthread context. [#69960](https://github.com/StarRocks/starrocks/pull/69960)
- Use-after-free race condition in `AsyncDeltaWriter` close/finish lifecycle. [#69940](https://github.com/StarRocks/starrocks/pull/69940)
- Journal replay not awaited in `changeCatalogDb` on follower FE, causing consistency issues. [#69834](https://github.com/StarRocks/starrocks/pull/69834)
- Race condition causing missed write transaction finished editlog. [#69899](https://github.com/StarRocks/starrocks/pull/69899)
- Several known CVEs addressed. [#69863](https://github.com/StarRocks/starrocks/pull/69863)
- Incorrect LIKE pattern matching with backslash escape sequences. [#69775](https://github.com/StarRocks/starrocks/pull/69775)
- Expression analysis failing after renaming a partition column. [#69771](https://github.com/StarRocks/starrocks/pull/69771)
- Use-after-free crash in `AsyncDeltaWriter::close`. [#69770](https://github.com/StarRocks/starrocks/pull/69770)
- Potential bugs in `PartitionColumnMinMaxRewriteRule` caused by incorrect `Partition.hasStorageData` results. [#69751](https://github.com/StarRocks/starrocks/pull/69751)
- Duplicated CSV compression suffix in file sink output file names. [#69749](https://github.com/StarRocks/starrocks/pull/69749)
- Lake `capture_tablet_and_rowsets` operation accessible without experimental config flag. [#69748](https://github.com/StarRocks/starrocks/pull/69748)
- Corrupted cache for Primary Key SST tables. [#69693](https://github.com/StarRocks/starrocks/pull/69693)
- Use-after-free in `AsyncFlushOutputStream`. [#69688](https://github.com/StarRocks/starrocks/pull/69688)
- Incorrect retention clock reset and incomplete scan in `disableRecoverPartitionWithSameName`. [#69677](https://github.com/StarRocks/starrocks/pull/69677)
- Tablet info not fetched correctly based on run mode in `SchemaBeTabletsScanner`. [#69645](https://github.com/StarRocks/starrocks/pull/69645)
- Incorrect minimum partition pruning with shadow partitions. [#69641](https://github.com/StarRocks/starrocks/pull/69641)
- Different transactions publishing the same version after graceful exit. [#69639](https://github.com/StarRocks/starrocks/pull/69639)
- Iterator undefined behavior in `get_column_values` when `rssid` is not found. [#69617](https://github.com/StarRocks/starrocks/pull/69617)
- `KILL ANALYZE` statement sometimes not stopping a running `ANALYZE TABLE` operation. [#69592](https://github.com/StarRocks/starrocks/pull/69592)
- Materialized view force refresh bugs for partition tables. [#69488](https://github.com/StarRocks/starrocks/pull/69488)

## 3.5.14

Release Date: March 5, 2026

### Improvements

- Added SST read/write failure metrics for Primary Key index in Lake tables. [#69513](https://github.com/StarRocks/starrocks/pull/69513)
- Added a counter metric for "segment file not found" errors. [#69543](https://github.com/StarRocks/starrocks/pull/69543)
- Extracted range predicates from scalar-subquery containing `convert_tz`. [#69055](https://github.com/StarRocks/starrocks/pull/69055)
- Supports complex type for Paimon tables. [#66784](https://github.com/StarRocks/starrocks/pull/66784)
- Deferred remote load Spill Directory removal. [#68803](https://github.com/StarRocks/starrocks/pull/68803)
- Supports repairing cloud-native tables. [#67108](https://github.com/StarRocks/starrocks/pull/67108)
- Supports inserting ARRAY type to Hive table in CSV format. [#67355](https://github.com/StarRocks/starrocks/pull/67355)

### Bug Fixes

The following issues have been fixed:

- Unexpected behavior caused by exceptions of `RowGroupWriter`. [#69568](https://github.com/StarRocks/starrocks/pull/69568)
- Sort key not including newly added key columns after schema change on Aggregate Key/Unique  Key tables. [#69529](https://github.com/StarRocks/starrocks/pull/69529)
- Mertic value `g_publish_version_failed_tasks` does not reflect the real situation during the `resource_busy` state. [#69526](https://github.com/StarRocks/starrocks/pull/69526)
- Rowset files are removed when moving Primary Key tablets to trash. [#69438](https://github.com/StarRocks/starrocks/pull/69438)
- Lock leak in `addPartitions` caused by name-based table lookup after concurrent SWAP. [#69284](https://github.com/StarRocks/starrocks/pull/69284)
- `isInternalCancelError` used `equals` instead of `startsWith`. [#69523](https://github.com/StarRocks/starrocks/pull/69523)
- Pipeline blocks or crashes when `_writer->Close()` throws an exception other than `ParquetStatusException`. [#69492](https://github.com/StarRocks/starrocks/pull/69492)
- A Hadoop-client lib bug. [#69503](https://github.com/StarRocks/starrocks/pull/69503)
- Success is mistakenly returned while write operations fails. [#69473](https://github.com/StarRocks/starrocks/pull/69473)
- CVE-2025-67721. [#69138](https://github.com/StarRocks/starrocks/pull/69138)
- Issue with RuntimeFilter with low-cardinality optimization in share-data clusters. [#64669](https://github.com/StarRocks/starrocks/pull/64669)
- Materialized view tablet meta inconsistency between FE leader and follower. [#69428](https://github.com/StarRocks/starrocks/pull/69428)
- Rollup handler's active transaction ID was not considered in `computeMinActiveTxnId`. [#69285](https://github.com/StarRocks/starrocks/pull/69285)
- Arrow Flight Proxy issue with multiple FE. [#68300](https://github.com/StarRocks/starrocks/pull/68300)
- Concurrency bug of function field. [#69315](https://github.com/StarRocks/starrocks/pull/69315)
- `DROP FUNCTION IF EXISTS` ignored `ifExists` flag. [#69216](https://github.com/StarRocks/starrocks/pull/69216)
- Lacking case-insensitive username normalization for LDAP authentication. [#67966](https://github.com/StarRocks/starrocks/pull/67966)
- Certain kinds of partitions cannot be written. [#68221](https://github.com/StarRocks/starrocks/pull/68221)
- Projection loss in materialized view rewrite due to shared mutable state. [#69063](https://github.com/StarRocks/starrocks/pull/69063)
- Issue with case-insensitive partition lookup in query table copy. [#69173](https://github.com/StarRocks/starrocks/pull/69173)
- All-null value handling bug in synchronous materialized views. [#69136](https://github.com/StarRocks/starrocks/pull/69136)
- `mv onReload` issues when visiting external catalogs. [#68926](https://github.com/StarRocks/starrocks/pull/68926)
- DISTINCT ORDER BY alias issues for duplicated constants. [#69014](https://github.com/StarRocks/starrocks/pull/69014)
- Incorrect query results after modifying CHAR column length in shared-data clusters. [#68808](https://github.com/StarRocks/starrocks/pull/68808)
- Issue with Azure ABFS/WASB FileSystem cache key. [#68901](https://github.com/StarRocks/starrocks/pull/68901)
- Incorrect predicate rewrite for OUTER JOIN with constant-side column reference. [#67072](https://github.com/StarRocks/starrocks/pull/67072)
- `IllegalArgumentException` comparator transitivity violation. [#68743](https://github.com/StarRocks/starrocks/pull/68743)
- Issue caused by the query lifetime being shorter than the fragment in `report_fragment`. [#67219](https://github.com/StarRocks/starrocks/pull/67219)
- Low-cardinality rewrite NPE caused by shared `DecodeInfo`. [#68799](https://github.com/StarRocks/starrocks/pull/68799)
- Missing `pcu_upt_cnt` metric. [#68845](https://github.com/StarRocks/starrocks/pull/68845)
- JSON-flatten array/object conflict on identical paths. [#68804](https://github.com/StarRocks/starrocks/pull/68804)
- `ClonExpr` nullable bug. [#68800](https://github.com/StarRocks/starrocks/pull/68800)

## 3.5.13

Release Date: February 13, 2026

### Improvements

- Added an FE configuration `enable_table_metrics_collect` to control the collection of table-level metrics. [#68691](https://github.com/StarRocks/starrocks/pull/68691)
- Supports setting the default Warehouse for Merge Commit at user level. [#68616](https://github.com/StarRocks/starrocks/pull/68616)

### Bug fixes

The following issues have been fixed:

- Issue with source partition checking in replication transactions. [#68883](https://github.com/StarRocks/starrocks/pull/68883)
- Used labels were not identified when labels were specified in BEGIN TRANSACTION. [#68660](https://github.com/StarRocks/starrocks/pull/68660)
- JOIN ON clause bug with CTE scope. [#68809](https://github.com/StarRocks/starrocks/pull/68809)
- Overlapping range partitions can be created when an explicit lower bound is provided. [#68255](https://github.com/StarRocks/starrocks/pull/68255)
- Incorrect parser logic when SQL dialect downgrades from Trino to StarRocks. [#68725](https://github.com/StarRocks/starrocks/pull/68725)
- Issue with pruning projection columns. [#68242](https://github.com/StarRocks/starrocks/pull/68242)
- Issue with subquery scope check. [#68415](https://github.com/StarRocks/starrocks/pull/68415)
- Unmatched type cast in the function analyzer. [#66749](https://github.com/StarRocks/starrocks/pull/66749)
- Incorrect candidate materialized view selection logic. [#68571](https://github.com/StarRocks/starrocks/pull/68571)
- The Thrift `accept` thread exits on exception. [#68644](https://github.com/StarRocks/starrocks/pull/68644)
- Inaccurate Iceberg data file size estimation. [#68787](https://github.com/StarRocks/starrocks/pull/68787)
- Lake table memory leak issue. [#68678](https://github.com/StarRocks/starrocks/pull/68678)
- Deadlock when the HMS connection pool is full. [#68033](https://github.com/StarRocks/starrocks/pull/68033)
- Iceberg delete column nullability issue. [#68649](https://github.com/StarRocks/starrocks/pull/68649)
- Materialized views hold large external tables. [#68171](https://github.com/StarRocks/starrocks/pull/68171)
- Iceberg table cache memory limit issue. [#67769](https://github.com/StarRocks/starrocks/pull/67769)
- Wrong timeout parameter is used for PocoHttpClient. [#68765](https://github.com/StarRocks/starrocks/pull/68765)
- BE compile failure with Clang. [#68805](https://github.com/StarRocks/starrocks/pull/68805)
- Materialized view was reloaded multiple times during startup. [#62351](https://github.com/StarRocks/starrocks/pull/62351)
- CVE-2025-27821. [#68529](https://github.com/StarRocks/starrocks/pull/68529)
- Variadic functions return incorrect date values in certain scenarios. [#67947](https://github.com/StarRocks/starrocks/pull/67947)

## 3.5.12

Release Date: January 22, 2026

### Improvements

- Added a cleaner for BrpcStubCache to clean up unused connections. [#61417](https://github.com/StarRocks/starrocks/pull/61417)
- Supports batch processing for statistics delete (for dropped tables) and Edit Log write requests. [#67896](https://github.com/StarRocks/starrocks/pull/67896)
- Preserves SQL comments in Audit Logs when encryption is required. [#63298](https://github.com/StarRocks/starrocks/pull/63298)
- Added the `warehouse_name` label to the materialized view metrics. [#67715](https://github.com/StarRocks/starrocks/pull/67715)
- Improved identifier wrapping for JDBC table and column names. [#67853](https://github.com/StarRocks/starrocks/pull/67853)
- Added the `CLIENT_FACTORY`  property to the Iceberg JDBC catalog. [#67613](https://github.com/StarRocks/starrocks/pull/67613)

### Bug fixes

The following issues have been fixed:

- Variadic functions return wrong dates when mixing DATE and DATETIME types. [#67947](https://github.com/StarRocks/starrocks/pull/67947)
- `NormalizePredicateRule` oscillation on non-deterministic expressions. [#67923](https://github.com/StarRocks/starrocks/pull/67923)
- Low cardinality bugs with the Lambda function. [#67843](https://github.com/StarRocks/starrocks/pull/67843)
- Subfield expression does not collect children subfields. [#67850](https://github.com/StarRocks/starrocks/pull/67850)
- NPE in RBO Join reorder when child statistics are missing. [#67693](https://github.com/StarRocks/starrocks/pull/67693)
- BE crash due to MemTable finalize failed. [#67787](https://github.com/StarRocks/starrocks/pull/67787)
- Temporary partitions are not cleaned up after FE restart for dynamic overwrite. [#67629](https://github.com/StarRocks/starrocks/pull/67629)
- Inaccurate I/O statistics of Compaction. [#67524](https://github.com/StarRocks/starrocks/pull/67524)
- Incorrect logic in physical partition comparison across clusters during replication transaction. [#67616](https://github.com/StarRocks/starrocks/pull/67616)
- Issue with SQL Server and Oracle identifier symbol handling. [#67965](https://github.com/StarRocks/starrocks/pull/67965)
- NPE in the Iceberg metadata table query due to missing configuration propagation. [#67151](https://github.com/StarRocks/starrocks/pull/67151)
- Issue with the `f``iles()` schema detection for empty Parquet or ORC files. [#67762](https://github.com/StarRocks/starrocks/pull/67762)
- Inaccurate value of metrics in Profile caused by UNION ALL on Hive tables. [#67912](https://github.com/StarRocks/starrocks/pull/67912)
- Lacking support for data retrieval from Arrow Flight proxy for FE queries. [#67794](https://github.com/StarRocks/starrocks/pull/67794)
- SIGSEGV crash during automatic partition creation caused by a race condition in `OlapTableSink::is_full()`. [#67566](https://github.com/StarRocks/starrocks/pull/67566)

## 3.5.11

Release date: January 5, 2026

### Improvements

- Supports Arrow Flight data retrieval from inaccessible nodes. [#66348](https://github.com/StarRocks/starrocks/pull/66348)
- Logs the cause (including the triggering process information) in the SIGTERM handler. [#66737](https://github.com/StarRocks/starrocks/pull/66737)
- Added an FE configuration `enable_statistic_collect_on_update` to control whether UPDATE statements can trigger automatic statistics collection. [#66794](https://github.com/StarRocks/starrocks/pull/66794)
- Supports configuring `networkaddress.cache.ttl`. [#66723](https://github.com/StarRocks/starrocks/pull/66723)
- Improve the “no rows imported” error message. [#66624](https://github.com/StarRocks/starrocks/pull/66624) [#66535](https://github.com/StarRocks/starrocks/pull/66535)
- Optimized `deltaRows` with lazy evaluation for large partition tables. [#66381](https://github.com/StarRocks/starrocks/pull/66381)
- Optimized materialized view rewrite performance. [#66623](https://github.com/StarRocks/starrocks/pull/66623)
- Supports single-tablet `ResultSink` optimization in shared-data clusters. [#66517](https://github.com/StarRocks/starrocks/pull/66517)
- `rewrite``_``simple``_``agg``_``to``_``meta``_``scan` is enabled by default. [#64698](https://github.com/StarRocks/starrocks/pull/64698)
- Supports pushing down GROUP BY expressions and materialized view rewrite. [#66507](https://github.com/StarRocks/starrocks/pull/66507)
- Add overloaded `newMessage` methods to improve materialized view logs. [#66367](https://github.com/StarRocks/starrocks/pull/66367)

### Bug Fixes

The following issues have been fixed:

- A Publish Compaction crash when the input rowset is not found. [#67154](https://github.com/StarRocks/starrocks/pull/67154)
- Significant CPU overhead and lock contention caused by repetitive invocation of `update_segment_cache_size` when querying tables with a large number of columns. [#66714](https://github.com/StarRocks/starrocks/pull/66714)
- `MulticastSinkOperator` stuck in the `OUTPUT_FULL` state. [#67153](https://github.com/StarRocks/starrocks/pull/67153)
- A “column not found” issue in the skew join hint. [#66929](https://github.com/StarRocks/starrocks/pull/66929)
- The growth of all tablets continues unabated, and the sum of pending and running tablets is not the total number of tablets. [#66718](https://github.com/StarRocks/starrocks/pull/66718)
- Transactions in the Compaction map built during Leader startup cannot be accessed by CompactionScheduler and will never be removed from the map. [#66533](https://github.com/StarRocks/starrocks/pull/66533)
- Delta Lake table refresh does not take effect. [#67156](https://github.com/StarRocks/starrocks/pull/67156)
- CN crash at queries against non-partitioned Iceberg tables with DATE predicates. [#66864](https://github.com/StarRocks/starrocks/pull/66864)
- Statements in Profiles cannot be correctly displayed when multiple statements are submitted. [#67097](https://github.com/StarRocks/starrocks/pull/67097)
- Missing dictionary information during collection because Meta Reader does not support reading from Delta column group files. [#66995](https://github.com/StarRocks/starrocks/pull/66995)
- Potential Java heap OOM in Java UDAF. [#67025](https://github.com/StarRocks/starrocks/pull/67025)
- BE crash due to the incorrect logic of ranking window optimization without PARTITION BY and ORDER BY. [#67081](https://github.com/StarRocks/starrocks/pull/67081)
- Misleading log level for timezone cache miss. [#66817](https://github.com/StarRocks/starrocks/pull/66817)
- Crash and incorrect results caused by the incorrect `can_use_bf` checking when merging runtime filters. [#67021](https://github.com/StarRocks/starrocks/pull/67021)
- Issue about pushing down runtime bitset filter with other OR predicates. [#66996](https://github.com/StarRocks/starrocks/pull/66996)
- Patch critical fix from lz4. [#67053](https://github.com/StarRocks/starrocks/pull/67053)
- AsyncTaskQueue deadlock issue. [#66791](https://github.com/StarRocks/starrocks/pull/66791)
- Cache inconsistency in ObjectColumn. [#66957](https://github.com/StarRocks/starrocks/pull/66957)
- RewriteUnnestBitmapRule causes wrong output column types. [#66855](https://github.com/StarRocks/starrocks/pull/66855)
- Data races and data loss when there are WRITE or FLUSH tasks after FINISH tasks in the Delta Writer. [#66943](https://github.com/StarRocks/starrocks/pull/66943)
- Invalid load channel and misleading internal errors caused by reopened load channels that were previously aborted. [#66793](https://github.com/StarRocks/starrocks/pull/66793)
- Bugs of Arrow Flight SQL. [#65889](https://github.com/StarRocks/starrocks/pull/65889)
- Issues when querying renamed columns with MetaScan. [#66819](https://github.com/StarRocks/starrocks/pull/66819)
- Hash column is not removed before flushing chunk in partitionwise spillable aggregation when skew elimination is off. [#66839](https://github.com/StarRocks/starrocks/pull/66839)
- BOOLEAN type default values were not correctly handled when stored as string literals. [#66818](https://github.com/StarRocks/starrocks/pull/66818)
- decimal2decimal cast unexpectedly returns the input column as the result directly. [#66773](https://github.com/StarRocks/starrocks/pull/66773)
- NPE in query planning during schema change. [#66811](https://github.com/StarRocks/starrocks/pull/66811)
- LocalTabletsChannel and LakeTabletsChannel deadlock. [#66748](https://github.com/StarRocks/starrocks/pull/66748)
- `publish_version` log shows empty `txn_ids` with new FE. [#66732](https://github.com/StarRocks/starrocks/pull/66732)
- Incorrect behavior of the FE configuration `statistic_collect_query_timeout`. [#66363](https://github.com/StarRocks/starrocks/pull/66363)
- UPDATE statements do not support statistics collection. [#66443](https://github.com/StarRocks/starrocks/pull/66443)
- Case rewrite errors related to low cardinality. [#66724](https://github.com/StarRocks/starrocks/pull/66724)
- Statistics query failure when the column list is empty. [#66138](https://github.com/StarRocks/starrocks/pull/66138)
- Usage/record mismatch when switching warehouse via hint. [#66677](https://github.com/StarRocks/starrocks/pull/66677)
- `ANALYZE TABLE` statements lack ExecTimeout. [#66361](https://github.com/StarRocks/starrocks/pull/66361)
- `array_map` returns wrong results from constant unary expressions. [#66514](https://github.com/StarRocks/starrocks/pull/66514)
- Foreign key constraints are lost after FE restart. [#66474](https://github.com/StarRocks/starrocks/pull/66474)
- `max(not null string)` on empty table throws `std::length_error`. [#66554](https://github.com/StarRocks/starrocks/pull/66554)
- Concurrency issue between Primary Key index Compaction and Apply. [#66282](https://github.com/StarRocks/starrocks/pull/66282)
- Improper behavior of `EXPLAIN <query>`. [#66542](https://github.com/StarRocks/starrocks/pull/66542)
- Issue when sinking DECIMAL128 to Iceberg table column. [#66071](https://github.com/StarRocks/starrocks/pull/66071)
- JSON length check issue for JSON → CHAR/VARCHAR when the target length equals the minimum. [#66628](https://github.com/StarRocks/starrocks/pull/66628)
- An expression children count error. [#66511](https://github.com/StarRocks/starrocks/pull/66511)

## 3.5.10

Release date: December 15, 2025

### Improvements

- Supports dumping plan node IDs in BE crash logs to speed up locating problematic operators. [#66454](https://github.com/StarRocks/starrocks/pull/66454)
- Optimized scans on the system views in `information_schema` to reduce the overhead. [#66200](https://github.com/StarRocks/starrocks/pull/66200)
- Added two histogram metrics (`slow_lock_held_time_ms` and `slow_lock_wait_time_ms`) to provide better observability for slow lock scenarios and distinguish between long-held locks and high lock contention. [#66027](https://github.com/StarRocks/starrocks/pull/66027)
- Optimized replica lock handling in tablet report and clone flows by switching the lock from database level to table level, reducing lock contention and improving scheduling efficiency. [#61939](https://github.com/StarRocks/starrocks/pull/61939)
- Avoided outputting columns in BE storage, and pushed down predicate computation to BE storage. [#60462](https://github.com/StarRocks/starrocks/pull/60462)  
- Improved query profile accuracy when deploying scan ranges in background threads. [#62223](https://github.com/StarRocks/starrocks/pull/62223)
- Improved profile accounting when deploying additional tasks, so CPU time is not repetitively counted. [#62186](https://github.com/StarRocks/starrocks/pull/62186)
- Added more detailed error messages when a referenced partition does not exist, making failures easier to diagnose. [#65674](https://github.com/StarRocks/starrocks/pull/65674)
- Made sample-type cardinality estimation more robust in corner cases to improve row-count estimates. [#65599](https://github.com/StarRocks/starrocks/pull/65599)
- Added a partition filter when loading statistics to prevent INSERT OVERWRITE from reading stale partition statistics. [#65578](https://github.com/StarRocks/starrocks/pull/65578)
- Splited pipeline CPU `execution_time` metrics into separate series for queries and loads, improving observability by workload type. [#65535](https://github.com/StarRocks/starrocks/pull/65535)
- Supported `enable_statistic_collect_on_first_load` at table granularity for finer-grained control over statistics collection on the first load. [#65463](https://github.com/StarRocks/starrocks/pull/65463)
- Renamed the S3-dependent unit test from `PocoClientTest` to an S3-specific name to better reflect its dependency and intent. [#65524](https://github.com/StarRocks/starrocks/pull/65524)

### Bug Fixes

The following issues have been fixed:

- libhdfs crashes when StarRocks is started with an incompatible JDK. [#65882](https://github.com/StarRocks/starrocks/pull/65882)
- Incorrect query results caused by `PartitionColumnMinMaxRewriteRule`. [#66356](https://github.com/StarRocks/starrocks/pull/66356)
- Rewrite issues due to the materialized view metadata not refreshed when resolving materialized views by AST keys. [#66472](https://github.com/StarRocks/starrocks/pull/66472)
- The trim function crashes or produces wrong results when trimming specific Unicode whitespace characters. [#66428](https://github.com/StarRocks/starrocks/pull/66428), [#66477](https://github.com/StarRocks/starrocks/pull/66477)
- Failures in load metadata and SQL execution that still referenced a deleted warehouse. [#66436](https://github.com/StarRocks/starrocks/pull/66436)
- Wrong results when group execution Join is combined with window functions. [#66441](https://github.com/StarRocks/starrocks/pull/66441)
- A possible FE null pointer in `resetDecommStatForSingleReplicaTabletUnlocked`. [#66034](https://github.com/StarRocks/starrocks/pull/66034)
- Missing Join runtime filter pushdown optimization in shared-data clusters for `LakeDataSource`. [#66354](https://github.com/StarRocks/starrocks/pull/66354)
- Parameters are inconsistent for runtime filter transmit options (timeouts, HTTP RPC limits, etc.) because they are not forwarded to receivers. [#66393](https://github.com/StarRocks/starrocks/pull/66393)
- Automatic partition creation fails when partition values already exist. [#66167](https://github.com/StarRocks/starrocks/pull/66167)
- Inaccurate scan statistics in audit logs when predicates have high selectivity. [#66280](https://github.com/StarRocks/starrocks/pull/66280)
- Incorrect query results because non-deterministic functions are pushed below operators. [#66323](https://github.com/StarRocks/starrocks/pull/66323)
- Exponential growth in the number of expressions caused by CASE WHEN. [#66324](https://github.com/StarRocks/starrocks/pull/66324)
- Materialized view compensation bugs when the same table appears multiple times in a query with different partition predicates. [#66369](https://github.com/StarRocks/starrocks/pull/66369)
- BE becomes unresponsive when using fork in subprocesses. [#66334](https://github.com/StarRocks/starrocks/pull/66334)
- CVE-2025-66566 and CVE-2025-12183. [#66453](https://github.com/StarRocks/starrocks/pull/66453), [#66362](https://github.com/StarRocks/starrocks/pull/66362)
- Errors caused by nested CTE reuse. [#65800](https://github.com/StarRocks/starrocks/pull/65800)
- Issues due to the lack of validation on conflicting schema-change clauses. [#66208](https://github.com/StarRocks/starrocks/pull/66208)  
- Improper rowset GC behavior when rowset commit fails. [#66301](https://github.com/StarRocks/starrocks/pull/66301)  
- A potential use-after-free when counting down pipelines. [#65940](https://github.com/StarRocks/starrocks/pull/65940)  
- The `W``arehouse` field is NULL in `information_schema.loads` for Stream Load. [#66202](https://github.com/StarRocks/starrocks/pull/66202)  
- Issues with materialized view creation when the referenced view has the same name as its base table. [#66274](https://github.com/StarRocks/starrocks/pull/66274)  
- The global dictionary is not updated correctly under some cases. [#66194](https://github.com/StarRocks/starrocks/pull/66194)  
- Incorrect query profile logging for queries forwarded from Follower nodes. [#64395](https://github.com/StarRocks/starrocks/pull/64395)  
- BE crash when caching SELECT results and reordering schema. [#65850](https://github.com/StarRocks/starrocks/pull/65850)  
- Shadow partitions are dropped when dropping partitions by expression. [#66171](https://github.com/StarRocks/starrocks/pull/66171)  
- DROP tasks run when a CLONE task exists for the same tablet. [#65780](https://github.com/StarRocks/starrocks/pull/65780)  
- Stability and observability issues because RocksDB log file options were not properly set. [#66166](https://github.com/StarRocks/starrocks/pull/66166)  
- Incorrect materialized view compensation that could produce NULL results. [#66216](https://github.com/StarRocks/starrocks/pull/66216)  
- BE reported as alive even after receiving `SIGSEGV`. [#66212](https://github.com/StarRocks/starrocks/pull/66212)  
- Bugs in Iceberg scans. [#65658](https://github.com/StarRocks/starrocks/pull/65658)  
- Regression coverage and stability issues for Iceberg view SQL test cases. [#66126](https://github.com/StarRocks/starrocks/pull/66126)  
- Unexpected behavior because `set_collector` is invoked repetitively. [#66199](https://github.com/StarRocks/starrocks/pull/66199)  
- Ingestion failures when column-mode partial updates are used together with conditional updates. [#66139](https://github.com/StarRocks/starrocks/pull/66139)  
- Temporary partition value conflicts under concurrent transactions. [#66025](https://github.com/StarRocks/starrocks/pull/66025)  
- An Iceberg table cache bug where Guava LocalCache could retain stale entries even when `cache.size() == 0`, causing refresh to be ineffective and queries to return outdated tables. [#65917](https://github.com/StarRocks/starrocks/pull/65917)  
- Incorrect format placeholder in `LargeInPredicateException`, causing the actual number of LargeInPredicate occurrences to be incorrectly reported in the error message. [#66152](https://github.com/StarRocks/starrocks/pull/66152)  
- NullPointerException in `ConnectScheduler’s` timeout checker when connectContext is null. [#66136](https://github.com/StarRocks/starrocks/pull/66136)  
- Crashes caused by unhandled exceptions thrown from threadpool tasks. [#66114](https://github.com/StarRocks/starrocks/pull/66114)  
- Data loss when pushing down DISTINCT LIMIT in certain plans. [#66109](https://github.com/StarRocks/starrocks/pull/66109)  
- `multi_distinct_count` not updating `distinct_size` after the underlying hash set is converted to a two-level hash set, which could lead to incorrect distinct counts. [#65916](https://github.com/StarRocks/starrocks/pull/65916)  
- A race condition when an exec group submits the next driver which could trigger `Check failed: !driver->is_in_blocked() `and abort the BE process. [#66099](https://github.com/StarRocks/starrocks/pull/66099)  
- INSERT failures when running ALTER TABLE ADD COLUMN with a default value concurrently with INSERT, due to mismatched types for the newly added column’s default expression. [#65968](https://github.com/StarRocks/starrocks/pull/65968) 
- An issue where `MemoryScratchSinkOperator` could remain in `pending_finish` after `RecordBatchQueue` was shut down when SparkSQL exited early, causing the pipeline to hang. [#66041](https://github.com/StarRocks/starrocks/pull/66041)  
- A core dump when reading Parquet files that contain empty row groups. [#65928](https://github.com/StarRocks/starrocks/pull/65928)  
- Recursive calls and potential stack overflow at high DOP because the event scheduler’s readiness check is complicated. [#66016](https://github.com/StarRocks/starrocks/pull/66016)  
- Asynchronous materialized view refresh skips updates when the Iceberg base table contains expired snapshots. [#65969](https://github.com/StarRocks/starrocks/pull/65969)  
- Potential issues in predicate reuse and rewrite because the optimizer relies solely on hashCode to distinguish differences in predicates. [#65999](https://github.com/StarRocks/starrocks/pull/65999)  
- In the refresh of an asynchronous materialized view with multi-level partitioned base tables, only the parent partition metadata was checked, while sub-partition updates were skipped. [#65596](https://github.com/StarRocks/starrocks/pull/65596)  
- Statistics collection issues where AVG(ARRAY_LENGTH(...)) could return NULL for empty result sets. [#65788](https://github.com/StarRocks/starrocks/pull/65788)  
- Runtime profile counters are not correctly updating or clearing their min/max values during incremental updates on both BE and FE. [#65869](https://github.com/StarRocks/starrocks/pull/65869)  
- Incorrect logic to obtain the image journal ID when creating a cluster snapshot to ensure the snapshot uses the correct log position. [#65970](https://github.com/StarRocks/starrocks/pull/65970)  
- Results are misreported when the cleanup fails due to incorrect status checking logic in file cleanup error handling. [#65709](https://github.com/StarRocks/starrocks/pull/65709)  
- A possible infinite loop in certain plans isVariable() in DictMappingOperator. [#65743](https://github.com/StarRocks/starrocks/pull/65743)  
- Failures and missing audit/profile data because ConnectContext is not passed into scan-range deployment threads. [#63544](https://github.com/StarRocks/starrocks/pull/63544)  
- A use-after-free issue in the local Primary Key index manager when the storage engine is stopped. [#65534](https://github.com/StarRocks/starrocks/pull/65534)  
- Statistics collection issues for INSERT OVERWRITE with dynamic overwrite. [#65657](https://github.com/StarRocks/starrocks/pull/65657)  
- Concurrency issues caused by coarse-grained locks in DiskAndTabletLoadReBalancer. [#65557](https://github.com/StarRocks/starrocks/pull/65557)  
- Slow locks cannot be detected and reported correctly for the lack of slow-lock detection for critical locks. [#65559](https://github.com/StarRocks/starrocks/pull/65559)  
- NullPointerException when replaying upsert transaction state after the target database has been dropped. [#65595](https://github.com/StarRocks/starrocks/pull/65595)  
- Stale statistics were used because outdated partition statistics are not dropped after statistics collection triggered by INSERT OVERWRITE. [#65586](https://github.com/StarRocks/starrocks/pull/65586)  
- Data race in partition ID allocation that could lead to ID conflicts under concurrency. [#65608](https://github.com/StarRocks/starrocks/pull/65608)  
- Missing tablet IDs when retrieving initial tablet metadata. [#65550](https://github.com/StarRocks/starrocks/pull/65550)  
- Incorrect record information for PREPARE/EXECUTE statements in audit and profile logs. [#65448](https://github.com/StarRocks/starrocks/pull/65448)  
- Potential crashes because the non–thread-safe has_output function is called from multiple threads. [#65514](https://github.com/StarRocks/starrocks/pull/65514)
- MemTable finalize tasks cannot be properly tracked because the `memtable_finalize_task_total` counter metric is lacking. [#65548](https://github.com/StarRocks/starrocks/pull/65548)  
- Query ID collisions in Arrow Flight, causing multiple queries no longer share the same query ID. [#65558](https://github.com/StarRocks/starrocks/pull/65558)  
- Lock conflicts for `TabletChecker.doCheck()` with other operations. [#65237](https://github.com/StarRocks/starrocks/pull/65237)  
- Scan behavior is inconsistent between shared-data and shared-nothing clusters, causing query semantics to differ. [#61100](https://github.com/StarRocks/starrocks/pull/61100)

## 3.5.9

Release date: November 26, 2025

### Improvements

- Added transaction latency metrics to FE for observing timing across transaction stages. [#64948](https://github.com/StarRocks/starrocks/pull/64948)
- Supports overwriting S3 unpartitioned Hive tables to simplify full-table rewrites in data lake scenarios. [#65340](https://github.com/StarRocks/starrocks/pull/65340)
- Introduced CacheOptions to provide finer-grained control over tablet metadata caching. [#65222](https://github.com/StarRocks/starrocks/pull/65222)
- Supports sample statistics collection for INSERT OVERWRITE to ensure statistics stay consistent with the latest data. [#65363](https://github.com/StarRocks/starrocks/pull/65363)
- Optimized the statistics collection strategy after INSERT OVERWRITE to avoid missing or incorrect statistics due to asynchronous tablet reports. [#65327](https://github.com/StarRocks/starrocks/pull/65327)
- Introduced a retention period for partitions dropped or replaced by INSERT OVERWRITE or materialized view refresh operations, keeping them in the recycle bin for a while to improve recoverability. [#64779](https://github.com/StarRocks/starrocks/pull/64779)

### Bug Fixes

The following issues have been fixed:

- Lock contention and concurrency issues related to `LocalMetastore.truncateTable()`. [#65191](https://github.com/StarRocks/starrocks/pull/65191)
- Lock contention and replica check performance issues related to TabletChecker. [#65312](https://github.com/StarRocks/starrocks/pull/65312)
- Incorrect error logging when changing user via HTTP SQL. [#65371](https://github.com/StarRocks/starrocks/pull/65371)
- Checksum failures caused by DelVec CRC32 upgrade compatibility issues. [#65442](https://github.com/StarRocks/starrocks/pull/65442)
- Tablet metadata load failures caused by RocksDB iteration timeout. [#65146](https://github.com/StarRocks/starrocks/pull/65146)
- When the internal `flat_path` string is empty because the JSON hyper extraction path is `$` or all paths are skipped, calling `substr` will throw an exception and cause BE crash. [#65260](https://github.com/StarRocks/starrocks/pull/65260)
- The PREPARED flag in fragment execution is not correctly set. [#65423](https://github.com/StarRocks/starrocks/pull/65423)
- Inaccurate write and flush metrics caused by duplicated load profile counters. [#65252](https://github.com/StarRocks/starrocks/pull/65252)
- When multiple HTTP requests reuse the same TCP connection, if a non‑ExecuteSQL request arrives after an ExecuteSQL request, the `HttpConnectContext` cannot be unregistered at channel close, causing HTTP context leaks. [#65203](https://github.com/StarRocks/starrocks/pull/65203)
- MySQL 8.0 schema introspection errors (Fixed by adding session variables `default_authentication_plugin` and `authentication_policy`). [#65330](https://github.com/StarRocks/starrocks/pull/65330)
- SHOW ANALYZE STATUS errors caused by unnecessary statistics collection for temporary partitions created after partition overwrite operations. [#65298](https://github.com/StarRocks/starrocks/pull/65298)
- Global Runtime Filter race in the Event Scheduler. [#65200](https://github.com/StarRocks/starrocks/pull/65200)
- Data Cache is aggressively disabled because the minimum Data Cache disk size constraint is too large. [#64909](https://github.com/StarRocks/starrocks/pull/64909)
- An aarch64 build issue related to the `gold` linker automatic fallback. [#65156](https://github.com/StarRocks/starrocks/pull/65156)

## 3.5.8

Release date: November 10, 2025

### Improvements

- Upgraded Arrow to 19.0.1 to support the Parquet legacy list to include nested, complex files. [#64238](https://github.com/StarRocks/starrocks/pull/64238)
- FILES() supports legacy Parquet LIST encodings. [#64160](https://github.com/StarRocks/starrocks/pull/64160)
- Automatically determine the Partial Update mode based on the session variable and the number of inserted columns. [#62091](https://github.com/StarRocks/starrocks/pull/62091)
- Applied low-cardinality optimization on analytic operators above table functions. [#63378](https://github.com/StarRocks/starrocks/pull/63378)
- Added configurable table lock timeout to `finishTransaction` to avoid blocking. [#63981](https://github.com/StarRocks/starrocks/pull/63981)
- Shared-data clusters support table-level scan metrics attribution. [#62832](https://github.com/StarRocks/starrocks/pull/62832)
- Window functions LEAD/LAG/FIRST_VALUE/LAST_VALUE now accept ARRAY type arguments. [#63547](https://github.com/StarRocks/starrocks/pull/63547)
- Supports constant folding for several array functions to improve predicate pushdown and join simplification. [#63692](https://github.com/StarRocks/starrocks/pull/63692)
- Supports batched API to optimize `tabletNum` retrieval for a given node via `SHOW PROC /backends/{id}`. Added an FE configuration item `enable_collect_tablet_num_in_show_proc_backend_disk_path` (Default: `true`). [#64013](https://github.com/StarRocks/starrocks/pull/64013)
- Ensured `INSERT ... SELECT` reads the freshest metadata by refreshing external tables before planning. [#64026](https://github.com/StarRocks/starrocks/pull/64026)
- Added `capacity_limit_reached` checks to table functions, NL-join probe, and hash-join probe to avoid constructing overflowing columns. [#64009](https://github.com/StarRocks/starrocks/pull/64009)
- Added FE configuration item `collect_stats_io_tasks_per_connector_operator` (Default: `4`) for setting the maximum number of tasks to collect statistics for external tables. [#64016](https://github.com/StarRocks/starrocks/pull/64016)
- Updated the default partition size for sample collection from 1000 to 300. [#64022](https://github.com/StarRocks/starrocks/pull/64022)
- Increased lock table slots to 256 and added `rid` to slow-lock logs. [#63945](https://github.com/StarRocks/starrocks/pull/63945)
- Improved robustness of Gson deserialization in the presence of legacy data. [#63555](https://github.com/StarRocks/starrocks/pull/63555)
- Reduced metadata lock scope for FILES() schema pushdown to cut lock contention and planning latency. [#63796](https://github.com/StarRocks/starrocks/pull/63796)
- Added Task Run execute timeout checker by introducing an FE configuration item `task_runs_timeout_second`, and refined cancellation logics for overdue runs. [#63842](https://github.com/StarRocks/starrocks/pull/63842)
- Ensured `REFRESH MATERIALIZED VIEW ... FORCE` always refreshes target partitions (even in inconsistent or corrupted cases). [#63844](https://github.com/StarRocks/starrocks/pull/63844)

### Bug Fixes

The following issues have been fixed:

- An exception when parsing the Nullable (Decimal) type of ClickHouse. [#64195](https://github.com/StarRocks/starrocks/pull/64195)
- An issue with tablet migration and Primary Key index lookup concurrency. [#64164](https://github.com/StarRocks/starrocks/pull/64164)
- Lack of FINISHED status in materialized view refresh. [#64191](https://github.com/StarRocks/starrocks/pull/64191)
- Schema Change Publish does not retry in shared-data clusters. [#64093](https://github.com/StarRocks/starrocks/pull/64093)
- Wrong row count statistics on Primary Key tables in Data Lake. [#64007](https://github.com/StarRocks/starrocks/pull/64007)
- When tablet creation times out in shared-data clusters, node information cannot be returned. [#63963](https://github.com/StarRocks/starrocks/pull/63963)
- Corrupted Lake DataCache cannot be cleared. [#63182](https://github.com/StarRocks/starrocks/pull/63182)
- Window function with IGNORE NULLS flags can not be consolidated with its counterpart without iIGNORE NULLS flag. [#63958](https://github.com/StarRocks/starrocks/pull/63958)
- Table compaction cannot be scheduled again after FE restart if the compaction was previously aborted. [#63881](https://github.com/StarRocks/starrocks/pull/63881)
- Tasks fail to be scheduled if FE restarts frequently. [#63966](https://github.com/StarRocks/starrocks/pull/63966)
- An issue with GCS error codes. [#64066](https://github.com/StarRocks/starrocks/pull/64066)
- Instability issue with StarMgr gRPC executor. [#63828](https://github.com/StarRocks/starrocks/pull/63828)
- Deadlock when creating an exclusive work group. [#63893](https://github.com/StarRocks/starrocks/pull/63893)
- Cache for Iceberg tables is not properly invalidated. [#63971](https://github.com/StarRocks/starrocks/pull/63971)
- Wrong results for sorted aggregation in shared-data clusters. [#63849](https://github.com/StarRocks/starrocks/pull/63849)
- ASAN error in `PartitionedSpillerWriter::_remove_partition`. [#63903](https://github.com/StarRocks/starrocks/pull/63903)
- BE crash when failing to get splits from morsel queue. [#62753](https://github.com/StarRocks/starrocks/pull/62753)
- A bug with aggregate push-down type cast in materialized view rewrite. [#63875](https://github.com/StarRocks/starrocks/pull/63875)
- NPE when removing expired load jobs in FE. [#63820](https://github.com/StarRocks/starrocks/pull/63820)
- Partitioned Spill crash when removing partitions. [#63825](https://github.com/StarRocks/starrocks/pull/63825)
- Materialized view rewrite throws `IllegalStateException` under certain plans. [#63655](https://github.com/StarRocks/starrocks/pull/63655)
- NPE when creating a partitioned materialized view. [#63830](https://github.com/StarRocks/starrocks/pull/63830)

## 3.5.7

Release date: October 21, 2025

### Improvements

- Improved memory statistics accuracy for Scan operators by introducing retry backoff under heavy memory contention scenarios. [#63788](https://github.com/StarRocks/starrocks/pull/63788)
- Optimized materialized view bucketing inference by leveraging existing tablet distribution to prevent excessive bucket creation. [#63367](https://github.com/StarRocks/starrocks/pull/63367)
- Revised the Iceberg table caching mechanism to enhance consistency and reduce cache invalidation risks during frequent metadata updates. [#63388](https://github.com/StarRocks/starrocks/pull/63388)
- Added the `querySource` field to `QueryDetail` and `AuditEvent` for better traceability of query origins across APIs and schedulers. [#63480](https://github.com/StarRocks/starrocks/pull/63480)
- Enhanced Persistent Index diagnostics by printing detailed context when duplicate keys are detected in MemTable writes. [#63560](https://github.com/StarRocks/starrocks/pull/63560)
- Reduced lock contention in materialized view operations by refining lock granularity and sequencing in concurrent scenarios. [#63481](https://github.com/StarRocks/starrocks/pull/63481)

### Bug Fixes

The following issues have been fixed:

- Materialized view rewrite failures caused by type mismatch. [#63659](https://github.com/StarRocks/starrocks/pull/63659)  
- `regexp_extract_all` has wrong behavior and lacks support for `pos=0`. [#63626](https://github.com/StarRocks/starrocks/pull/63626)  
- Degraded scan performance caused by the profitless simplification of CASE WHEN with complex functions. [#63732](https://github.com/StarRocks/starrocks/pull/63732)  
- Incorrect DCG data reading when partial updates switch from column mode to row mode. [#61529](https://github.com/StarRocks/starrocks/pull/61529)  
- A potential deadlock during initialization of `ExceptionStackContext`. [#63776](https://github.com/StarRocks/starrocks/pull/63776)  
- Crashes in Parquet numeric conversion for ARM architecture machines. [#63294](https://github.com/StarRocks/starrocks/pull/63294)  
- An issue caused by the aggregate intermediate type uses `ARRAY<NULL_TYPE>`. [#63371](https://github.com/StarRocks/starrocks/pull/63371)
- Stability issue caused by incorrect overflow detection when casting LARGEINT to DECIMAL128 at sign-edge cases (for example, INT128_MIN) [#63559](https://github.com/StarRocks/starrocks/pull/63559)
- LZ4 compression and decompression errors cannot be perceived. [#63629](https://github.com/StarRocks/starrocks/pull/63629)
- `ClassCastException` when querying tables partitioned by `FROM_UNIXTIME` on INT-type columns. [#63684](https://github.com/StarRocks/starrocks/pull/63684)
- Tablets cannot be repaired after a balance-triggered migration when the only valid source replica is marked `DECOMMISSION`. [#62942](https://github.com/StarRocks/starrocks/pull/62942)
- Profiles lost SQL statements and Planner Trace when the PREPARE statement is used. [#63519](https://github.com/StarRocks/starrocks/pull/63519)
- The `extract_number`, `extract_bool`, and `extract_string` functions are not exception-safe. [#63575](https://github.com/StarRocks/starrocks/pull/63575)
- Shutdown tablets cannot be garbage-collected properly. [#63595](https://github.com/StarRocks/starrocks/pull/63595)
- Profiles showing SQL as `omit` for returns of the PREPARE/EXECUTE statements. [#62988](https://github.com/StarRocks/starrocks/pull/62988)
- `date_trunc` partition pruning with combined predicates that mistakenly produced EMPTYSET. [#63464](https://github.com/StarRocks/starrocks/pull/63464)
- Crashes in release builds due to the CHECK in NullableColumn. [#63553](https://github.com/StarRocks/starrocks/pull/63553)

## 3.5.6

Release date: September 22, 2025

### Improvements

- A decommissioned BE will be forcibly dropped when all its tablets are in the recycle bin, to avoid the decommission being blocked by those tablets. [#62781](https://github.com/StarRocks/starrocks/pull/62781)
- Vacuum metrics will be updated when Vacuum succeeds. [#62540](https://github.com/StarRocks/starrocks/pull/62540)
- Added thread pool metrics to the fragment instance execution state report, including active threads, queue count, and running threads. [#63067](https://github.com/StarRocks/starrocks/pull/63067)
- Supports S3 path-style access in shared-data clusters to improve compatibility with MinIO and other S3-compatible storage systems. You can enable this feature by setting `aws.s3.enable_path_style_access` to `true` when creating a storage volume. [#62591 ](https://github.com/StarRocks/starrocks/pull/62591)
- Supports resetting the starting point of the AUTO_INCREMENT value via `ALTER TABLE`` <table_name>`` AUTO_INCREMENT`` = 10000;`. [#62767 ](https://github.com/StarRocks/starrocks/pull/62767)
- Supports using Distinguished Name (DN) in Group Provider for group matching, improving the user group solution for LDAP/Microsoft Active Directory environments.  [#62711](https://github.com/StarRocks/starrocks/pull/62711)
- Supports Azure Workload Identity authentication for Azure Data Lake Storage Gen2.  [#62754](https://github.com/StarRocks/starrocks/pull/62754)
- Added transaction error messages to the `information_schema.``loads` view to aid failure diagnosis. [#61364](https://github.com/StarRocks/starrocks/pull/61364)
- Supports reusing common expressions for complex CASE WHEN expressions in Scan predicates to reduce repetitive computation. [#62779](https://github.com/StarRocks/starrocks/pull/62779)
- Uses the REFRESH (instead of ALTER) privilege on the materialized view to execute REFRESH statements. [#62636](https://github.com/StarRocks/starrocks/pull/62636)
- Disabled low-cardinality optimization on Lake tables by default to avoid potential issues. [#62586](https://github.com/StarRocks/starrocks/pull/62586)
- Enabled tablet balancing between workers by default in shared-data clusters. [#62661](https://github.com/StarRocks/starrocks/pull/62661)
- Supports reusing expressions in outer-join WHERE predicates to reduce repetitive computation. [#62139](https://github.com/StarRocks/starrocks/pull/62139)
- Added Clone metrics in FE. [#62421](https://github.com/StarRocks/starrocks/pull/62421)
- Added Clone metrics in BE. [#62479](https://github.com/StarRocks/starrocks/pull/62479)
- Added an FE configuration item `enable_statistic_cache_refresh_after_write` to disable statistics-cache lazy refresh by default. [#62518](https://github.com/StarRocks/starrocks/pull/62518)
- Masked credential information in SUBMIT TASK for better security. [#62311](https://github.com/StarRocks/starrocks/pull/62311)
- `json_extract` in the Trino dialect returns a JSON type. [#59718](https://github.com/StarRocks/starrocks/pull/59718)
- Supports ARRAY type in `null_or_empty`. [#62207](https://github.com/StarRocks/starrocks/pull/62207)
- Adjusted the size limit for the Iceberg manifest cache. [#61966](https://github.com/StarRocks/starrocks/pull/61966)
- Added a remote file-cache limit for Hive. [#62288](https://github.com/StarRocks/starrocks/pull/62288)

### Bug Fixes

The following issues have been fixed:

- Secondary replicas hang indefinitely due to negative timeout values, which cause incorrect timestamp comparisons. [#62805](https://github.com/StarRocks/starrocks/pull/62805)
- PublishTask may be blocked when TransactionState is REPLICATION. [#61664](https://github.com/StarRocks/starrocks/pull/61664)
- Incorrect repair mechanism for Hive tables that have been dropped and recreated during materialized view refresh. [#63072](https://github.com/StarRocks/starrocks/pull/63072)
- Incorrect execution plans were generated after the materialized view aggregation push‑down rewrite. [#63060](https://github.com/StarRocks/starrocks/pull/63060)
- ANALYZE PROFILE failures caused by PlanTuningGuide producing unrecognized strings (null explainString) in the query profiles. [#63024](https://github.com/StarRocks/starrocks/pull/63024)
- Inappropriate return type of  `hour_from_unixtime` and incorrect rewrite rule of `CAST`. [#63006  ](https://github.com/StarRocks/starrocks/pull/63006)
- NPE in Iceberg manifest cache under data races. [#63043  ](https://github.com/StarRocks/starrocks/pull/63043)
- Shared-data clusters lack support for colocation in materialized views. [#62941 ](https://github.com/StarRocks/starrocks/pull/62941) 
- Iceberg table Scan Exception during Scan Range deployment.[ #62994 ](https://github.com/StarRocks/starrocks/pull/62994) 
- Incorrect execution plans were generated for view-based rewrite. [#62918](https://github.com/StarRocks/starrocks/pull/62918)
- Errors and disrupted tasks due to Compute Nodes are not gracefully shut down on exit. [#62916 ](https://github.com/StarRocks/starrocks/pull/62916) 
- NPE when Stream Load execution status updates. [#62921](https://github.com/StarRocks/starrocks/pull/62921)
- An issue with statistics when the column name and the name in the PARTITION BY clause differ in case. [#62953](https://github.com/StarRocks/starrocks/pull/62953)
- Wrong results are returned when the `LEAST` function is used as a predicate. [#62826](https://github.com/StarRocks/starrocks/pull/62826)  
- Invalid ProjectOperator above the table-pruning frontier CTEConsumer. [#62914](https://github.com/StarRocks/starrocks/pull/62914)  
- Redundant replica handling after Clone. [#62542](https://github.com/StarRocks/starrocks/pull/62542)  
- Failed to collect Stream Load profiles. [#62802](https://github.com/StarRocks/starrocks/pull/62802)  
- Ineffective disk rebalancing caused by improper BE selection. [#62776](https://github.com/StarRocks/starrocks/pull/62776)  
- A potential NPE crash in LocalTabletsChannel when a missing `tablet_id` leads to a null delta writer.  [#62861](https://github.com/StarRocks/starrocks/pull/62861)
- KILL ANALYZE does not take effect. [ #62842](https://github.com/StarRocks/starrocks/pull/62842)
- SQL syntax errors in histogram stats when MCV values contain single quotes. [#62853](https://github.com/StarRocks/starrocks/pull/62853)
- Incorrect output format of metrics for Prometheus. [#62742](https://github.com/StarRocks/starrocks/pull/62742)
- NPE when querying `information_schema.analyze_status` after the database is dropped. [#62796](https://github.com/StarRocks/starrocks/pull/62796)
- CVE-2025-58056. [#62801](https://github.com/StarRocks/starrocks/pull/62801)
- When SHOW CREATE ROUTINE LOAD is executed, wrong results are returned because the database is considered null if not specified. [#62745](https://github.com/StarRocks/starrocks/pull/62745)
- Data loss caused by incorrectly skipping CSV headers in `files()`. [#62719](https://github.com/StarRocks/starrocks/pull/62719)
- NPE when replaying batch-transaction upserts. [#62715](https://github.com/StarRocks/starrocks/pull/62715)
- Publish being incorrectly reported as successful during graceful shutdown in shared-nothing clusters. [#62417](https://github.com/StarRocks/starrocks/pull/62417)
- Crash in asynchronous delta writer due to a null pointer. [#62626](https://github.com/StarRocks/starrocks/pull/62626)
- Materialized view refresh is skipped because the materialized view version map is not cleared after a failed restore job. [#62634](https://github.com/StarRocks/starrocks/pull/62634)
- Issues caused by case-sensitive partition column validation in the materialized view analyzer. [#62598](https://github.com/StarRocks/starrocks/pull/62598)
- Duplicate IDs for statements with syntax errors. [#62258](https://github.com/StarRocks/starrocks/pull/62258)
- StatisticsExecutor status is overridden due to redundant state assignment in CancelableAnalyzeTask. [#62538](https://github.com/StarRocks/starrocks/pull/62538)
- Incorrect error messages produced by statistics collection. [#62533](https://github.com/StarRocks/starrocks/pull/62533)
- Premature throttling caused by insufficient default maximum connections for external users. [#62523](https://github.com/StarRocks/starrocks/pull/62523)
- A potential NPE in materialized view backup and restore operations. [#62514](https://github.com/StarRocks/starrocks/pull/62514)
- Incorrect `http_workers_num` metric. [#62457](https://github.com/StarRocks/starrocks/pull/62457)
- The runtime filter fails to locate the corresponding execution group during construction. [#62465](https://github.com/StarRocks/starrocks/pull/62465)
- Tedious results on Scan Node caused by simplifying CASE WHEN with complex functions. [#62505](https://github.com/StarRocks/starrocks/pull/62505)
- `gmtime` is not thread-safe. [#60483](https://github.com/StarRocks/starrocks/pull/60483)
- An issue with getting Hive partitions with escaped strings. [#59032](https://github.com/StarRocks/starrocks/pull/59032)

## 3.5.5

Release date: September 5, 2025

### Improvements

- Added a new system variable `enable_drop_table_check_mv_dependency` (default: `false`). When set to `true`, if the object to be dropped is referenced by a downstream materialized view, the system prevents the execution of `DROP TABLE` / `DROP VIEW` / `DROP MATERIALIZED VIEW`. The error message lists the dependent materialized views and suggests checking the `sys.object_dependencies` view for details. [#61584](https://github.com/StarRocks/starrocks/pull/61584)
- Logs now include the Linux distribution and CPU architecture of the build, to facilitate issue reproduction and troubleshooting. Log format: `... build <hash> distro <id> arch <arch>`. [#62017](https://github.com/StarRocks/starrocks/pull/62017)
- Persisted per-Tablet index and incremental column group file sizes are now cached, replacing on-demand directory scans. This accelerates Tablet status reporting in BE and reduces latency under high I/O scenarios. [#61901](https://github.com/StarRocks/starrocks/pull/61901)
- Downgraded several high-frequency INFO logs in FE and BE to VLOG, and aggregated task submission logs, significantly reducing redundant storage-related logs and log volume under heavy load. [#62121](https://github.com/StarRocks/starrocks/pull/62121)
- Improved query performance for External Catalog metadata through `information_schema` by pushing table filters before calling `getTable`, avoiding per-table RPCs. [#62404](https://github.com/StarRocks/starrocks/pull/62404)

### Bug Fixes

The following issues have been fixed:

- NullPointerException when fetching partition-level column statistics during the Plan stage due to missing data. [#61935](https://github.com/StarRocks/starrocks/pull/61935)
- Fixed Parquet write issues with non-empty NULL arrays, and corrected `SPLIT(NULL, …)` behavior to consistently return NULL, preventing data corruption and runtime errors. [#61999](https://github.com/StarRocks/starrocks/pull/61999)
- Failure when creating materialized views using `CASE WHEN` expressions due to incompatible VARCHAR type returns (fixed by ensuring consistency before and after refresh, and introducing a new FE configuration `transform_type_prefer_string_for_varchar` to prefer STRING and avoid length mismatch). [#61996](https://github.com/StarRocks/starrocks/pull/61996)
- Statistics for nested CTEs could not be computed outside of memo when `enable_rbo_table_prune` was `false`. [#62070](https://github.com/StarRocks/starrocks/pull/62070)
- In Audit Logs, inaccurate Scan Rows results for INSERT INTO SELECT statements. [#61381](https://github.com/StarRocks/starrocks/pull/61381)
- ExceptionInInitializerError/NullPointerException during initialization caused FE startup failure when Query Queue v2 was enabled. [#62161](https://github.com/StarRocks/starrocks/pull/62161)
- BE crash when `LakePersistentIndex` initialization failed and `_memtable` cleanup was triggered. [#62279](https://github.com/StarRocks/starrocks/pull/62279)
- Permission issues during materialized view refresh due to creator roles not being activated (fixed by adding FE configuration `mv_use_creator_based_authorization`. When set to `false`, materialized views are refreshed as root, for compatibility with LDAP-authenticated clusters). [#62396](https://github.com/StarRocks/starrocks/pull/62396)
- Materialized view refresh failures caused by case-sensitive List partition table names (fixed by enforcing case-insensitive uniqueness checks on partition names, aligning with OLAP table semantics). [#62389](https://github.com/StarRocks/starrocks/pull/62389)

## 3.5.4

Release Date: August 22, 2025

### Improvements

- Added logs to clarify the reason that tablets cannot be repaired.  [#61959](https://github.com/StarRocks/starrocks/pull/61959)
- Optimized DROP PARTITION information in logs.  [#61787](https://github.com/StarRocks/starrocks/pull/61787)
- Assigned a large but configurable row count to tables with unknown stats for statistical estimation.  [#61332](https://github.com/StarRocks/starrocks/pull/61332)
- Added balance statistic according to label location.  [#61905](https://github.com/StarRocks/starrocks/pull/61905)
- Added colocate group balance statistics to improve cluster monitoring. [#61736](https://github.com/StarRocks/starrocks/pull/61736)
- Skipped the Publish waiting phase when the number of healthy replicas exceeds the default replica count. [#61820](https://github.com/StarRocks/starrocks/pull/61820)
- Included the tablet information collection time in the tablet report. [#61643](https://github.com/StarRocks/starrocks/pull/61643)
- Supports writing Starlet files with tags. [ #61605](https://github.com/StarRocks/starrocks/pull/61605)
- Supports viewing cluster balance statistics via SHOW PROC.  [#61578](https://github.com/StarRocks/starrocks/pull/61578)
- Bumped librdkafka to 2.11.0 to support Kafka 4.0 and removed deprecated configurations.  [#61698](https://github.com/StarRocks/starrocks/pull/61698)
- Added `prepared_timeout` configuration to Stream Load Transaction Interface.  [#61539](https://github.com/StarRocks/starrocks/pull/61539)
- Upgraded StarOS to v3.5‑rc3.  [#61685](https://github.com/StarRocks/starrocks/pull/61685)

### Bug Fixes

The following issues have been fixed:

- Incorrect Dict version of random distribution tables. [#61933](https://github.com/StarRocks/starrocks/pull/61933)
- Incorrect query context in context conditions. [#61929](https://github.com/StarRocks/starrocks/pull/61929)
- Publish failures caused by synchronous Publish for shadow tablets during ALTER operations. [#61887](https://github.com/StarRocks/starrocks/pull/61887)
- CVE‑2025‑55163 issue.  [#62041](https://github.com/StarRocks/starrocks/pull/62041)
- Memory leak in real-time data ingestion from Apache Kafka.  [#61698](https://github.com/StarRocks/starrocks/pull/61698)
- Incorrect count of rebuild files in the lake persistent index. [#61859](https://github.com/StarRocks/starrocks/pull/61859)
- Statistics collection on generated expression columns causes cross-database query errors. [#61829](https://github.com/StarRocks/starrocks/pull/61829)
- Query Cache misaligns in shared-nothing clusters, causing inconsistent results. [#61783](https://github.com/StarRocks/starrocks/pull/61783)
- High memory usage in CatalogRecycleBin due to retaining deleted partition information.[#61582](https://github.com/StarRocks/starrocks/pull/61582)
- SQL Server JDBC connections fail when the timeout exceeds 65,535 milliseconds. [#61719](https://github.com/StarRocks/starrocks/pull/61719)
- Security Integration fails to encrypt passwords, exposing sensitive information. [#60666](https://github.com/StarRocks/starrocks/pull/60666)
- `MIN()` and `MAX()` functions on Iceberg partition columns return NULL unexpectedly.  [#61858](https://github.com/StarRocks/starrocks/pull/61858)
- Other predicates of Join containing non‑push‑down subfields were incorrectly rewritten.  [#61868](https://github.com/StarRocks/starrocks/pull/61868)
- QueryContext cancellation can lead to a use‑after‑free situation.  [#61897](https://github.com/StarRocks/starrocks/pull/61897)
- CBO’s table pruning overlooks other predicates.  [#61881](https://github.com/StarRocks/starrocks/pull/61881)
- Partial Updates in `COLUMN_UPSERT_MODE` may overwrite auto-increment columns with zero.  [#61341](https://github.com/StarRocks/starrocks/pull/61341)
- JDBC TIME type conversion uses an incorrect timezone offset that leads to wrong time values. [#61783](https://github.com/StarRocks/starrocks/pull/61783)
- `max_filter_ratio` was not being serialized in Routine Load jobs. [#61755](https://github.com/StarRocks/starrocks/pull/61755)
- Precision loss in the `now(precision)` function in Stream Load. [#61721](https://github.com/StarRocks/starrocks/pull/61721)
- Cancelling a query may result in a “query id not found” error. [#61667](https://github.com/StarRocks/starrocks/pull/61667)
- LDAP authentication may miss PartialResultException, causing incomplete query results.[ #60667](https://github.com/StarRocks/starrocks/pull/60667)
- Paimon Timestamp timezone conversion issue when the query condition contains DATETIME.[ #60473](https://github.com/StarRocks/starrocks/pull/60473)

## 3.5.3

Release Date: August 11, 2025

### Improvements

- Lake Compaction adds Segment write time statistics. [#60891](https://github.com/StarRocks/starrocks/pull/60891)
- Disable inline mode for Data Cache writes to avoid performance degradation. [#60530](https://github.com/StarRocks/starrocks/pull/60530)
- Iceberg metadata scan supports shared file I/O. [#61012](https://github.com/StarRocks/starrocks/pull/61012)
- Support termination of all PENDING ANALYZE tasks. [#61118](https://github.com/StarRocks/starrocks/pull/61118)
- Force reuse when there are too many CTE nodes to avoid excessive optimization time. [#60983](https://github.com/StarRocks/starrocks/pull/60983)
- Added `BALANCE` type to cluster balance results. [#61081](https://github.com/StarRocks/starrocks/pull/61081)
- Optimized materialized view rewrite for external tables. [#61037](https://github.com/StarRocks/starrocks/pull/61037)
- Default value of system variable `enable_materialized_view_agg_pushdown_rewrite` is changed to `true`, enabling aggregation pushdown for materialized view queries by default. [#60976](https://github.com/StarRocks/starrocks/pull/60976)
- Optimized partition statistics lock competition. [#61041](https://github.com/StarRocks/starrocks/pull/61041)

### Bug Fixes

The following issues have been fixed:

- Inconsistent Chunk column size after column pruning. [#61271](https://github.com/StarRocks/starrocks/pull/61271)
- Synchronous execution of partition statistics loading may cause deadlocks. [#61300](https://github.com/StarRocks/starrocks/pull/61300)
- Crash when `array_map` processes constant array columns. [#61309](https://github.com/StarRocks/starrocks/pull/61309)
- Setting an auto-increment column to NULL results in the system mistakenly rejecting valid data within the same Chunk. [#61255](https://github.com/StarRocks/starrocks/pull/61255)
- The actual number of JDBC connections may exceed the `jdbc_connection_pool_size` limit. [#61038](https://github.com/StarRocks/starrocks/pull/61038)
- FQDN mode did not use IP addresses as cache map keys. [#61203](https://github.com/StarRocks/starrocks/pull/61203)
- Array column cloning error during array comparison. [#61036](https://github.com/StarRocks/starrocks/pull/61036)
- Deploying serialized thread pool blockage led to query performance degradation. [#61150](https://github.com/StarRocks/starrocks/pull/61150)
- OK hbResponse not synchronized after heartbeat retry counter reset. [#61249](https://github.com/StarRocks/starrocks/pull/61249)
- Incorrect result for the `hour_from_unixtime` function. [#61206](https://github.com/StarRocks/starrocks/pull/61206)
- Conflicts between ALTER TABLE jobs and partition creation. [#60890](https://github.com/StarRocks/starrocks/pull/60890)
- Cache does not take effect after upgrading from v3.3 to v3.4 or later. [#60973](https://github.com/StarRocks/starrocks/pull/60973)
- Vector index metric `hit_count` is not set. [#61102](https://github.com/StarRocks/starrocks/pull/61102)
- Stream Load transactions fail to find the coordinator node. [#60154](https://github.com/StarRocks/starrocks/pull/60154)
- BE crashes when loading OOM partitions. [#60778](https://github.com/StarRocks/starrocks/pull/60778)
- INSERT OVERWRITE failed on manually created partitions. [#60750](https://github.com/StarRocks/starrocks/pull/60750)
- Partition creation failed when partition names matched case-insensitively but had different values. [#60909](https://github.com/StarRocks/starrocks/pull/60909)
- The system does not support PostgreSQL UUID type. [#61021](https://github.com/StarRocks/starrocks/pull/61021)
- Case sensitivity issue with column names when loading Parquet data via `FILES()`. [#61059](https://github.com/StarRocks/starrocks/pull/61059)

## 3.5.2

Release Date: July 18, 2025

### Improvements

- Collected NDV (number of distinct values) statistics for ARRAY columns to improve query plan accuracy. [#60623](https://github.com/StarRocks/starrocks/pull/60623)
- Disabled replica balancing for Colocate tables and tablet scheduling in Shared-data clusters to reduce unnecessary log output. [#60737](https://github.com/StarRocks/starrocks/pull/60737)
- Optimized Catalog access workflow: FE now delays accessing external data sources asynchronously at startup to prevent hanging due to external service unavailability. [#60614](https://github.com/StarRocks/starrocks/pull/60614)
- Added session variable `enable_predicate_expr_reuse` to control predicate pushdown. [#60603](https://github.com/StarRocks/starrocks/pull/60603)
- Supports a retry mechanism when fetching Kafka partition information fails. [#60513](https://github.com/StarRocks/starrocks/pull/60513)
- Removed the restriction requiring exact mapping of partition columns between materialized views and base tables. [#60565](https://github.com/StarRocks/starrocks/pull/60565)
- Supports building Runtime In-Filters to enhance aggregation performance by filtering data during aggregation. [#59288](https://github.com/StarRocks/starrocks/pull/59288)

### Bug Fixes

Fixed the following issues:

- COUNT DISTINCT queries crash due to low-cardinality optimization for multiple columns. [#60664](https://github.com/StarRocks/starrocks/pull/60664)
- Incorrect matching of global UDFs when multiple functions share the same name. [#60550](https://github.com/StarRocks/starrocks/pull/60550)
- Null pointer exception (NPE) issue during Stream Load import. [#60755](https://github.com/StarRocks/starrocks/pull/60755)
- Null pointer exception (NPE) issue when starting FE during a recovery from a cluster snapshot. [#60604](https://github.com/StarRocks/starrocks/pull/60604)
- BE crash caused by column mode mismatch when processing short-circuit queries with out-of-order values. [#60466](https://github.com/StarRocks/starrocks/pull/60466)
- Session variables set via PROPERTIES in SUBMIT TASK statements did not take effect. [#60584](https://github.com/StarRocks/starrocks/pull/60584)
- Incorrect results for `SELECT min/max` queries under specific conditions. [#60601](https://github.com/StarRocks/starrocks/pull/60601)
- Incorrect bucket pruning when the left side of a predicate is a function, leading to incorrect query results. [#60467](https://github.com/StarRocks/starrocks/pull/60467)
- Crash for queries against a non-existent `query_id` via Arrow Flight SQL. [#60497](https://github.com/StarRocks/starrocks/pull/60497)

### Behavior Changes

- The default value of `lake_compaction_allow_partial_success` is set to `true`. Compaction operations can now be marked as successful even if partially completed, preventing blockage of subsequent compaction tasks. [#60643](https://github.com/StarRocks/starrocks/pull/60643)

## 3.5.1

Release Date: July 1, 2025

### New Features

- [Experimental] Starting from v3.5.1, StarRocks introduces a high-performance data transfer channel based on the Apache Arrow Flight SQL protocol, comprehensively optimizing the data import channel and significantly improving transfer efficiency. This solution establishes a fully columnar data transfer pipeline from the StarRocks columnar execution engine to the client, eliminating the frequent row-column conversions and serialization overhead typically seen in traditional JDBC and ODBC interfaces, and achieving true zero-copy, low-latency, and high-throughput data transfer capabilities. [#57956](https://github.com/StarRocks/starrocks/pull/57956)
- Java Scalar UDFs (user-defined functions) now support ARRAY and MAP types as input parameters. [#55356](https://github.com/StarRocks/starrocks/pull/55356)
- **Cross-node data cache sharing**: Enables nodes to share cached external table data of data lakes across compute nodes via the network. If a local cache miss occurs, the system first attempts to fetch data from the caches of other nodes within the same cluster. Only if all caches miss will it re-fetch data from remote storage. This feature effectively reduces performance jitter caused by cache invalidation during elastic scaling and ensures stable query performance. A new FE configuration parameter `enable_trace_historical_node` controls this behavior (Default: `false`). [#57083](https://github.com/StarRocks/starrocks/pull/57083)
- **Storage Volume adds native support for Google Cloud Storage (GCS)**: You can now use GCS as a backend storage volume and manage and access GCS resources through the native SDK. [#58815](https://github.com/StarRocks/starrocks/pull/58815)

### Improvements

- Optimized error messages when creating Hive external tables fails. [#60076](https://github.com/StarRocks/starrocks/pull/60076)
- Optimized `count(1)` query performance using the `file_record_count` in Iceberg metadata. [#60022](https://github.com/StarRocks/starrocks/pull/60022)
- Refined the Compaction scheduling logic to avoid delayed scheduling when all subtasks succeed. [#59998](https://github.com/StarRocks/starrocks/pull/59998)
- Added `JAVA_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED"` to BE and CN after upgrading to JDK 17. [#59947](https://github.com/StarRocks/starrocks/pull/59947)
- Supports modifying the `kafka_broker_list` property via the ALTER ROUTINE LOAD command when Kafka Broker endpoints change. [#59787](https://github.com/StarRocks/starrocks/pull/59787)
- Supports reducing build dependencies of the Docker base image through parameters. [#59772](https://github.com/StarRocks/starrocks/pull/59772)
- Supports accessing Azure using Managed Identity authentication. [#59657](https://github.com/StarRocks/starrocks/pull/59657)
- Improved error messages when querying external data via `Files()` function with duplicate path column names. [#59597](https://github.com/StarRocks/starrocks/pull/59597)
- Optimized LIMIT pushdown logic. [#59265](https://github.com/StarRocks/starrocks/pull/59265)

### Bug Fixes

Fixed the following issues:

- Partition pruning issue when queries include Max and Min aggregations and empty partitions. [#60162](https://github.com/StarRocks/starrocks/pull/60162)
- Incorrect query results when rewriting queries with materialized views due to missing NULL partitions. [#60087](https://github.com/StarRocks/starrocks/pull/60087)
- Refresh errors on Iceberg external tables when using partition expressions based on `str2date`. [#60089](https://github.com/StarRocks/starrocks/pull/60089)
- Incorrect partition range when creating temporary partitions using the START END syntax. [#60014](https://github.com/StarRocks/starrocks/pull/60014)
- Incorrect display of Routine Load metrics on non-leader FE nodes. [#59985](https://github.com/StarRocks/starrocks/pull/59985)
- BE/CN crashes when executing queries containing `COUNT(*)` window functions. [#60003](https://github.com/StarRocks/starrocks/pull/60003)
- Stream Load failures when the target table name contains Chinese characters. [#59722](https://github.com/StarRocks/starrocks/pull/59722)
- Overall loading failures to triple-replica tables when loading to a secondary replica fails. [#59762](https://github.com/StarRocks/starrocks/pull/59762)
- Missing parameters in SHOW CREATE VIEW output. [#59714](https://github.com/StarRocks/starrocks/pull/59714)

### Behavior Changes

- Some FE metrics include the `is_leader` label. [#59883](https://github.com/StarRocks/starrocks/pull/59883)

## 3.5.0

Release Date: June 13, 2025

### Shared-data Enhancement

- Shared-data clusters support generated columns. [#53526](https://github.com/StarRocks/starrocks/pull/53526)
- Cloud-native Primary Key tables in shared-data clusters support rebuilding specific indexes. The performance of the indexes is also optimized. [#53971](https://github.com/StarRocks/starrocks/pull/53971) [#54178](https://github.com/StarRocks/starrocks/pull/54178)
- Optimized the execution logic of large-scale data loading operations to avoid generating too many small files in Rowset due to memory limitations. During the import, the system will merge the temporary data blocks to reduce the generation of small files, which improves the query performance after the import and also reduces the subsequent Compaction operations to improve the system resource utilization. [#53954](https://github.com/StarRocks/starrocks/issues/53954) 

### Data Lake Analytics

- **[Beta]** Supports creating Iceberg views in the Iceberg Catalog with Hive Metastore integration. And supports adding or modifying the dialect of the Iceberg view using the ALTER VIEW statement for better syntax compatibility with external systems. [#56120](https://github.com/StarRocks/starrocks/pull/56120)
- Supports nested namespace for [Iceberg REST Catalog](https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/#rest). [#58016](https://github.com/StarRocks/starrocks/pull/58016)
- Supports using `IcebergAwsClientFactory` to create AWS clients in [Iceberg REST Catalog](https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/#rest) to offer vended credentials. [#58296](https://github.com/StarRocks/starrocks/pull/58296)
- Parquet Reader supports filtering data with Bloom Filter. [#56445](https://github.com/StarRocks/starrocks/pull/56445)
- Supports automatically creating global dictionaries for low-cardinality columns in Parquet-formatted Hive/Iceberg tables during queries. [#55167](https://github.com/StarRocks/starrocks/pull/55167) 

### Performance Improvement and Query Optimization

- Statistics optimization:
  - Supports Table Sample. Improved statistics accuracy and query performance by sampling data blocks in physical files. [#52787](https://github.com/StarRocks/starrocks/issues/52787)
  - Supports [recording the predicate columns in queries](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/#predicate-column) for targeted statistics collection. [#53204](https://github.com/StarRocks/starrocks/issues/53204)
  - Supports partition-level cardinality estimation. The system reuses the system-defined view `_statistics_.column_statistics` to record the NDV of each partition. [#51513](https://github.com/StarRocks/starrocks/pull/51513)
  - Supports [multi-column Joint NDV collection](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/#multi-column-joint-statistics) to optimize the query plan generated by CBO in the scenario where columns correlate with each other.  [#56481](https://github.com/StarRocks/starrocks/pull/56481) [#56715](https://github.com/StarRocks/starrocks/pull/56715) [#56766](https://github.com/StarRocks/starrocks/pull/56766) [#56836](https://github.com/StarRocks/starrocks/pull/56836)
  - Supports using histograms to estimate the Join node cardinality and in_predicate selectivity, thus improving the estimation accuracy in data skew. [#57874](https://github.com/StarRocks/starrocks/pull/57874) [#57639](https://github.com/StarRocks/starrocks/pull/57639)
  - Optimized [Query Feedback](https://docs.starrocks.io/docs/using_starrocks/query_feedback/). Queries with the identical structure but different parameter values will be categorized as the same type and share the same tuning guide for plan execution optimization. [#58306](https://github.com/StarRocks/starrocks/pull/58306)
- Supports Runtime Bitset Filter as an alternative for optimization to Bloom Filter in specific scenarios. [#57157](https://github.com/StarRocks/starrocks/pull/57157)
- Supports pushing down Join Runtime Filter to the storage layer. [#55124](https://github.com/StarRocks/starrocks/pull/55124)
- Supports Pipeline Event Scheduler. [#54259](https://github.com/StarRocks/starrocks/pull/54259)

### Partition Management

- Supports using ALTER TABLE to [merge expression partitions based on time functions](https://docs.starrocks.io/docs/table_design/data_distribution/expression_partitioning/#merge-expression-partitions) for optimized storage efficiency and query performance. [#56840](https://github.com/StarRocks/starrocks/pull/56840)
- Supports partition Time-to-live (TTL) for List-partitioned tables and materialized views. And supports the property `partition_retention_condition` in tables and materialized views to allow users to set data retention strategies for list partitions, thus achieving more flexible partition deletion strategies. [#53117](https://github.com/StarRocks/starrocks/issues/53117)
- Supports using ALTER TABLE to [delete partitions specified by common partition expressions](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/#drop-partitions), allowing users to flexibly delete partitions in batches. [#53118](https://github.com/StarRocks/starrocks/pull/53118)

### Cluster Management

- Upgraded FE compile target from Java 11 to Java 17 for better system stability and performance. [#53617](https://github.com/StarRocks/starrocks/pull/53617)  [#57030](https://github.com/StarRocks/starrocks/pull/57030)

### Security and Authentication

- Supports secure [connections encrypted by SSL](https://docs.starrocks.io/zh/docs/administration/user_privs/ssl_authentication/) based on the MySQL protocol. [#54877](https://github.com/StarRocks/starrocks/pull/54877)
- Enhanced authentication using external systems:
  - Supports creating StarRocks users with [OAuth 2.0](https://docs.starrocks.io/docs/administration/user_privs/authentication/oauth2_authentication/) and [JSON Web Token (JWT)](https://docs.starrocks.io/docs/administration/user_privs/authentication/jwt_authentication/).
  - Supports [Security Integration](https://docs.starrocks.io/docs/administration/user_privs/authentication/security_integration/) to simplify the authentication process with external systems. Security Integration supports LDAP, OAuth 2.0, and JWT. [#55846](https://github.com/StarRocks/starrocks/pull/55846)
- Supports [Group Provider](https://docs.starrocks.io/docs/administration/user_privs/group_provider/) to obtain the user group information from external authentication services. The group information can then be used in authentication and authorization. Group Provider supports acquiring group information from LDAP, operating systems, or files. Users can query the user group they belong to using the function `current_group()`. [#56670](https://github.com/StarRocks/starrocks/pull/56670) 

### Materialized Views

- Supports creating materialized views with multiple partition columns to allow users to partition the data with a more flexible strategy. [#52576](https://github.com/StarRocks/starrocks/issues/52576)
- Supports setting `query_rewrite_consistency` to `force_mv` to force the system to use the materialized view for query rewrite, thus keeping performance stability at the cost of data timeliness to a certain extent. [#53819](https://github.com/StarRocks/starrocks/pull/53819)

### Loading and Unloading

- Supports pausing Routine Load jobs on JSON parse errors by setting the property `pause_on_json_parse_error` to `true`. [#56062](https://github.com/StarRocks/starrocks/pull/56062)
- **[Beta]** Supports [transactions with multiple SQL statements](https://docs.starrocks.io/docs/loading/SQL_transaction/) (currently, only INSERT is supported). Users can start, apply, or undo a transaction to guarantee the ACID (atomicity, consistency, isolation, and durability) properties of multiple loading operations. [#53978](https://github.com/StarRocks/starrocks/issues/53978)

### Functions

- Introduced the system variable `lower_upper_support_utf8` on the session and global level, enhancing the support for UTF-8 strings (especially non-ASCII characters) in case conversion functions such as `upper()` and `lower()`. [#56192](https://github.com/StarRocks/starrocks/pull/56192)
- Added new functions:
  - [`field()`](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/field/) [#5533](https://github.com/StarRocks/starrocks/pull/55331)
  - [`ds_theta_count_distinct()`](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/ds_theta_count_distinct/) [#56960](https://github.com/StarRocks/starrocks/pull/56960)
  - [`array_flatten()`](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_flatten/) [#50080](https://github.com/StarRocks/starrocks/pull/50080)
  - [`inet_aton()`](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/inet_aton/) [#51883](https://github.com/StarRocks/starrocks/pull/51883)
  - [`percentile_approx_weight()`](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/percentile_approx_weight/) [#57410](https://github.com/StarRocks/starrocks/pull/57410)
