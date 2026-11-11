// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.AsyncIterable;
import com.starrocks.connector.iceberg.CachingIcebergCatalog.IcebergTableName;
import com.starrocks.connector.iceberg.DataFileWrapper;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.StarRocksIcebergTableScanContext;
import com.starrocks.connector.iceberg.cost.IcebergMetricsReporter;
import com.starrocks.connector.metadata.MetadataCollectJob;
import com.starrocks.connector.metadata.iceberg.IcebergMetadataCollectJob;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TResultSinkType;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.executeInNewThread;
import static com.starrocks.connector.iceberg.IcebergApiConverter.mayHaveEqualityDeletes;

public class StarRocksIcebergTableScan
        extends DataScan<TableScan, FileScanTask, CombinedScanTask> implements TableScan {
    private static final Logger LOG = LogManager.getLogger(StarRocksIcebergTableScan.class);

    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final PlanMode planMode;
    private final Cache<String, Set<DataFile>> dataFileCache;
    private final Cache<String, Set<DeleteFile>> deleteFileCache;
    private final DeleteFileIndexCache deleteFileIndexCache;
    private final Map<IcebergTableName, Set<String>> metaFileCacheMap;
    private final Map<Integer, PartitionSpec> scanSpecsById;
    private final Map<Integer, String> specStringCache;
    private final Map<Integer, ResidualEvaluator> residualCache;
    private final Map<Integer, Evaluator> partitionEvaluatorCache;
    private final Map<Integer, InclusiveMetricsEvaluator> inclusiveMetricsEvaluatorCache;
    private final String schemaString;
    private DeleteFileIndex deleteFileIndex;
    private boolean dataFileCacheWithMetrics;
    // Set only when the catalog caches column statistics; identifier-only caching keeps per-file copies.
    private final int cacheStatsColumnLimit;
    private boolean enableCacheDataFileIdentifierColumnMetrics;
    private final StarRocksIcebergTableScanContext scanContext;
    private final boolean onlyReadCache;
    private final int localParallelism;
    private final long localPlanningMaxSlotSize;
    private ConnectContext connectContext;
    private final IcebergTableName icebergTableName;
    private IcebergMetricsReporter metricsReporter;
    private static final String READ_SPLIT_TARGET_SIZE = "read.split.target-size";

    public static TableScanContext newTableScanContext(Table table, StarRocksIcebergTableScanContext scanContext) {
        if (table instanceof BaseTable) {
            MetricsReporter reporter = ((BaseTable) table).reporter();
            ImmutableTableScanContext.Builder builder = ImmutableTableScanContext.builder();
            builder = builder.metricsReporter(reporter);
            if (scanContext.getFileSplitSize() > 0) {
                builder.putOptions(READ_SPLIT_TARGET_SIZE, String.valueOf(scanContext.getFileSplitSize()));
            }
            return builder.build();
        } else {
            return TableScanContext.empty();
        }
    }

    public StarRocksIcebergTableScan(Table table,
                                     Schema schema,
                                     TableScanContext context,
                                     StarRocksIcebergTableScanContext scanContext) {
        super(table, schema, context);
        this.catalogName = scanContext.getCatalogName();
        this.dbName = scanContext.getDbName();
        this.tableName = scanContext.getTableName();
        this.icebergTableName = new IcebergTableName(dbName, tableName);
        this.planMode = scanContext.getPlanMode();
        this.connectContext = scanContext.getConnectContext();
        this.scanContext = scanContext;
        // The scan schema is the snapshot schema for time travel (DataScan#useSnapshotSchema), but
        // partition specs stay bound to the current schema. Rebind them so evaluators bind against
        // the same schema as the filter; spec strings sent to scan tasks keep the original specs.
        this.scanSpecsById = specsForScanSchema(table.specs());
        this.specStringCache = specCache(table.specs(), PartitionSpecParser::toJson);
        this.residualCache = specCache(scanSpecsById, this::newResidualEvaluator);
        this.partitionEvaluatorCache = specCache(scanSpecsById, this::newPartitionEvaluator);
        this.inclusiveMetricsEvaluatorCache = specCache(scanSpecsById, this::newInclusiveMetricsEvaluator);
        this.schemaString = SchemaParser.toJson(tableSchema());
        this.dataFileCache = scanContext.getDataFileCache();
        this.deleteFileCache = scanContext.getDeleteFileCache();
        this.deleteFileIndexCache = scanContext.getDeleteFileIndexCache();
        this.metaFileCacheMap = scanContext.getMetaFileCacheMap();
        this.dataFileCacheWithMetrics = scanContext.isDataFileCacheWithMetrics();
        // without cached column statistics, only the identifier columns may be cached (equality deletes), so no
        // column beyond the keep-set
        this.cacheStatsColumnLimit =
                dataFileCacheWithMetrics ? Math.max(scanContext.getDataFileCacheStatsColumnLimit(), 0) : 0;
        this.enableCacheDataFileIdentifierColumnMetrics = scanContext.isEnableCacheDataFileIdentifierColumnMetrics();
        this.onlyReadCache = scanContext.isOnlyReadCache();
        this.localParallelism = scanContext.getLocalParallelism();
        this.localPlanningMaxSlotSize = scanContext.getLocalPlanningMaxSlotSize();
    }

    @Override
    protected TableScan newRefinedScan(Table newTable, Schema newSchema, TableScanContext newContext) {
        StarRocksIcebergTableScan scan = new StarRocksIcebergTableScan(newTable, newSchema, newContext, scanContext);
        scan.metricsReporter = this.metricsReporter;
        return scan;
    }

    @Override
    public TableScan useSnapshot(long scanSnapshotId) {
        // Delegate to the parent (inheriting its validation and context assembly), which binds the scan
        // to the target snapshot's recorded schema (DataScan#useSnapshotSchema). When a read schema was
        // resolved for this query, rebind to it so the scan uses the same schema the query bound its
        // predicates and descriptor against: the current table schema for an ordinary read (making a
        // metadata-only ADD COLUMN visible), or the targeted snapshot's schema for a time-travel read.
        TableScan scan = super.useSnapshot(scanSnapshotId);
        Schema readSchema = scanContext.getReadSchema();
        if (readSchema != null && useSnapshotSchema()) {
            return newRefinedScan(table(), readSchema, ((StarRocksIcebergTableScan) scan).context());
        }
        return scan;
    }

    @Override
    protected ManifestGroup newManifestGroup(List<ManifestFile> dataManifests,
                                             List<ManifestFile> deleteManifests,
                                             boolean withColumnStats) {
        // Use the specs rebound to the scan schema so that the manifest evaluators bind the
        // filter consistently for time-travel reads.
        return super.newManifestGroup(dataManifests, deleteManifests, withColumnStats)
                .specsById(scanSpecsById);
    }

    @Override
    public TableScan metricsReporter(MetricsReporter reporter) {
        if (reporter instanceof IcebergMetricsReporter) {
            this.metricsReporter = (IcebergMetricsReporter) reporter;
        }
        TableScan scan = super.metricsReporter(reporter);
        return scan;
    }

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
        List<ManifestFile> dataManifests = findMatchingDataManifests(snapshot());
        List<ManifestFile> deleteManifests = findMatchingDeleteManifests(snapshot());

        boolean mayHaveEqualityDeletes = !deleteManifests.isEmpty() && mayHaveEqualityDeletes(snapshot());
        // optional statistics are served by the local (cached) planning when it runs, but never force it
        boolean loadColumnStats = mayHaveEqualityDeletes ||
                (shouldReturnColumnStats() && !scanContext.isOptionalColumnStats());

        if (shouldPlanLocally(dataManifests, loadColumnStats)) {
            return planFileTasksLocally(dataManifests, deleteManifests);
        } else {
            return planFileTasksRemotely(dataManifests, deleteManifests);
        }
    }

    private CloseableIterable<FileScanTask> planFileTasksRemotely(
            List<ManifestFile> dataManifests, List<ManifestFile> deleteManifests) {
        LOG.info("Planning file tasks remotely for table {}.{}", dbName, tableName);

        String name = "ICEBERG.REMOTE_PLAN." + dbName + "." + tableName + "[" + filter() + "]";
        Tracers.record(Tracers.Module.EXTERNAL, name, "true");

        long liveFilesCount = liveFilesCount(dataManifests);
        scanMetrics().scannedDataManifests().increment(dataManifests.size());

        String icebergSerializedPredicate = filter() == Expressions.alwaysTrue() ? "" :
                SerializationUtil.serializeToBase64(filter());
        this.deleteFileIndex = planDeletesLocally(deleteManifests, Sets.newHashSet());

        MetadataCollectJob metadataCollectJob = new IcebergMetadataCollectJob(
                catalogName, dbName, tableName, TResultSinkType.METADATA_ICEBERG, snapshotId(), icebergSerializedPredicate);

        metadataCollectJob.init(connectContext.getSessionVariable());

        long currentTimestamp = System.currentTimeMillis();
        String threadNamePrefix = String.format("%s-%s-%s-%d", catalogName, dbName, tableName, currentTimestamp);
        executeInNewThread(threadNamePrefix + "-fetch_result", metadataCollectJob::asyncCollectMetadata);

        MetadataParser parser = new MetadataParser(
                table(), schemaString, specStringCache, residualCache, planExecutor(), scanMetrics(),
                deleteFileIndex, metadataCollectJob, liveFilesCount);
        executeInNewThread(threadNamePrefix + "-parallel_parser", parser::parse);

        return new AsyncIterable<>(parser.getFileScanTaskQueue(), parser);
    }

    private DeleteFileIndex planDeletesLocally(List<ManifestFile> deleteManifests, Set<DeleteFile> cachedDeleteFiles) {
        DeleteFileIndex.Builder builder = DeleteFileIndex.builderFor(fileIO().get(), deleteManifests);
        if (cachedDeleteFiles != null && !cachedDeleteFiles.isEmpty()) {
            builder.cachedDeleteFiles(cachedDeleteFiles);
        }

        if (shouldPlanWithExecutor() && deleteManifests.size() > 1) {
            builder.planWith(planExecutor());
        }

        return builder
                .specsById(scanSpecsById)
                .schemasById(table().schemas())
                .filterData(filter())
                .caseSensitive(isCaseSensitive())
                .scanMetrics(scanMetrics())
                .deleteFileCache(deleteFileCache)
                .build();
    }

    private List<ManifestFile> findMatchingDataManifests(Snapshot snapshot) {
        List<ManifestFile> dataManifests = snapshot.dataManifests(fileIO().get());
        scanMetrics().totalDataManifests().increment(dataManifests.size());

        List<ManifestFile> matchingDataManifests =
                IcebergApiConverter.filterManifests(dataManifests, scanSpecsById, filter());
        int skippedDataManifestsCount = dataManifests.size() - matchingDataManifests.size();
        scanMetrics().skippedDataManifests().increment(skippedDataManifestsCount);

        return matchingDataManifests;
    }

    private List<ManifestFile> findMatchingDeleteManifests(Snapshot snapshot) {
        List<ManifestFile> deleteManifests = snapshot.deleteManifests(fileIO().get());
        List<ManifestFile> matchingDeleteManifests =
                IcebergApiConverter.filterManifests(deleteManifests, scanSpecsById, filter());

        scanMetrics().totalDeleteManifests().increment(deleteManifests.size());
        scanMetrics().skippedDeleteManifests().increment(deleteManifests.size() - matchingDeleteManifests.size());

        return matchingDeleteManifests;
    }

    private CloseableIterable<FileScanTask> planFileTasksLocally(
            List<ManifestFile> dataManifests, List<ManifestFile> deleteManifests) {
        if (useCache()) {
            planDeletesLocallyWithCache(deleteManifests);
            return planTaskWithCache(dataManifests);
        } else {
            ManifestGroup manifestGroup = newManifestGroup(dataManifests, deleteManifests);
            return manifestGroup.planFiles();
        }
    }

    private boolean useCache() {
        if (dataFileCache == null) {
            return false;
        }
        // A scan that returns column statistics reuses the manifest cache only when the cache keeps every
        // requested column; otherwise it reads the statistics fresh via the non-cache path.
        if (!shouldReturnColumnStats()) {
            return true;
        }
        boolean covered = cacheKeepsStatsColumns(columnsToKeepStats());
        Tracers.record(Tracers.Module.EXTERNAL, "ICEBERG.STATS_FROM_MANIFEST_CACHE." + dbName + "." + tableName,
                String.valueOf(covered));
        return covered;
    }

    /**
     * Whether this scan, which returns column statistics, will be served from the manifest cache, i.e. the cache
     * keeps the statistics of every requested column. Planning nothing, it lets a caller decide which statistics
     * to request before planning.
     */
    public boolean statsServedFromCache() {
        return dataFileCache != null && shouldReturnColumnStats() && cacheKeepsStatsColumns(columnsToKeepStats());
    }

    // Whether every manifest cache entry keeps the statistics of `columnIds`: the cache always keeps the keep-set
    // (statsKeepColumnIds) and fills the rest of the cacheStatsColumnLimit budget, which the keep-set columns with
    // statistics count against, with the lowest ranked other columns that have statistics in the manifest. So a
    // column ranked within the budget left after the whole keep-set can never be crowded out: it is kept in every
    // manifest where it has statistics. All statistics (null) are never kept. An entry cached under an older metrics
    // config may rank columns differently and lack one; min/max then skips that column for the file (slower, not
    // wrong).
    private boolean cacheKeepsStatsColumns(Set<Integer> columnIds) {
        if (columnIds == null || columnIds.isEmpty() || !scanContext.isDataFileCacheWithMetrics()) {
            return false;
        }
        return statsColumnIdsKeptByCache(table(), tableSchema(), cacheStatsColumnLimit, columnIds).size() ==
                columnIds.size();
    }

    // The columns of `columnIds` whose statistics every manifest cache entry keeps, for a cache that keeps the
    // statistics of `cacheStatsColumnLimit` columns (see cacheKeepsStatsColumns).
    public static Set<Integer> statsColumnIdsKeptByCache(Table table, Schema schema, int cacheStatsColumnLimit,
                                                         Set<Integer> columnIds) {
        Set<Integer> keepColumnIds = statsKeepColumnIds(table, schema);
        Set<Integer> topRankedColumnIds = null;
        Set<Integer> kept = new HashSet<>();
        for (Integer id : columnIds) {
            if (!keepColumnIds.contains(id)) {
                if (topRankedColumnIds == null) {
                    int budget = cacheStatsColumnLimit - keepColumnIds.size();
                    topRankedColumnIds = budget > 0 ?
                            topRankedStatsColumnIds(statsCandidateRanks(table), keepColumnIds, budget) : Set.of();
                }
                if (!topRankedColumnIds.contains(id)) {
                    continue;
                }
            }
            kept.add(id);
        }
        return kept;
    }

    // The `limit` lowest ranked columns outside the keep-set.
    static Set<Integer> topRankedStatsColumnIds(Map<Integer, Long> ranks, Set<Integer> keepColumnIds, int limit) {
        return ranks.entrySet().stream()
                .filter(e -> !keepColumnIds.contains(e.getKey()))
                .sorted(Map.Entry.comparingByValue())
                .limit(limit)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private void planDeletesLocallyWithCache(List<ManifestFile> deleteManifests) {
        if (reuseCachedDeleteFileIndex(deleteManifests)) {
            return;
        }

        List<ManifestFile> deleteManifestWithoutCache = new ArrayList<>();
        Set<DeleteFile> matchingCachedDeleteFiles = Sets.newHashSet();
        if (deleteFileCache != null) {
            for (ManifestFile manifestFile : deleteManifests) {
                Set<DeleteFile> deleteFiles = getCompleteCachedFiles(deleteFileCache, manifestFile);
                if (deleteFiles != null) {
                    scanMetrics().scannedDeleteManifests().increment();
                    int entrySize = deleteFiles.size();
                    if (filter() != null && filter() != Expressions.alwaysTrue()) {
                        deleteFiles = deleteFiles.stream()
                                .filter(f -> partitionEvaluatorCache.get(f.specId()).eval(f.partition()))
                                .filter(f -> inclusiveMetricsEvaluatorCache.get(f.specId()).eval(f))
                                .collect(Collectors.toSet());
                    }

                    scanMetrics().skippedDeleteFiles().increment(entrySize - deleteFiles.size());
                    if (deleteFiles.isEmpty()) {
                        continue;
                    }
                    matchingCachedDeleteFiles.addAll(deleteFiles);
                } else {
                    deleteFileCache.put(manifestFile.path(), ConcurrentHashMap.newKeySet());
                    metaFileCacheMap.computeIfAbsent(icebergTableName,
                            t -> ConcurrentHashMap.newKeySet()).add(manifestFile.path());
                    deleteManifestWithoutCache.add(manifestFile);
                }
            }
        } else {
            deleteManifestWithoutCache = deleteManifests;
        }

        this.deleteFileIndex = planDeletesLocally(deleteManifestWithoutCache, matchingCachedDeleteFiles);
    }

    // Position deletes and DVs reach a data file only through its path or its (spec, partition), and every
    // delete for a data file the filter keeps lives in a delete manifest the filter keeps. So an index built
    // without the scan filter returns the deletes a filtered one would for those data files (the filter has no
    // bounds of data columns to prune them with), and can be shared by every scan over the same delete
    // manifests. Equality deletes are excluded: the filter prunes them by column bounds, and a filter-free
    // index would hand the BE extra delete files.
    private boolean reuseCachedDeleteFileIndex(List<ManifestFile> deleteManifests) {
        if (deleteFileIndexCache == null || deleteFileCache == null || deleteManifests.isEmpty()
                || mayHaveEqualityDeletes(snapshot())) {
            return false;
        }

        DeleteFileIndexCache.Key key = new DeleteFileIndexCache.Key(icebergTableName, tableSchema().schemaId(),
                Set.copyOf(table().specs().keySet()),
                deleteManifests.stream().map(ManifestFile::path).collect(Collectors.toUnmodifiableSet()));
        DeleteFileIndex index = deleteFileIndexCache.get(key);
        if (index == null) {
            // Build only from complete cache entries, so the index shares the cached delete file objects.
            // Otherwise the regular path reads the missing manifests and fills the cache for the next scan.
            List<Set<DeleteFile>> deleteFiles = new ArrayList<>();
            for (ManifestFile manifestFile : deleteManifests) {
                Set<DeleteFile> files = getCompleteCachedFiles(deleteFileCache, manifestFile);
                if (files == null) {
                    return false;
                }
                deleteFiles.add(files);
            }

            index = DeleteFileIndex.builderFor(fileIO().get(), Collections.emptyList())
                    .cachedDeleteFiles(Iterables.concat(deleteFiles))
                    .specsById(scanSpecsById)
                    .schemasById(table().schemas())
                    .caseSensitive(isCaseSensitive())
                    .scanMetrics(scanMetrics())
                    .build();
            if (index.hasEqualityDeletes()) {
                return false;
            }
            // Sort every group now rather than on first lookup: the index is read by concurrent scans.
            index.referencedDeleteFiles();
            deleteFileIndexCache.put(key, index, deleteFiles);
        }

        scanMetrics().scannedDeleteManifests().increment(deleteManifests.size());
        this.deleteFileIndex = index;
        return true;
    }

    private CloseableIterable<FileScanTask> planTaskWithCache(List<ManifestFile> dataManifests) {
        List<Pair<ManifestFile, Set<DataFile>>> dataManifestWithCache = new ArrayList<>();
        List<ManifestFile> dataManifestWithoutCache = new ArrayList<>();
        for (ManifestFile manifestFile : dataManifests) {
            Set<DataFile> dataFiles = getCompleteCachedFiles(dataFileCache, manifestFile);
            if (dataFiles != null) {
                scanMetrics().scannedDataManifests().increment();
                if (!dataFiles.isEmpty()) {
                    dataManifestWithCache.add(new Pair(manifestFile, dataFiles));
                }
            } else {
                if (!onlyReadCache) {
                    dataFileCache.put(manifestFile.path(), ConcurrentHashMap.newKeySet());
                    metaFileCacheMap.computeIfAbsent(icebergTableName,
                            t -> ConcurrentHashMap.newKeySet()).add(manifestFile.path());
                }
                dataManifestWithoutCache.add(manifestFile);
            }
        }

        Iterable<CloseableIterable<FileScanTask>> tasks =
                CloseableIterable.transform(CloseableIterable.withNoopClose(dataManifestWithCache), this::filterDataFiles);

        CloseableIterable<FileScanTask> tasksWithCache = new ParallelIterable<>(tasks, planExecutor());
        if (dataManifestWithoutCache.isEmpty()) {
            return tasksWithCache;
        } else {
            CloseableIterable<FileScanTask> fileScanTaskWithoutCache =
                    planFileTasks(dataManifestWithoutCache, new ArrayList<>());
            return CloseableIterable.concat(Lists.newArrayList(fileScanTaskWithoutCache, tasksWithCache));
        }
    }

    private CloseableIterable<FileScanTask> filterDataFiles(Pair<ManifestFile, Set<DataFile>> manifestFile) {
        CloseableIterable<DataFile> matchedDataFiles = CloseableIterable.withNoopClose(manifestFile.second);

        if (filter() != Expressions.alwaysTrue()) {
            matchedDataFiles = CloseableIterable.filter(
                    scanMetrics().skippedDataFiles(),
                    CloseableIterable.withNoopClose(manifestFile.second),
                    file -> partitionEvaluatorCache.get(file.specId()).eval(file.partition()));
        }

        if (dataFileCacheWithMetrics ||
                (!tableSchema().identifierFieldIds().isEmpty() && enableCacheDataFileIdentifierColumnMetrics)) {
            matchedDataFiles = CloseableIterable.filter(
                    scanMetrics().skippedDataFiles(),
                    matchedDataFiles,
                    file -> inclusiveMetricsEvaluatorCache.get(file.specId()).eval(file));
        }

        return CloseableIterable.transform(matchedDataFiles, this::toFileScanTask);
    }

    private CloseableIterable<FileScanTask> planFileTasks(
            List<ManifestFile> dataManifests, List<ManifestFile> deleteManifests) {
        LOG.info("Planning file tasks locally for table {}", table().name());

        ManifestGroup manifestGroup =
                new ManifestGroup(fileIO().get(), dataManifests, deleteManifests)
                        .caseSensitive(isCaseSensitive())
                        .select(shouldReturnColumnStats() ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
                        .filterData(filter())
                        .specsById(scanSpecsById)
                        .schemasById(table().schemas())
                        .scanMetrics(scanMetrics())
                        .ignoreDeleted()
                        .withDataFileCache(dataFileCache)
                        .preparedDeleteFileIndex(deleteFileIndex)
                        .columnsToKeepStats(columnsToKeepStats())
                        .identifierFieldIds(getIdentifierFieldIds())
                        .cacheWithMetrics(dataFileCacheWithMetrics)
                        .cacheStatsColumnLimit(cacheStatsColumnLimit)
                        .cacheStatsCandidateRanks(cacheStatsColumnLimit > 0 ? statsCandidateRanks(table()) : null)
                        .cacheStatsTruncatedTypes(statsTruncatedTypes(table()));

        if (shouldIgnoreResiduals()) {
            manifestGroup = manifestGroup.ignoreResiduals();
        }

        if (shouldPlanWithExecutor() && (dataManifests.size() > 1 || deleteManifests.size() > 1)) {
            manifestGroup = manifestGroup.planWith(planExecutor());
        }

        return manifestGroup.planFiles();
    }

    private Set<Integer> getIdentifierFieldIds() {
        if (dataFileCache == null || deleteFileIndex == null) {
            return null;
        }

        if (dataFileCacheWithMetrics) {
            // The keep-set is always cached; other columns are cached too (ManifestColumnStats), up to the
            // column limit in total, so file-level min/max still prunes on columns that correlate with the
            // file layout without being partition or sort keys. Scans that need the stats of other columns
            // never reach here -- useCache() routes them to the non-cache path.
            Set<Integer> keepColumnIds = statsKeepColumnIds(table(), tableSchema());
            if (!keepColumnIds.isEmpty()) {
                return keepColumnIds;
            }
            return null;
        }

        if (deleteFileIndex.hasEqualityDeletes() && enableCacheDataFileIdentifierColumnMetrics &&
                !tableSchema().identifierFieldIds().isEmpty()) {
            this.dataFileCacheWithMetrics = true;
            return tableSchema().identifierFieldIds();
        }

        return null;
    }

    // Columns where file-level min/max actually prunes: partition source columns and sort-order
    // source columns -- unioned across all specs and sort orders so partition/sort evolution stays
    // covered -- plus identifier columns (equality-delete correctness).
    static Set<Integer> statsKeepColumnIds(Table table, Schema schema) {
        Set<Integer> ids = new HashSet<>(schema.identifierFieldIds());
        table.specs().values().forEach(spec -> spec.fields().forEach(field -> ids.add(field.sourceId())));
        table.sortOrders().values().forEach(order -> order.fields().forEach(field -> ids.add(field.sourceId())));
        return ids;
    }

    // Columns whose file-level min/max can prune, in every schema of the table, ranked for the cached statistics
    // column limit. Tiers by the table's current metrics config, which only orders the columns (files may have been
    // written under another config; what is kept still comes from the manifest): columns with an explicit
    // write.metadata.metrics.column.<col> mode that writes bounds, then the other columns whose mode writes bounds,
    // then the rest (counts or none, or no longer in the current schema). Inside a tier, top-level primitive columns
    // first, then primitive fields inside structs, each in field id order. Predicates cannot reference fields inside
    // lists or maps, so their statistics are not cached.
    static Map<Integer, Long> statsCandidateRanks(Table table) {
        Map<Integer, Long> ranks = new HashMap<>();
        table.schemas().values().forEach(schema -> collectPrunableFields(schema.asStruct(), true, ranks));
        Map<Integer, Long> tiers = statsMetricsTiers(table, ranks.keySet());
        if (tiers != null) {
            ranks.replaceAll((id, rank) -> (tiers.get(id) << 34) + rank);
        }
        return ranks;
    }

    private static final long TIER_EXPLICIT_BOUNDS = 0;
    private static final long TIER_BOUNDS = 1;
    private static final long TIER_OTHER = 2;

    // Null when the metrics config cannot be read: the columns then keep the untiered order.
    private static Map<Integer, Long> statsMetricsTiers(Table table, Set<Integer> fieldIds) {
        try {
            Schema schema = table.schema();
            Map<String, String> properties = table.properties();
            MetricsConfig config = MetricsConfig.forTable(table);
            Map<Integer, Long> tiers = new HashMap<>();
            for (Integer id : fieldIds) {
                String name = schema.findColumnName(id);
                long tier = TIER_OTHER;
                if (name != null) {
                    MetricsModes.MetricsMode mode = config.columnMode(name);
                    if (mode instanceof MetricsModes.Truncate || mode instanceof MetricsModes.Full) {
                        tier = properties.containsKey(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + name) ?
                                TIER_EXPLICIT_BOUNDS : TIER_BOUNDS;
                    }
                }
                tiers.put(id, tier);
            }
            return tiers;
        } catch (RuntimeException e) {
            LOG.warn("Failed to read the metrics config of table {}, cached statistics columns are not tiered by it",
                    table.name(), e);
            return null;
        }
    }

    // STRING and BINARY fields of every schema of the table: the cached statistics truncate their long bounds.
    static Map<Integer, Type.TypeID> statsTruncatedTypes(Table table) {
        Map<Integer, Type.TypeID> types = new HashMap<>();
        table.schemas().values().forEach(schema -> TypeUtil.indexById(schema.asStruct()).forEach((id, field) -> {
            Type.TypeID type = field.type().typeId();
            if (type == Type.TypeID.STRING || type == Type.TypeID.BINARY) {
                types.put(id, type);
            }
        }));
        return types;
    }

    private static void collectPrunableFields(Types.StructType struct, boolean topLevel, Map<Integer, Long> ranks) {
        for (Types.NestedField field : struct.fields()) {
            if (field.type().isPrimitiveType()) {
                long rank = (topLevel ? 0L : 1L << 32) + Integer.toUnsignedLong(field.fieldId());
                ranks.merge(field.fieldId(), rank, Math::min);
            } else if (field.type().isStructType()) {
                collectPrunableFields(field.type().asStructType(), false, ranks);
            }
        }
    }

    public void refreshDataFileCache(List<ManifestFile> manifestFiles) {
        manifestFiles.forEach(manifestFile -> {
            dataFileCache.put(manifestFile.path(), Sets.newHashSet());
            metaFileCacheMap.computeIfAbsent(icebergTableName,
                    t -> ConcurrentHashMap.newKeySet()).add(manifestFile.path());
        });
        this.deleteFileIndex = DeleteFileIndex.builderFor(new ArrayList<>()).build();

        try (CloseableIterable<FileScanTask> fileScanTaskIterable = planFileTasks(manifestFiles, new ArrayList<>());
                CloseableIterator<FileScanTask> fileScanTaskIterator = fileScanTaskIterable.iterator()) {
            while (fileScanTaskIterator.hasNext()) {
                fileScanTaskIterator.next();
            }
        } catch (IOException e) {
            LOG.error("Failed to refresh data file cache", e);
            throw new StarRocksConnectorException("Failed to refresh manifest cache", e);
        }
    }

    public Set<DeleteFile> getDeleteFiles(FileContent fileContent) {
        List<ManifestFile> deleteManifests = findMatchingDeleteManifests(snapshot());
        List<ManifestFile> deleteManifestWithoutCache = new ArrayList<>();
        Set<DeleteFile> matchingCachedDeleteFiles = Sets.newHashSet();
        if (deleteFileCache != null) {
            for (ManifestFile manifestFile : deleteManifests) {
                Set<DeleteFile> deleteFiles = getCompleteCachedFiles(deleteFileCache, manifestFile);
                if (deleteFiles != null) {
                    deleteFiles = deleteFiles.stream()
                            .filter(f -> f.content() == fileContent)
                            .collect(Collectors.toSet());
                    if (deleteFiles.isEmpty()) {
                        continue;
                    }

                    if (filter() != null && filter() != Expressions.alwaysTrue()) {
                        deleteFiles = deleteFiles.stream()
                                .filter(f -> partitionEvaluatorCache.get(f.specId()).eval(f.partition()))
                                .filter(f -> inclusiveMetricsEvaluatorCache.get(f.specId()).eval(f))
                                .collect(Collectors.toSet());
                    }

                    if (deleteFiles.isEmpty()) {
                        continue;
                    }

                    matchingCachedDeleteFiles.addAll(deleteFiles);
                } else {
                    deleteFileCache.put(manifestFile.path(), ConcurrentHashMap.newKeySet());
                    metaFileCacheMap.computeIfAbsent(icebergTableName,
                            t -> ConcurrentHashMap.newKeySet()).add(manifestFile.path());
                    deleteManifestWithoutCache.add(manifestFile);
                }
            }
        } else {
            deleteManifestWithoutCache = deleteManifests;
        }

        Set<DeleteFile> fetchedDeleteFiles = new HashSet<>();
        if (!deleteManifestWithoutCache.isEmpty()) {
            DeleteFileIndex.Builder builder = DeleteFileIndex.builderFor(fileIO().get(), deleteManifestWithoutCache);
            if (shouldPlanWithExecutor() && deleteManifests.size() > 1) {
                builder.planWith(planExecutor());
            }
            builder.specsById(scanSpecsById)
                    .schemasById(table().schemas())
                    .filterData(filter())
                    .caseSensitive(isCaseSensitive())
                    .scanMetrics(scanMetrics())
                    .deleteFileCache(deleteFileCache);

            fetchedDeleteFiles = builder.loadDeleteFiles().stream()
                    .filter(f -> f.content() == fileContent)
                    .collect(Collectors.toSet());
        }

        fetchedDeleteFiles.addAll(matchingCachedDeleteFiles);

        return fetchedDeleteFiles;
    }

    private FileScanTask toFileScanTask(DataFile dataFile) {
        String specString = specStringCache.get(dataFile.specId());
        ResidualEvaluator residuals = residualCache.get(dataFile.specId());

        DeleteFile[] deleteFiles = deleteFileIndex.forDataFile(dataFile);

        ScanMetricsUtil.fileTask(scanMetrics(), dataFile, deleteFiles);

        // Pruning and delete matching are done; the task keeps only the statistics the scan asked for, which
        // useCache() checked the cache keeps.
        DataFile taskFile = dataFile;
        if (dataFile instanceof DataFileWrapper) {
            DataFileWrapper wrapper = (DataFileWrapper) dataFile;
            taskFile = shouldReturnColumnStats() ? wrapper.withStats(columnsToKeepStats()) :
                    wrapper.withoutSharedStats();
        }
        return new BaseFileScanTask(
                taskFile,
                deleteFiles,
                schemaString,
                specString,
                residuals);
    }

    private boolean shouldPlanLocally(List<ManifestFile> manifests, boolean loadColumnStats) {
        return (planMode == PlanMode.AUTO && loadColumnStats) || shouldPlanLocally(manifests);
    }

    // TODO(stephen): add more strategies
    private boolean shouldPlanLocally(List<ManifestFile> manifests) {
        switch (planMode) {
            case LOCAL:
                return true;

            case DISTRIBUTED:
                return manifests.isEmpty();

            case AUTO:
                long localPlanningSizeThreshold = localParallelism * localPlanningMaxSlotSize;
                return remoteParallelism() <= localParallelism
                        || manifests.size() <= 2 * localParallelism
                        || totalSize(manifests) <= localPlanningSizeThreshold;

            default:
                throw new IllegalArgumentException("Unknown plan mode: " + planMode);
        }
    }

    private int remoteParallelism() {
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                .backendAndComputeNodeStream()
                .filter(ComputeNode::isAvailable)
                .mapToInt(ComputeNode::getCpuCores).sum();
    }

    private long totalSize(List<ManifestFile> manifests) {
        return manifests.stream().mapToLong(ManifestFile::length).sum();
    }

    private ResidualEvaluator newResidualEvaluator(PartitionSpec spec) {
        try {
            return ResidualEvaluator.of(spec, residualFilter(), isCaseSensitive());
        } catch (ValidationException e) {
            // The filter references a column this spec's schema doesn't have (the spec couldn't
            // be rebound to the scan schema). Keep the whole filter as the residual so the rows
            // are still filtered after scanning.
            return ResidualEvaluator.unpartitioned(residualFilter());
        }
    }

    private Evaluator newPartitionEvaluator(PartitionSpec spec) {
        try {
            Expression projected = Projections.inclusive(spec, false).project(filter());
            return new Evaluator(spec.partitionType(), projected, false);
        } catch (ValidationException e) {
            // Cannot evaluate the filter against this spec; skip partition pruning for it.
            return new Evaluator(spec.partitionType(), Expressions.alwaysTrue(), false);
        }
    }

    private InclusiveMetricsEvaluator newInclusiveMetricsEvaluator(PartitionSpec spec) {
        try {
            if (filter() != null) {
                return new InclusiveMetricsEvaluator(spec.schema(), filter(), false);
            } else {
                return new InclusiveMetricsEvaluator(spec.schema(), Expressions.alwaysTrue(), false);
            }
        } catch (ValidationException e) {
            // Cannot evaluate the filter against this spec's schema; skip metrics pruning for it.
            return new InclusiveMetricsEvaluator(spec.schema(), Expressions.alwaysTrue(), false);
        }
    }

    // Rebind the given specs to the scan schema, which is the snapshot schema for time-travel
    // reads. A spec referencing a column the scan schema doesn't have is kept as-is; the
    // evaluators above fall back to no pruning for it.
    private Map<Integer, PartitionSpec> specsForScanSchema(Map<Integer, PartitionSpec> specs) {
        Schema scanSchema = tableSchema();
        Map<Integer, PartitionSpec> result = new ConcurrentHashMap<>();
        specs.forEach((specId, spec) -> {
            PartitionSpec boundSpec = spec;
            if (spec.schema().schemaId() != scanSchema.schemaId()) {
                try {
                    boundSpec = spec.toUnbound().bind(scanSchema);
                } catch (RuntimeException e) {
                    boundSpec = spec;
                }
            }
            result.put(specId, boundSpec);
        });
        return result;
    }

    private <R> Map<Integer, R> specCache(Map<Integer, PartitionSpec> specs, Function<PartitionSpec, R> load) {
        Map<Integer, R> cache = new ConcurrentHashMap<>();
        specs.forEach((specId, spec) -> cache.put(specId, load.apply(spec)));
        return cache;
    }

    @Override
    public CloseableIterable<CombinedScanTask> planTasks() {
        return TableScanUtil.planTasks(
                planFiles(), targetSplitSize(), splitLookback(), splitOpenFileCost());
    }

    private int liveFilesCount(List<ManifestFile> manifests) {
        return manifests.stream().mapToInt(this::liveFilesCount).sum();
    }

    private int liveFilesCount(ManifestFile manifest) {
        return manifest.existingFilesCount() + manifest.addedFilesCount();
    }

    public IcebergTableName getIcebergTableName() {
        return icebergTableName;
    }

    public IcebergMetricsReporter getMetricsReporter() {
        return metricsReporter;
    }

    static Integer expectedLiveFilesCount(ManifestFile manifest) {
        Integer existingFilesCount = manifest.existingFilesCount();
        Integer addedFilesCount = manifest.addedFilesCount();
        if (existingFilesCount == null || addedFilesCount == null) {
            return null;
        }
        return existingFilesCount + addedFilesCount;
    }

    static boolean isCompleteCachedFiles(ManifestFile manifest, Set<?> files) {
        if (files == null) {
            return false;
        }

        Integer expectedLiveFilesCount = expectedLiveFilesCount(manifest);
        if (expectedLiveFilesCount == null) {
            // Without manifest counts, keep the previous "non-empty means usable" behavior,
            // but still reject empty placeholders so they don't look like valid cache hits.
            return !files.isEmpty();
        }

        return files.size() == expectedLiveFilesCount;
    }

    static <F> Set<F> getCompleteCachedFiles(Cache<String, Set<F>> cache, ManifestFile manifest) {
        Set<F> files = cache.getIfPresent(manifest.path());
        if (isCompleteCachedFiles(manifest, files)) {
            return files;
        }

        if (files != null) {
            if (!files.isEmpty()) {
                // A non-empty entry that fails the count check is a corrupted/incomplete cache write, not a
                // normal empty placeholder; log it so this failure path is no longer silent.
                LOG.warn("Dropping incomplete Iceberg manifest cache entry {}: cached {} files, manifest expects {}",
                        manifest.path(), files.size(), expectedLiveFilesCount(manifest));
            }
            cache.invalidate(manifest.path());
        }
        return null;
    }
}
