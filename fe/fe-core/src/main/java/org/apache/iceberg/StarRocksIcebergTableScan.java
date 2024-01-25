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

import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.AsyncIterable;
import com.starrocks.connector.iceberg.StarRocksIcebergTableScanContext;
import com.starrocks.connector.metadata.MetadataCollectJob;
import com.starrocks.connector.metadata.iceberg.IcebergMetadataCollectJob;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TResultSinkType;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.executeInNewThread;

public class StarRocksIcebergTableScan
        extends DataScan<TableScan, FileScanTask, CombinedScanTask> implements TableScan {
    private static final Logger LOG = LogManager.getLogger(StarRocksIcebergTableScan.class);

    private final Cache<String, Set<DataFile>> dataFileCache;
    private final Cache<String, Set<DeleteFile>> deleteFileCache;
    private final Map<Integer, String> specStringCache;
    private final Map<Integer, ResidualEvaluator> residualCache;
    private final Map<Integer, Evaluator> partitionEvaluatorCache;
    private final Map<Integer, InclusiveMetricsEvaluator> inclusiveMetricsEvaluatorCache;
    private final String schemaString;
    private DeleteFileIndex deleteFileIndex;
    private final boolean dataFileCacheWithMetrics;
    private final boolean forceEnableManifestCacheWithoutMetricsWithDeleteFile;
    private int localParallelism;
    private final long localPlanningMaxSlotSize;
    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final PlanMode planMode;
    private final StarRocksIcebergTableScanContext scanContext;
    private boolean loadColumnStats;
    private boolean onlyReadCache;
    private List<ManifestFile> toRefreshDataManifests = new ArrayList<>();

    public static TableScanContext newTableScanContext(Table table) {
        if (table instanceof BaseTable) {
            MetricsReporter reporter = ((BaseTable) table).reporter();
            return ImmutableTableScanContext.builder().metricsReporter(reporter).build();
        } else {
            return TableScanContext.empty();
        }
    }

    public StarRocksIcebergTableScan(Table table,
                                     Schema schema,
                                     TableScanContext context,
                                     StarRocksIcebergTableScanContext scanContext) {
        super(table, schema, context);
        this.scanContext = scanContext;
        this.planMode = scanContext.getPlanMode();
        this.localParallelism = scanContext.getLocalParallelism();
        this.catalogName = scanContext.getCatalogName();
        this.dbName = scanContext.getDbName();
        this.tableName = scanContext.getTableName();
        this.specStringCache = specCache(PartitionSpecParser::toJson);
        this.residualCache = specCache(this::newResidualEvaluator);
        this.partitionEvaluatorCache = specCache(this::newPartitionEvaluator);
        this.inclusiveMetricsEvaluatorCache = specCache(this::newInclusiveMetricsEvaluator);
        this.schemaString = SchemaParser.toJson(tableSchema());
        this.localParallelism = scanContext.getLocalParallelism();
        this.localPlanningMaxSlotSize = scanContext.getLocalPlanningMaxSlotSize();
        this.dataFileCache = scanContext.getDataFileCache();
        this.deleteFileCache = scanContext.getDeleteFileCache();
        this.dataFileCacheWithMetrics = scanContext.isDataFileCacheWithMetrics();
        this.forceEnableManifestCacheWithoutMetricsWithDeleteFile =
                scanContext.isForceEnableManifestCacheWithoutMetricsWithDeleteFile();
        this.onlyReadCache = scanContext.isOnlyReadCache();
    }

    @Override
    protected TableScan newRefinedScan(
            Table newTable, Schema newSchema, TableScanContext newContext) {
        return new StarRocksIcebergTableScan(newTable, newSchema, newContext, scanContext);
    }

    public StarRocksIcebergTableScan dataFileCache(Cache<String, Set<DataFile>> dataFileCache) {
        scanContext.setDataFileCache(dataFileCache);
        return this;
    }

    public StarRocksIcebergTableScan deleteFileCache(Cache<String, Set<DeleteFile>> deleteFileCache) {
        scanContext.setDeleteFileCache(deleteFileCache);
        return this;
    }

    public StarRocksIcebergTableScan dataFileCacheWithMetrics(boolean dataFileCacheWithMetrics) {
        scanContext.setDataFileCacheWithMetrics(dataFileCacheWithMetrics);
        return this;
    }

    public StarRocksIcebergTableScan localParallelism(int localParallelism) {
        scanContext.setLocalParallelism(localParallelism);
        return this;
    }

    public StarRocksIcebergTableScan localPlanningMaxSlotSize(long localPlanningMaxSlotSize) {
        scanContext.setLocalPlanningMaxSlotSize(localPlanningMaxSlotSize);
        return this;
    }

    public StarRocksIcebergTableScan forceEnableManifestCacheWithoutMetricsWithDeleteFile(
            boolean forceEnableManifestCacheWithoutMetricsWithDeleteFile) {
        scanContext.setForceEnableManifestCacheWithoutMetricsWithDeleteFile(
                forceEnableManifestCacheWithoutMetricsWithDeleteFile);
        return this;
    }

    public StarRocksIcebergTableScan toRefreshManifest(List<ManifestFile> manifestFiles) {
        this.toRefreshDataManifests = manifestFiles;
        return this;
    }

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
        List<ManifestFile> dataManifests = findMatchingDataManifests(snapshot());
        List<ManifestFile> deleteManifests = findMatchingDeleteManifests(snapshot());

        boolean mayHaveEqualityDeletes = !deleteManifests.isEmpty() && mayHaveEqualityDeletes(snapshot());
        loadColumnStats = mayHaveEqualityDeletes || shouldReturnColumnStats();

        if (shouldPlanLocally(dataManifests, loadColumnStats)) {
            return planFileTasksLocally(dataManifests, deleteManifests);
        } else {
            scanMetrics().scannedDataManifests().increment(dataManifests.size());
            scanMetrics().scannedDeleteManifests().increment(deleteManifests.size());
            long liveFilesCount = liveFilesCount(dataManifests);
            return planFileTasksRemotely(deleteManifests, liveFilesCount);
        }
    }

    private CloseableIterable<FileScanTask> planFileTasksRemotely(List<ManifestFile> deleteManifests, long liveFilesCount) {
        LOG.info("Planning file tasks remotely for table {}.{}", dbName, tableName);
        String predicate = filter() == Expressions.alwaysTrue() ? "" : SerializationUtil.serializeToBase64(filter());
        this.deleteFileIndex = planDeletesLocally(deleteManifests, Sets.newHashSet());

        ConnectContext currentContext = ConnectContext.get();
        MetadataCollectJob metadataCollectJob = new IcebergMetadataCollectJob(
                catalogName, dbName, tableName, TResultSinkType.METADATA_ICEBERG, snapshotId(), predicate);
        metadataCollectJob.init(currentContext);

        long currentTimestamp = System.currentTimeMillis();
        String threadNamePrefix = String.format("%s-%s-%s-%d", catalogName, dbName, tableName, currentTimestamp);
        executeInNewThread(threadNamePrefix + "-fetch_result", metadataCollectJob::asyncCollectMetadata);

        MetadataParser parser = new MetadataParser(
                table(), specStringCache, residualCache, planExecutor(), scanMetrics(),
                deleteFileIndex, metadataCollectJob, liveFilesCount);
        executeInNewThread(threadNamePrefix + "-parallel_parser", parser::parse);

        currentContext.setThreadLocalInfo();
        return new AsyncIterable<>(parser.getFileScanTaskQueue(), parser);
    }

    private DeleteFileIndex planDeletesLocally(List<ManifestFile> deleteManifests, Set<DeleteFile> cachedDeleteFiles) {
        DeleteFileIndex.Builder builder = DeleteFileIndex.builderFor(io(), deleteManifests);
        if (cachedDeleteFiles != null && !cachedDeleteFiles.isEmpty()) {
            builder.cachedDeleteFiles(cachedDeleteFiles);
        }

        if (shouldPlanWithExecutor() && deleteManifests.size() > 1) {
            builder.planWith(planExecutor());
        }

        return builder
                .specsById(table().specs())
                .filterData(filter())
                .caseSensitive(isCaseSensitive())
                .scanMetrics(scanMetrics())
                .deleteFileCache(deleteFileCache)
                .build();
    }

    private List<ManifestFile> findMatchingDataManifests(Snapshot snapshot) {
        List<ManifestFile> dataManifests = snapshot.dataManifests(io());
        scanMetrics().totalDataManifests().increment(dataManifests.size());

        List<ManifestFile> matchingDataManifests = filterManifests(dataManifests);
        int skippedDataManifestsCount = dataManifests.size() - matchingDataManifests.size();
        scanMetrics().skippedDataManifests().increment(skippedDataManifestsCount);

        return matchingDataManifests;
    }

    private List<ManifestFile> findMatchingDeleteManifests(Snapshot snapshot) {
        List<ManifestFile> deleteManifests = snapshot.deleteManifests(io());
        scanMetrics().totalDeleteManifests().increment(deleteManifests.size());

        List<ManifestFile> matchingDeleteManifests = filterManifests(deleteManifests);
        int skippedDeleteManifestsCount = deleteManifests.size() - matchingDeleteManifests.size();
        scanMetrics().skippedDeleteManifests().increment(skippedDeleteManifestsCount);

        return matchingDeleteManifests;
    }

    private List<ManifestFile> filterManifests(List<ManifestFile> manifests) {
        Map<Integer, ManifestEvaluator> evalCache = specCache(this::newManifestEvaluator);

        return manifests.stream()
                .filter(manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles())
                .filter(manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest))
                .collect(Collectors.toList());
    }

    private boolean shouldPlanLocally(List<ManifestFile> manifests, boolean loadColumnStats) {
        return (planMode == PlanMode.AUTO && loadColumnStats) || shouldPlanLocally(manifests);
    }

    // TODO(stephen): check cluster load
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
        List<ComputeNode> workers = GlobalStateMgr.getCurrentState().getNodeMgr()
                .getClusterInfo().getAvailableComputeNodes();
        return workers.stream().mapToInt(ComputeNode::getCpuCores).sum();
    }

    private long totalSize(List<ManifestFile> manifests) {
        return manifests.stream().mapToLong(ManifestFile::length).sum();
    }

    private CloseableIterable<FileScanTask> planFileTasksLocally(
            List<ManifestFile> dataManifests, List<ManifestFile> deleteManifests) {
        if (dataFileCache != null) {
            planDeletesLocallyWithCache(deleteManifests);
            return planTaskWithCache(dataManifests);
        } else {
            ManifestGroup manifestGroup = newManifestGroup(dataManifests, deleteManifests);
            return manifestGroup.planFiles();
        }
    }

    private void planDeletesLocallyWithCache(List<ManifestFile> deleteManifests) {
        List<ManifestFile> deleteManifestWithoutCache = new ArrayList<>();
        Set<DeleteFile> matchingCachedDeleteFiles = Sets.newHashSet();
        if (deleteFileCache != null) {
            for (ManifestFile manifestFile : deleteManifests) {
                Set<DeleteFile> deleteFiles = deleteFileCache.getIfPresent(manifestFile.path());
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
                    deleteManifestWithoutCache.add(manifestFile);
                }
            }
        } else {
            deleteManifestWithoutCache = deleteManifests;
        }

        this.deleteFileIndex = planDeletesLocally(deleteManifestWithoutCache, matchingCachedDeleteFiles);
    }

    private CloseableIterable<FileScanTask> planTaskWithCache(List<ManifestFile> dataManifests) {
        List<ManifestFile> dataManifestWithCache = new ArrayList<>();
        List<ManifestFile> dataManifestWithoutCache = new ArrayList<>();
        if (canUseDataFileCache()) {
            for (ManifestFile manifestFile : dataManifests) {
                if (dataFileCache.asMap().containsKey(manifestFile.path())) {
                    dataManifestWithCache.add(manifestFile);
                    scanMetrics().scannedDataManifests().increment();
                } else {
                    if (!onlyReadCache) {
                        dataFileCache.put(manifestFile.path(), ConcurrentHashMap.newKeySet());
                    }
                    dataManifestWithoutCache.add(manifestFile);
                }
            }
        } else {
            dataManifestWithoutCache = dataManifests;
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

    public void refreshManifest(List<ManifestFile> manifestFiles) {
        for (ManifestFile manifestFile : manifestFiles) {
            dataFileCache.put(manifestFile.path(), Sets.newHashSet());
        }
        this.deleteFileIndex = DeleteFileIndex.builderFor(new ArrayList<>()).build();
        try (CloseableIterable<FileScanTask> fileScanTaskIterable = planFileTasks(manifestFiles, new ArrayList<>());
                CloseableIterator<FileScanTask> fileScanTaskIterator = fileScanTaskIterable.iterator()) {
            while (fileScanTaskIterator.hasNext()) {
                fileScanTaskIterator.next();
            }
        } catch (IOException e) {
            throw new StarRocksConnectorException("Failed to refresh manifest cache", e);
        }
    }

    private boolean canUseDataFileCache() {
        return deleteFileIndex.isEmpty() ||
                (dataFileCacheWithMetrics || forceEnableManifestCacheWithoutMetricsWithDeleteFile);
    }

    private CloseableIterable<FileScanTask> filterDataFiles(ManifestFile manifestFile) {
        CloseableIterable<DataFile> matchedDataFiles = CloseableIterable.withNoopClose(
                dataFileCache.getIfPresent(manifestFile.path()));

        if (filter() != Expressions.alwaysTrue()) {
            matchedDataFiles =  CloseableIterable.filter(
                    scanMetrics().skippedDataFiles(),
                    CloseableIterable.withNoopClose(dataFileCache.getIfPresent(manifestFile.path())),
                    file -> partitionEvaluatorCache.get(file.specId()).eval(file.partition()));
        }

        if (dataFileCacheWithMetrics) {
            matchedDataFiles =  CloseableIterable.filter(
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
                new ManifestGroup(io(), dataManifests, deleteManifests)
                        .caseSensitive(isCaseSensitive())
                        .select(shouldReturnColumnStats() ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
                        .filterData(filter())
                        .specsById(table().specs())
                        .scanMetrics(scanMetrics())
                        .ignoreDeleted()
                        .withDataFileCache(dataFileCache)
                        .preparedDeleteFileIndex(deleteFileIndex)
                        .cacheWithMetrics(dataFileCacheWithMetrics);

        if (shouldIgnoreResiduals()) {
            manifestGroup = manifestGroup.ignoreResiduals();
        }

        if (shouldPlanWithExecutor() && (dataManifests.size() > 1 || deleteManifests.size() > 1)) {
            manifestGroup = manifestGroup.planWith(planExecutor());
        }

        return manifestGroup.planFiles();
    }

    private FileScanTask toFileScanTask(DataFile dataFile) {
        String specString = specStringCache.get(dataFile.specId());
        ResidualEvaluator residuals = residualCache.get(dataFile.specId());

        DeleteFile[] deleteFiles = deleteFileIndex.forDataFile(dataFile);

        ScanMetricsUtil.fileTask(scanMetrics(), dataFile, deleteFiles);

        return new BaseFileScanTask(
                dataFile,
                deleteFiles,
                schemaString,
                specString,
                residuals);
    }

    private ManifestEvaluator newManifestEvaluator(PartitionSpec spec) {
        Expression projection = Projections.inclusive(spec, isCaseSensitive()).project(filter());
        return ManifestEvaluator.forPartitionFilter(projection, spec, isCaseSensitive());
    }

    private ResidualEvaluator newResidualEvaluator(PartitionSpec spec) {
        return ResidualEvaluator.of(spec, residualFilter(), isCaseSensitive());
    }

    private Evaluator newPartitionEvaluator(PartitionSpec spec) {
        Expression projected = Projections.inclusive(spec, false).project(filter());
        return new Evaluator(spec.partitionType(), projected, false);
    }

    private InclusiveMetricsEvaluator newInclusiveMetricsEvaluator(PartitionSpec spec) {
        if (filter() != null) {
            return new InclusiveMetricsEvaluator(spec.schema(), filter(), false);
        } else {
            return new InclusiveMetricsEvaluator(spec.schema(), Expressions.alwaysTrue(), false);
        }
    }

    private <R> Map<Integer, R> specCache(Function<PartitionSpec, R> load) {
        Map<Integer, R> cache = new ConcurrentHashMap<>();
        table().specs().forEach((specId, spec) -> cache.put(specId, load.apply(spec)));
        return cache;
    }

    private boolean mayHaveEqualityDeletes(Snapshot snapshot) {
        String count = snapshot.summary().get(SnapshotSummary.TOTAL_EQ_DELETES_PROP);
        return count == null || !count.equals("0");
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
}
