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
import com.starrocks.connector.iceberg.StarRocksIcebergTableScanContext;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private final StarRocksIcebergTableScanContext scanContext;

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
        this.specStringCache = specCache(PartitionSpecParser::toJson);
        this.residualCache = specCache(this::newResidualEvaluator);
        this.partitionEvaluatorCache = specCache(this::newPartitionEvaluator);
        this.inclusiveMetricsEvaluatorCache = specCache(this::newInclusiveMetricsEvaluator);
        this.schemaString = SchemaParser.toJson(tableSchema());
        this.dataFileCache = scanContext.getDataFileCache();
        this.deleteFileCache = scanContext.getDeleteFileCache();
        this.dataFileCacheWithMetrics = scanContext.isDataFileCacheWithMetrics();
    }

    @Override
    protected TableScan newRefinedScan(Table newTable, Schema newSchema, TableScanContext newContext) {
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

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
        List<ManifestFile> dataManifests = findMatchingDataManifests(snapshot());
        List<ManifestFile> deleteManifests = findMatchingDeleteManifests(snapshot());

        // TODO(stephen): add distributed plan
        return planFileTasksLocally(dataManifests, deleteManifests);
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
        return dataFileCache != null;
    }

    private void planDeletesLocallyWithCache(List<ManifestFile> deleteManifests) {
        List<ManifestFile> deleteManifestWithoutCache = new ArrayList<>();
        Set<DeleteFile> matchingCachedDeleteFiles = Sets.newHashSet();
        if (deleteFileCache != null) {
            for (ManifestFile manifestFile : deleteManifests) {
                Set<DeleteFile> deleteFiles = deleteFileCache.getIfPresent(manifestFile.path());
                if (deleteFiles != null && !deleteFiles.isEmpty()) {
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
        for (ManifestFile manifestFile : dataManifests) {
            Set<DataFile> dataFiles = dataFileCache.getIfPresent(manifestFile.path());
            if (dataFiles != null && !dataFiles.isEmpty()) {
                dataManifestWithCache.add(manifestFile);
                scanMetrics().scannedDataManifests().increment();
            } else {
                dataFileCache.put(manifestFile.path(), ConcurrentHashMap.newKeySet());
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

    @Override
    public CloseableIterable<CombinedScanTask> planTasks() {
        return TableScanUtil.planTasks(
                planFiles(), targetSplitSize(), splitLookback(), splitOpenFileCost());
    }
}
