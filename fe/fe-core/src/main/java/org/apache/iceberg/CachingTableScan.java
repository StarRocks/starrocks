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
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
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

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

public class CachingTableScan
        extends DataScan<TableScan, FileScanTask, CombinedScanTask> implements TableScan {
    private static final Logger LOG = LogManager.getLogger(CachingTableScan.class);
    private final String catalogName;
    private final String dbName;
    private final String tableName;

    private final Map<Integer, String> specStringCache;
    private final Map<Integer, ResidualEvaluator> residualCache;
    private final Map<Integer, Evaluator> partitionEvaluatorCache;
    private final String schemaString;
    private final Cache<String, Set<DataFile>> dataFileCache;
    private DeleteFileIndex deleteFileIndex;

    public static TableScanContext newTableScanContext(Table table) {
        if (table instanceof BaseTable) {
            MetricsReporter reporter = ((BaseTable) table).reporter();
            return ImmutableTableScanContext.builder().metricsReporter(reporter).build();
        } else {
            return TableScanContext.empty();
        }
    }

    public CachingTableScan(Table table, Schema schema, TableScanContext context,
                               String catalogName, String dbName, String tableName, Cache<String, Set<DataFile>> dataFileCache) {
        super(table, schema, context);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.specStringCache = specCache(PartitionSpecParser::toJson);
        this.residualCache = specCache(this::newResidualEvaluator);
        this.partitionEvaluatorCache = specCache(this::newPartitionEvaluator);
        this.schemaString = SchemaParser.toJson(tableSchema());
        this.dataFileCache = dataFileCache;
    }

    @Override
    protected TableScan newRefinedScan(
            Table newTable, Schema newSchema, TableScanContext newContext) {
        return new CachingTableScan(newTable, newSchema, newContext, catalogName, dbName, tableName, dataFileCache);
    }

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
        Snapshot snapshot = snapshot();
        List<ManifestFile> dataManifests = findMatchingDataManifests(snapshot);
        List<ManifestFile> deleteManifests = findMatchingDeleteManifests(snapshot);

        List<ManifestFile> manifestWithCache = new ArrayList<>();
        List<ManifestFile> manifestWithoutCache = new ArrayList<>();
        for (ManifestFile manifestFile : dataManifests) {
            if (dataFileCache.asMap().containsKey(manifestFile.path())) {
                manifestWithCache.add(manifestFile);
            } else {
                dataFileCache.put(manifestFile.path(), ConcurrentHashMap.newKeySet());
                manifestWithoutCache.add(manifestFile);
            }
        }

        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.buildDeleteIndex")) {
            deleteFileIndex = planDeletesLocally(deleteManifests);
        }

        Iterable<CloseableIterable<FileScanTask>> tasks =
                CloseableIterable.transform(CloseableIterable.withNoopClose(manifestWithCache), this::filterManifests);

        CloseableIterable<FileScanTask> tasksWithCache = new ParallelIterable<>(tasks, planExecutor());
        if (manifestWithoutCache.isEmpty()) {
            return tasksWithCache;
        } else {
            CloseableIterable<FileScanTask> fileScanTaskWithoutCache = planFileTasks(manifestWithoutCache, deleteManifests);
            return CloseableIterable.concat(Lists.newArrayList(fileScanTaskWithoutCache, tasksWithCache));
        }
    }

    private CloseableIterable<FileScanTask> filterManifests(ManifestFile manifestFile) {
        CloseableIterable<DataFile> matchedDataFiles =  CloseableIterable.filter(
                CloseableIterable.withNoopClose(dataFileCache.getIfPresent(manifestFile.path())),
                file -> partitionEvaluatorCache.get(file.specId()).eval(file.partition()));

        return CloseableIterable.transform(
                matchedDataFiles,
                dataFile -> toFileScanTask(dataFile, deleteFileIndex));
    }

    private DeleteFileIndex planDeletesLocally(List<ManifestFile> deleteManifests) {
        DeleteFileIndex.Builder builder = DeleteFileIndex.builderFor(io(), deleteManifests);

        if (shouldPlanWithExecutor() && deleteManifests.size() > 1) {
            builder.planWith(planExecutor());
        }

        return builder
                .specsById(table().specs())
                .filterData(filter())
                .caseSensitive(isCaseSensitive())
                .scanMetrics(scanMetrics())
                .build();
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
                        .withCache(dataFileCache);

        if (shouldIgnoreResiduals()) {
            manifestGroup = manifestGroup.ignoreResiduals();
        }

        if (shouldPlanWithExecutor() && (dataManifests.size() > 1 || deleteManifests.size() > 1)) {
            manifestGroup = manifestGroup.planWith(planExecutor());
        }

        return manifestGroup.planFiles();
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

    private long totalSize(List<ManifestFile> manifests) {
        return manifests.stream().mapToLong(ManifestFile::length).sum();
    }

    private FileScanTask toFileScanTask(
            DataFile dataFile,
            DeleteFileIndex deleteFileIndex) {
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

    private int liveFilesCount(List<ManifestFile> manifests) {
        return manifests.stream().mapToInt(this::liveFilesCount).sum();
    }

    private int liveFilesCount(ManifestFile manifest) {
        return manifest.existingFilesCount() + manifest.addedFilesCount();
    }
}
