/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.cache.Cache;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.ParallelIterable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Predicate;

// copy from https://github.com/apache/iceberg/blob/apache-iceberg-1.5.0/core/src/main/java/org/apache/iceberg/ManifestGroup.java
class ManifestGroup {
    private static final Types.StructType EMPTY_STRUCT = Types.StructType.of();

    private final FileIO io;
    private final Set<ManifestFile> dataManifests;
    private final DeleteFileIndex.Builder deleteIndexBuilder;
    private Predicate<ManifestFile> manifestPredicate;
    private Predicate<ManifestEntry<DataFile>> manifestEntryPredicate;
    private Map<Integer, PartitionSpec> specsById;
    private Expression dataFilter;
    private Expression fileFilter;
    private Expression partitionFilter;
    private boolean ignoreDeleted;
    private boolean ignoreExisting;
    private boolean ignoreResiduals;
    private List<String> columns;
    private boolean caseSensitive;
    private Set<Integer> columnsToKeepStats;
    private ExecutorService executorService;
    private ScanMetrics scanMetrics;
    private boolean dataFileCacheWithMetrics;
    private Cache<String, Set<DataFile>> dataFileCache;
    private DeleteFileIndex preparedDeleteFileIndex;
    private Set<Integer> identifierFieldIds;

    ManifestGroup(FileIO io, Iterable<ManifestFile> manifests) {
        this(
                io,
                Iterables.filter(manifests, manifest -> manifest.content() == ManifestContent.DATA),
                Iterables.filter(manifests, manifest -> manifest.content() == ManifestContent.DELETES));
    }

    ManifestGroup(
            FileIO io, Iterable<ManifestFile> dataManifests, Iterable<ManifestFile> deleteManifests) {
        this.io = io;
        this.dataManifests = Sets.newHashSet(dataManifests);
        this.deleteIndexBuilder = DeleteFileIndex.builderFor(io, deleteManifests);
        this.dataFilter = Expressions.alwaysTrue();
        this.fileFilter = Expressions.alwaysTrue();
        this.partitionFilter = Expressions.alwaysTrue();
        this.ignoreDeleted = false;
        this.ignoreExisting = false;
        this.ignoreResiduals = false;
        this.columns = ManifestReader.ALL_COLUMNS;
        this.caseSensitive = true;
        this.manifestPredicate = m -> true;
        this.manifestEntryPredicate = e -> true;
        this.scanMetrics = ScanMetrics.noop();
    }

    ManifestGroup specsById(Map<Integer, PartitionSpec> newSpecsById) {
        this.specsById = newSpecsById;
        deleteIndexBuilder.specsById(newSpecsById);
        return this;
    }

    ManifestGroup filterData(Expression newDataFilter) {
        this.dataFilter = Expressions.and(dataFilter, newDataFilter);
        deleteIndexBuilder.filterData(newDataFilter);
        return this;
    }

    ManifestGroup filterFiles(Expression newFileFilter) {
        this.fileFilter = Expressions.and(fileFilter, newFileFilter);
        return this;
    }

    ManifestGroup filterPartitions(Expression newPartitionFilter) {
        this.partitionFilter = Expressions.and(partitionFilter, newPartitionFilter);
        deleteIndexBuilder.filterPartitions(newPartitionFilter);
        return this;
    }

    ManifestGroup filterManifests(Predicate<ManifestFile> newManifestPredicate) {
        this.manifestPredicate = manifestPredicate.and(newManifestPredicate);
        return this;
    }

    ManifestGroup filterManifestEntries(
            Predicate<ManifestEntry<DataFile>> newManifestEntryPredicate) {
        this.manifestEntryPredicate = manifestEntryPredicate.and(newManifestEntryPredicate);
        return this;
    }

    ManifestGroup scanMetrics(ScanMetrics metrics) {
        this.scanMetrics = metrics;
        return this;
    }

    ManifestGroup ignoreDeleted() {
        this.ignoreDeleted = true;
        return this;
    }

    ManifestGroup ignoreExisting() {
        this.ignoreExisting = true;
        return this;
    }

    ManifestGroup ignoreResiduals() {
        this.ignoreResiduals = true;
        return this;
    }

    ManifestGroup select(List<String> newColumns) {
        this.columns = Lists.newArrayList(newColumns);
        return this;
    }

    ManifestGroup caseSensitive(boolean newCaseSensitive) {
        this.caseSensitive = newCaseSensitive;
        deleteIndexBuilder.caseSensitive(newCaseSensitive);
        return this;
    }

    ManifestGroup columnsToKeepStats(Set<Integer> newColumnsToKeepStats) {
        this.columnsToKeepStats =
                newColumnsToKeepStats == null ? null : Sets.newHashSet(newColumnsToKeepStats);
        return this;
    }

    ManifestGroup planWith(ExecutorService newExecutorService) {
        this.executorService = newExecutorService;
        deleteIndexBuilder.planWith(newExecutorService);
        return this;
    }

    ManifestGroup cacheWithMetrics(boolean cacheWithMetrics) {
        this.dataFileCacheWithMetrics = cacheWithMetrics;
        return this;
    }

    ManifestGroup withDataFileCache(Cache<String, Set<DataFile>> fileCache) {
        this.dataFileCache = fileCache;
        return this;
    }

    ManifestGroup identifierFieldIds(Set<Integer> identifierFieldIds) {
        this.identifierFieldIds = identifierFieldIds;
        return this;
    }


    ManifestGroup preparedDeleteFileIndex(DeleteFileIndex deleteFileIndex) {
        this.preparedDeleteFileIndex = deleteFileIndex;
        return this;
    }

    /**
     * Returns an iterable of scan tasks. It is safe to add entries of this iterable to a collection
     * as {@link DataFile} in each {@link FileScanTask} is defensively copied.
     *
     * @return a {@link CloseableIterable} of {@link FileScanTask}
     */
    public CloseableIterable<FileScanTask> planFiles() {
        return plan(ManifestGroup::createFileScanTasks);
    }

    public <T extends ScanTask> CloseableIterable<T> plan(CreateTasksFunction<T> createTasksFunc) {
        LoadingCache<Integer, ResidualEvaluator> residualCache =
                Caffeine.newBuilder()
                        .build(
                                specId -> {
                                    PartitionSpec spec = specsById.get(specId);
                                    Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
                                    return ResidualEvaluator.of(spec, filter, caseSensitive);
                                });

        DeleteFileIndex deleteFiles = preparedDeleteFileIndex != null ? preparedDeleteFileIndex :
                deleteIndexBuilder.scanMetrics(scanMetrics).build();

        boolean dropStats = ManifestReader.dropStats(columns);
        if (!deleteFiles.isEmpty()) {
            select(ManifestReader.withStatsColumns(columns));
        }

        LoadingCache<Integer, TaskContext> taskContextCache =
                Caffeine.newBuilder()
                        .build(
                                specId -> {
                                    PartitionSpec spec = specsById.get(specId);
                                    ResidualEvaluator residuals = residualCache.get(specId);
                                    return new TaskContext(
                                            spec, deleteFiles, residuals, dropStats, columnsToKeepStats, scanMetrics);
                                });

        Iterable<CloseableIterable<T>> tasks =
                entries(
                        (manifest, entries) -> {
                            int specId = manifest.partitionSpecId();
                            TaskContext taskContext = taskContextCache.get(specId);
                            return createTasksFunc.apply(entries, taskContext);
                        });

        if (executorService != null) {
            return new ParallelIterable<>(tasks, executorService);
        } else {
            return CloseableIterable.concat(tasks);
        }
    }

    /**
     * Returns an iterable for manifest entries in the set of manifests.
     *
     * <p>Entries are not copied and it is the caller's responsibility to make defensive copies if
     * adding these entries to a collection.
     *
     * @return a CloseableIterable of manifest entries.
     */
    public CloseableIterable<ManifestEntry<DataFile>> entries() {
        return CloseableIterable.concat(entries((manifest, entries) -> entries));
    }

    /**
     * Returns an iterable for groups of data files in the set of manifests.
     *
     * <p>Files are not copied, it is the caller's responsibility to make defensive copies if adding
     * these files to a collection.
     *
     * @return an iterable of file groups
     */
    public Iterable<CloseableIterable<DataFile>> fileGroups() {
        return entries(
                (manifest, entries) -> CloseableIterable.transform(entries, ManifestEntry::file));
    }

    private <T> Iterable<CloseableIterable<T>> entries(
            BiFunction<ManifestFile, CloseableIterable<ManifestEntry<DataFile>>, CloseableIterable<T>>
                    entryFn) {
        LoadingCache<Integer, ManifestEvaluator> evalCache =
                specsById == null
                        ? null
                        : Caffeine.newBuilder()
                        .build(
                                specId -> {
                                    PartitionSpec spec = specsById.get(specId);
                                    return ManifestEvaluator.forPartitionFilter(
                                            Expressions.and(
                                                    partitionFilter,
                                                    Projections.inclusive(spec, caseSensitive).project(dataFilter)),
                                            spec,
                                            caseSensitive);
                                });

        Evaluator evaluator;
        if (fileFilter != null && fileFilter != Expressions.alwaysTrue()) {
            evaluator = new Evaluator(DataFile.getType(EMPTY_STRUCT), fileFilter, caseSensitive);
        } else {
            evaluator = null;
        }

        CloseableIterable<ManifestFile> closeableDataManifests =
                CloseableIterable.withNoopClose(dataManifests);
        CloseableIterable<ManifestFile> matchingManifests =
                evalCache == null
                        ? closeableDataManifests
                        : CloseableIterable.filter(
                        scanMetrics.skippedDataManifests(),
                        closeableDataManifests,
                        manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));

        if (ignoreDeleted) {
            // only scan manifests that have entries other than deletes
            // remove any manifests that don't have any existing or added files. if either the added or
            // existing files count is missing, the manifest must be scanned.
            matchingManifests =
                    CloseableIterable.filter(
                            scanMetrics.skippedDataManifests(),
                            matchingManifests,
                            manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles());
        }

        if (ignoreExisting) {
            // only scan manifests that have entries other than existing
            // remove any manifests that don't have any deleted or added files. if either the added or
            // deleted files count is missing, the manifest must be scanned.
            matchingManifests =
                    CloseableIterable.filter(
                            scanMetrics.skippedDataManifests(),
                            matchingManifests,
                            manifest -> manifest.hasAddedFiles() || manifest.hasDeletedFiles());
        }

        matchingManifests =
                CloseableIterable.filter(
                        scanMetrics.skippedDataManifests(), matchingManifests, manifestPredicate);
        matchingManifests =
                CloseableIterable.count(scanMetrics.scannedDataManifests(), matchingManifests);

        return Iterables.transform(
                matchingManifests,
                manifest ->
                        new CloseableIterable<T>() {
                            private CloseableIterable<T> iterable;

                            @Override
                            public CloseableIterator<T> iterator() {
                                ManifestReader<DataFile> reader =
                                        ManifestFiles.read(manifest, io, specsById)
                                                .filterRows(dataFilter)
                                                .filterPartitions(partitionFilter)
                                                .caseSensitive(caseSensitive)
                                                .select(columns)
                                                .dataFileCache(dataFileCache)
                                                .cacheWithMetrics(dataFileCacheWithMetrics)
                                                .identifierFieldIds(identifierFieldIds)
                                                .scanMetrics(scanMetrics);

                                CloseableIterable<ManifestEntry<DataFile>> entries;
                                if (ignoreDeleted) {
                                    entries = reader.liveEntries();
                                } else {
                                    entries = reader.entries();
                                }

                                if (ignoreExisting) {
                                    entries =
                                            CloseableIterable.filter(
                                                    scanMetrics.skippedDataFiles(),
                                                    entries,
                                                    entry -> entry.status() != ManifestEntry.Status.EXISTING);
                                }

                                if (evaluator != null) {
                                    entries =
                                            CloseableIterable.filter(
                                                    scanMetrics.skippedDataFiles(),
                                                    entries,
                                                    entry -> evaluator.eval((GenericDataFile) entry.file()));
                                }

                                entries =
                                        CloseableIterable.filter(
                                                scanMetrics.skippedDataFiles(), entries, manifestEntryPredicate);

                                iterable = entryFn.apply(manifest, entries);

                                return iterable.iterator();
                            }

                            @Override
                            public void close() throws IOException {
                                if (iterable != null) {
                                    iterable.close();
                                }
                            }
                        });
    }

    private static CloseableIterable<FileScanTask> createFileScanTasks(
            CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext ctx) {
        return CloseableIterable.transform(
                entries,
                entry -> {
                    DataFile dataFile =
                            ContentFileUtil.copy(entry.file(), ctx.shouldKeepStats(), ctx.columnsToKeepStats());
                    DeleteFile[] deleteFiles = ctx.deletes().forEntry(entry);
                    ScanMetricsUtil.fileTask(ctx.scanMetrics(), dataFile, deleteFiles);
                    return new BaseFileScanTask(
                            dataFile, deleteFiles, ctx.schemaAsString(), ctx.specAsString(), ctx.residuals());
                });
    }

    @FunctionalInterface
    interface CreateTasksFunction<T extends ScanTask> {
        CloseableIterable<T> apply(
                CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext context);
    }

    static class TaskContext {
        private final String schemaAsString;
        private final String specAsString;
        private final DeleteFileIndex deletes;
        private final ResidualEvaluator residuals;
        private final boolean dropStats;
        private final Set<Integer> columnsToKeepStats;
        private final ScanMetrics scanMetrics;

        TaskContext(
                PartitionSpec spec,
                DeleteFileIndex deletes,
                ResidualEvaluator residuals,
                boolean dropStats,
                Set<Integer> columnsToKeepStats,
                ScanMetrics scanMetrics) {
            this.schemaAsString = SchemaParser.toJson(spec.schema());
            this.specAsString = PartitionSpecParser.toJson(spec);
            this.deletes = deletes;
            this.residuals = residuals;
            this.dropStats = dropStats;
            this.columnsToKeepStats = columnsToKeepStats;
            this.scanMetrics = scanMetrics;
        }

        String schemaAsString() {
            return schemaAsString;
        }

        String specAsString() {
            return specAsString;
        }

        DeleteFileIndex deletes() {
            return deletes;
        }

        ResidualEvaluator residuals() {
            return residuals;
        }

        boolean shouldKeepStats() {
            return !dropStats;
        }

        Set<Integer> columnsToKeepStats() {
            return columnsToKeepStats;
        }

        public ScanMetrics scanMetrics() {
            return scanMetrics;
        }
    }
}