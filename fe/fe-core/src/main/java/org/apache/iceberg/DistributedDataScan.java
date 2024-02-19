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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.MetadataCollectJob;
import com.starrocks.connector.MetadataExecutor;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.AsyncIterable;
import com.starrocks.connector.share.iceberg.CommonMetadataBean;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TIcebergMetadata;
import com.starrocks.thrift.TMetadataEntry;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.PartitionUtil.executeInNewThread;
import static org.apache.iceberg.BaseFile.EMPTY_PARTITION_DATA;

public class DistributedDataScan
        extends DataScan<TableScan, FileScanTask, CombinedScanTask> implements TableScan {
    private static final Logger LOG = LogManager.getLogger(DistributedDataScan.class);
    private static final long LOCAL_PLANNING_MAX_SLOT_SIZE = 8L * 1024 * 1024;
    private final Map<Integer, PartitionData> partitionDataTemplates = new ConcurrentHashMap<>();
    private final int localParallelism;
    private final long localPlanningSizeThreshold;
    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final PlanMode planMode;
    private boolean loadColumnStats;
    private final boolean isPartitionedTable;
    private boolean fetchResultFinished;
    AtomicBoolean buildScanTaskFinished = new AtomicBoolean(false);

    private final ConcurrentLinkedQueue<TResultBatch> resultBatchQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<FileScanTask> fileScanTaskQueue = new ConcurrentLinkedQueue<>();

    private final Map<Integer, String> specStringCache;
    private final Map<Integer, ResidualEvaluator> residualCache;
    private final String schemaString;

    private final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.register(CommonMetadataBean.class);
        return kryo;
    });

    public static TableScanContext newTableScanContext(Table table) {
        if (table instanceof BaseTable) {
            MetricsReporter reporter = ((BaseTable) table).reporter();
            return ImmutableTableScanContext.builder().metricsReporter(reporter).build();
        } else {
            return TableScanContext.empty();
        }
    }

    public DistributedDataScan(Table table, Schema schema, TableScanContext context,
                               String catalogName, String dbName, String tableName, PlanMode planMode) {
        super(table, schema, context);
        this.localParallelism = ThreadPools.WORKER_THREAD_POOL_SIZE;
        this.localPlanningSizeThreshold = localParallelism * LOCAL_PLANNING_MAX_SLOT_SIZE;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.planMode = planMode;
        this.isPartitionedTable = table.spec().isPartitioned();
        this.specStringCache = specCache(PartitionSpecParser::toJson);
        this.residualCache = specCache(this::newResidualEvaluator);
        this.schemaString = SchemaParser.toJson(tableSchema());
    }

    @Override
    protected TableScan newRefinedScan(
            Table newTable, Schema newSchema, TableScanContext newContext) {
        return new DistributedDataScan(newTable, newSchema, newContext, catalogName, dbName, tableName, planMode);
    }

    private int remoteParallelism() {
        List<ComputeNode> workers = GlobalStateMgr.getCurrentState().getNodeMgr()
                .getClusterInfo().getAvailableComputeNodes();
        return workers.stream().mapToInt(ComputeNode::getCpuCores).sum();
    }

    // TODO(stephen): add more distributed strategies
    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
        List<ManifestFile> dataManifests;
        List<ManifestFile> deleteManifests;
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.prepare")) {
            Snapshot snapshot = snapshot();
            deleteManifests = findMatchingDeleteManifests(snapshot);
            boolean mayHaveEqualityDeletes = deleteManifests.size() > 0 && mayHaveEqualityDeletes(snapshot);

            dataManifests = findMatchingDataManifests(snapshot);
            loadColumnStats = mayHaveEqualityDeletes || shouldReturnColumnStats();
        }


        if (shouldPlanLocally(dataManifests, loadColumnStats)) {
            return planFileTasksLocally(dataManifests, deleteManifests);
        } else {
            try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.planFileTaskRemotely")) {
                scanMetrics().scannedDataManifests().increment(dataManifests.size());
                scanMetrics().scannedDeleteManifests().increment(deleteManifests.size());
                long liveFilesCount = liveFilesCount(dataManifests);
                return planFileTasksRemotely(deleteManifests, liveFilesCount);
            }
        }
    }

    private CloseableIterable<FileScanTask> planFileTasksRemotely(List<ManifestFile> deleteManifests, long liveFilesCount) {
        LOG.info("Planning file tasks remotely for table {}.{}", dbName, tableName);

        String predicate = filter() == Expressions.alwaysTrue() ? "" : SerializationUtil.serializeToBase64(filter());
        DeleteFileIndex deleteFileIndex;
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.buildDeleteIndex")) {
            deleteFileIndex = planDeletesLocally(deleteManifests);
        }

        ConnectContext currentContext = ConnectContext.get();

        Coordinator coord;
        MetadataCollectJob collectJob = new MetadataCollectJob(catalogName, dbName, tableName,
                snapshot().snapshotId(), predicate, TResultSinkType.METADATA_ICEBERG);
        collectJob.build();
        coord = MetadataExecutor.executeDQL(collectJob);

        long currentTimestamp = System.currentTimeMillis();
        Tracers tracers = Tracers.get();
        executeInNewThread(String.format("%s-%s-%s-%d-fetch_result", catalogName, dbName, tableName, currentTimestamp),
                () -> fetchResult(coord, tracers));
        executeInNewThread(String.format("%s-%s-%s-%d-parallel_deserialize", catalogName, dbName, tableName, currentTimestamp),
                () -> parallelBuildFileScanTask(tracers, fileScanTaskQueue, deleteFileIndex, liveFilesCount));

        currentContext.setThreadLocalInfo();
        return new AsyncIterable<>(fileScanTaskQueue, buildScanTaskFinished);
    }

    private boolean fetchResult(Coordinator coord, Tracers tracers) {
        try (Timer ignored = Tracers.watchScope(tracers, EXTERNAL, "ICEBERG.getRowBatch")) {
            RowBatch batch;
            do {
                batch = coord.getNext();
                if (batch.getBatch() != null) {
                    resultBatchQueue.add(batch.getBatch());
                }
            } while (!batch.isEos());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            this.fetchResultFinished = true;
            QeProcessorImpl.INSTANCE.unregisterQuery(ConnectContext.get().getExecutionId());
        }
        return true;
    }
    
    private boolean parallelBuildFileScanTask(Tracers tracers, ConcurrentLinkedQueue<FileScanTask> fileScanTaskQueue,
                                              DeleteFileIndex deleteFileIndex, long liveFilesCount) {
        ExecutorService executorService = planExecutor();
        List<Future<Boolean>> taskFutures = new ArrayList<>();
        try (Timer ignored = Tracers.watchScope(tracers, EXTERNAL, "ICEBERG.ParallelDeserializeMetadata")) {
            while (!fetchResultFinished || !resultBatchQueue.isEmpty()) {
                if (!resultBatchQueue.isEmpty()) {
                    TResultBatch resultBatch = resultBatchQueue.poll();
                    taskFutures.add(executorService.submit(() -> {
                        List<FileScanTask> scanTasks = buildFileScanTask(resultBatch, deleteFileIndex);
                        return fileScanTaskQueue.addAll(scanTasks);
                    }));
                }
            }
        }

        try (Timer ignored = Tracers.watchScope(tracers, EXTERNAL, "ICEBERG.getFutures")) {
            for (Future<Boolean> future : taskFutures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOG.error("build file scan task failed", e);
                    fileScanTaskQueue.clear();
                }
            }
        } finally {
            while (true) {
                if (fileScanTaskQueue.isEmpty()) {
                    buildScanTaskFinished.set(true);
                    break;
                }

                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    LOG.error(e);
                }
            }
        }

        scanMetrics().skippedDataFiles().increment(liveFilesCount - scanMetrics().resultDataFiles().value());
        return true;
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
                return remoteParallelism() <= localParallelism
                        || manifests.size() <= 2 * localParallelism
                        || totalSize(manifests) <= localPlanningSizeThreshold;

            default:
                throw new IllegalArgumentException("Unknown plan mode: " + planMode);
        }
    }

    private long totalSize(List<ManifestFile> manifests) {
        return manifests.stream().mapToLong(ManifestFile::length).sum();
    }

    private CloseableIterable<FileScanTask> planFileTasksLocally(
            List<ManifestFile> dataManifests, List<ManifestFile> deleteManifests) {
        LOG.info("Planning file tasks locally for table {}", table().name());
        ManifestGroup manifestGroup = newManifestGroup(dataManifests, deleteManifests);
        return manifestGroup.planFiles();
    }
    
    private List<FileScanTask> buildFileScanTask(TResultBatch resultBatch, DeleteFileIndex deleteFileIndex) {
        List<DataFile> dataFiles = deserializedMetadata(resultBatch);
        return toFileScanTasks(dataFiles, deleteFileIndex);
    }

    private List<DataFile> deserializedMetadata(TResultBatch resultBatch) {
        List<DataFile> dataFiles = new ArrayList<>();
        TDeserializer deserializer = new TDeserializer();
        for (ByteBuffer bb : resultBatch.rows) {
            TMetadataEntry entry;
            entry = new TMetadataEntry();
            byte[] bytes = new byte[bb.limit() - bb.position()];
            bb.get(bytes);
            try {
                deserializer.deserialize(entry, bytes);
            } catch (TException e) {
                throw new RuntimeException(e);
            }
            DataFile baseFile = (DataFile) buildIcebergFile(entry);
            dataFiles.add(baseFile);
        }

        return dataFiles;
    }

    private ContentFile<?> buildIcebergFile(TMetadataEntry entry) {
        if (!entry.isSetIceberg_metadata()) {
            throw new StarRocksConnectorException("not found iceberg metadata in result");
        }
        TIcebergMetadata thrift = entry.getIceberg_metadata();
        FileContent content = FileContent.values()[thrift.content];

        int specId = thrift.getSpec_id();
        String filePath = thrift.getFile_path();
        FileFormat fileFormat = FileFormat.fromString(thrift.getFile_format());

        PartitionData partitionData;
        partitionData = buildPartitionData(thrift);
        long fileLength = thrift.getFile_size_in_bytes();
        Metrics metrics;
         metrics = buildMetrics(thrift);

        List<Long> splitOffset = thrift.getSplit_offsets();
        Integer sortId = thrift.getSort_id();

        int[] equalityFiledIds = thrift.isSetEquality_ids() ?  ArrayUtil.toIntArray(thrift.getEquality_ids()) : null;

        if (content == FileContent.DATA) {
            GenericDataFile dataFile = new GenericDataFile(
                        specId,
                        filePath,
                        fileFormat,
                        partitionData,
                        fileLength,
                        metrics,
                        null,
                        splitOffset,
                        equalityFiledIds,
                        null);
            if (thrift.isSetFile_sequence_number()) {
                dataFile.setFileSequenceNumber(thrift.getFile_sequence_number());
            }
            if (thrift.isSetData_sequence_number()) {
                dataFile.setDataSequenceNumber(thrift.getData_sequence_number());
            }
            return dataFile;
        } else {
            GenericDeleteFile deleteFile = new GenericDeleteFile(
                    specId,
                    content,
                    filePath,
                    FileFormat.PARQUET,
                    partitionData,
                    fileLength,
                    metrics,
                    equalityFiledIds,
                    sortId,
                    splitOffset,
                    null
            );
            if (thrift.isSetFile_sequence_number()) {
                deleteFile.setFileSequenceNumber(thrift.getFile_sequence_number());
            }
            if (thrift.isSetData_sequence_number()) {
                deleteFile.setDataSequenceNumber(thrift.getData_sequence_number());
            }
            return deleteFile;
        }
    }

    private PartitionData buildPartitionData(TIcebergMetadata thrift) {
        Integer specId = thrift.getSpec_id();
        PartitionSpec spec = table().specs().get(specId);
        if (!isPartitionedTable) {
            return EMPTY_PARTITION_DATA;
        }

        PartitionData partitionDataTemplate = partitionDataTemplates.computeIfAbsent(specId,
                k -> new PartitionData(spec.partitionType()));

        Input input = new Input(org.apache.thrift.TBaseHelper.rightSize(thrift.partition_data).array());
        CommonMetadataBean bean = kryoThreadLocal.get().readObject(input, CommonMetadataBean.class);
        input.close();

        return new PartitionData(partitionDataTemplate, bean.getValues());
    }

    private Metrics buildMetrics(TIcebergMetadata thrift) {
        long recordCount = thrift.getRecord_count();
        // TODO(stephen): hard code and refactor it.
        return new Metrics(recordCount, null, null, null, null, null, null);
    }


    private List<FileScanTask> toFileScanTasks(
            List<DataFile> dataFiles,
            DeleteFileIndex deleteFileIndex) {
        List<FileScanTask> scanTasks = new ArrayList<>();
        for (DataFile dataFile : dataFiles) {
            FileScanTask task = toFileScanTasks(dataFile, deleteFileIndex);
            scanTasks.add(task);
        }

        return scanTasks;
    }

    private FileScanTask toFileScanTasks(
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
