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
import com.starrocks.thrift.TRowBatch;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.FutureTask;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.PartitionUtil.executeInNewThread;
import static org.apache.iceberg.BaseFile.EMPTY_PARTITION_DATA;

public class DistributedDataScan
        extends DataScan<TableScan, FileScanTask, CombinedScanTask> implements TableScan {
    private static final Logger LOG = LogManager.getLogger(DistributedDataScan.class);
    private static final long LOCAL_PLANNING_MAX_SLOT_SIZE = 8L * 1024 * 1024;
    private Map<Integer, PartitionData> partitionDataTemplates = new HashMap<>();
    private final int localParallelism;
    private final long localPlanningSizeThreshold;
    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final PlanMode planMode;
    private boolean loadColumnStats;
    private final Kryo kryo = new Kryo();
    private boolean isPartitionedTable;
    private boolean finished;

    private final ConcurrentLinkedQueue<TResultBatch> queue = new ConcurrentLinkedQueue<>();

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


        kryo.register(CommonMetadataBean.class);
        if (shouldPlanLocally(dataManifests, loadColumnStats)) {
            return planFileTasksLocally(dataManifests, deleteManifests);
        } else {
            try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.planFileTaskRemotely")) {
                return planFileTasksRemotely(deleteManifests);
            }
        }
    }

    private CloseableIterable<FileScanTask> planFileTasksRemotely(List<ManifestFile> deleteManifests) {
        LOG.info("Planning file tasks remotely for table {}.{}", dbName, tableName);

        String predicate = filter() == Expressions.alwaysTrue() ? "" : SerializationUtil.serializeToBase64(filter());
        ConnectContext currentContext = ConnectContext.get();

        Coordinator coord;
            // TODO(stephen): refactor this
        MetadataCollectJob collectJob = new MetadataCollectJob(catalogName, dbName, tableName,
                snapshot().snapshotId(), predicate, TResultSinkType.METADATA_ICEBERG);
        collectJob.build();
        coord = MetadataExecutor.executeDQL(collectJob);

        // TODO(stephen): use stream to process according to the file content
//        currentContext.setThreadLocalInfo();

        List<DataFile> dataFiles = new ArrayList<>();

        executeInNewThread("xxx", () -> fetchResult(coord));
//        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.deserializedAndBuild")) {
//            dataFiles = deserializedMetadata(resultBatches);
//        }
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.deserMetadata")) {
            while (!finished) {
                if (!queue.isEmpty()) {
                    TResultBatch resultBatch = queue.poll();
                    dataFiles.addAll(deserializedMetadata(resultBatch));
                }
            }
        }

        currentContext.setThreadLocalInfo();
        DeleteFileIndex deleteFileIndex;
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.buildDeleteIndex")) {
            deleteFileIndex = planDeletesLocally(deleteManifests);
        }

        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.toFileTask")) {
            return CloseableIterable.withNoopClose(toFileTasks(dataFiles, deleteFileIndex));
        }
    }

    private boolean fetchResult(Coordinator coord) {
        Long start = System.currentTimeMillis();
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.getRowBatch")) {
            RowBatch batch;
            do {
                batch = coord.getNext();
                if (batch.getBatch() != null) {
                    queue.add(batch.getBatch());
                }
            } while (!batch.isEos());
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            finished = true;
            LOG.error("=================fetch result cost : {}", System.currentTimeMillis() - start);
            QeProcessorImpl.INSTANCE.unregisterQuery(ConnectContext.get().getExecutionId());
            return true;
        }
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
        CloseableIterable<? extends ScanTask> fileTasks = manifestGroup.planFiles();
        return (CloseableIterable<FileScanTask>) fileTasks;
    }

    private List<DataFile> deserializedMetadata(TResultBatch resultBatch) {
        List<DataFile> dataFiles = new ArrayList<>();
        TDeserializer deserializer = new TDeserializer();
//        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.deserializeRowBatch")) {
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
//        }

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
        // FileFormat fileFormat = FileFormat.fromString(thrift.getFile_format());

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
                        FileFormat.PARQUET,
                        partitionData,
                        fileLength,
                        metrics,
                        null,
                        splitOffset,
                        equalityFiledIds,
                        null);
//            dataFile.setSplitOffsets(splitOffset);
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

        CommonMetadataBean bean;
        PartitionData partitionDataTemplate = partitionDataTemplates.computeIfAbsent(specId,
                k -> new PartitionData(spec.partitionType()));

        long start = System.currentTimeMillis();
        Input input = new Input(org.apache.thrift.TBaseHelper.rightSize(thrift.partition_data).array());
        bean = kryo.readObject(input, CommonMetadataBean.class);
        input.close();

        return new PartitionData(partitionDataTemplate, bean.getValues());
    }

    private Metrics buildMetrics(TIcebergMetadata thrift) {
        long recordCount = thrift.getRecord_count();
        // TODO(stephen): hard code and refactor it.
        return new Metrics(recordCount, null, null, null, null, null, null);
    }


    private List<FileScanTask> toFileTasks(
            List<DataFile> dataFiles,
            DeleteFileIndex deleteFileIndex) {
        Map<Integer, String> specStringCache;
        Map<Integer, ResidualEvaluator> residualCache;
        String schemaString = SchemaParser.toJson(tableSchema());
        specStringCache = specCache(PartitionSpecParser::toJson);
        residualCache = specCache(this::newResidualEvaluator);

        List<FileScanTask> scanTasks = new ArrayList<>();
        for (DataFile dataFile : dataFiles) {
            FileScanTask task = toFileTasks(dataFile, deleteFileIndex, schemaString, specStringCache, residualCache);
            scanTasks.add(task);
        }

        return scanTasks;
    }

    private FileScanTask toFileTasks(
            DataFile dataFile,
            DeleteFileIndex deleteFileIndex,
            String schemaString,
            Map<Integer, String> specStringCache,
            Map<Integer, ResidualEvaluator> residualCache) {
        String specString = specStringCache.get(dataFile.specId());
        ResidualEvaluator residuals = residualCache.get(dataFile.specId());

        DeleteFile[] deleteFiles = deleteFileIndex.forDataFile(dataFile);

//        ScanMetricsUtil.fileTask(scanMetrics(), dataFile, deleteFiles);

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
        Map<Integer, R> cache = new HashMap<>();
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
}
