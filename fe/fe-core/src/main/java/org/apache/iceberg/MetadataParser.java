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
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metadata.MetadataCollectJob;
import com.starrocks.connector.share.iceberg.CommonMetadataBean;
import com.starrocks.connector.share.iceberg.IcebergMetricsBean;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TIcebergMetadata;
import com.starrocks.thrift.TMetadataEntry;
import com.starrocks.thrift.TResultBatch;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.iceberg.BaseFile.EMPTY_PARTITION_DATA;

public class MetadataParser {
    private static final Logger LOG = LogManager.getLogger(MetadataParser.class);
    private final Table table;
    private final Map<Integer, String> specStringCache;
    private final Map<Integer, ResidualEvaluator> residualCache;
    private final String schemaString;
    private final ExecutorService executorService;
    private final ScanMetrics metrics;
    private final DeleteFileIndex deleteFileIndex;
    private final MetadataCollectJob job;
    private final boolean isPartitionedTable;
    private final long liveFilesCount;
    private final ConcurrentLinkedQueue<FileScanTask> fileScanTaskQueue = new ConcurrentLinkedQueue<>();

    private final Map<Integer, PartitionData> partitionDataTemplates = new ConcurrentHashMap<>();
    AtomicBoolean parserFinished = new AtomicBoolean(false);
    private StarRocksConnectorException metadataCollectionException;

    private final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.register(CommonMetadataBean.class);
        kryo.register(IcebergMetricsBean.class);
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        return kryo;
    });

    public MetadataParser(Table table,
                          Map<Integer, String> specStringCache,
                          Map<Integer, ResidualEvaluator> residualCache,
                          ExecutorService executorService,
                          ScanMetrics scanMetrics,
                          DeleteFileIndex deleteFileIndex,
                          MetadataCollectJob job,
                          long liveFilesCount) {
        this.table = table;
        this.job = job;
        this.metrics = scanMetrics;
        this.deleteFileIndex = deleteFileIndex;
        this.isPartitionedTable = table.spec().isPartitioned();
        this.specStringCache = specStringCache;
        this.residualCache = residualCache;
        this.schemaString = SchemaParser.toJson(table.schema());
        this.executorService = executorService;
        this.liveFilesCount = liveFilesCount;
    }

    public void parse() {
        List<Future<Boolean>> futures = new ArrayList<>();
        ConnectContext context = job.getContext();
        Queue<TResultBatch> resultBatchQueue = job.getResultQueue();
        while (context.getState().isRunning() || !resultBatchQueue.isEmpty()) {
            if (!resultBatchQueue.isEmpty()) {
                TResultBatch resultBatch = resultBatchQueue.poll();
                futures.add(executorService.submit(() -> {
                    List<FileScanTask> scanTasks = buildFileScanTask(resultBatch);
                    return fileScanTaskQueue.addAll(scanTasks);
                }));
            }
        }

        if (context.getState().isError()) {
            String collectErrorMsg = "Failed to execute metadata collection job. ";
            String errMsgToClient = job.getMetadataJobCoord().getExecStatus().getErrorMsg();
            this.metadataCollectionException = new StarRocksConnectorException(collectErrorMsg + errMsgToClient);
            fileScanTaskQueue.clear();
            return;
        }

        try {
            for (Future<Boolean> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            String parserErrorMsg = "Failed to parse iceberg file scan task. ";
            LOG.error(parserErrorMsg, e);
            this.metadataCollectionException = new StarRocksConnectorException(parserErrorMsg + e.getMessage());
            fileScanTaskQueue.clear();
        } finally {
            while (true) {
                if (fileScanTaskQueue.isEmpty()) {
                    parserFinished.set(true);
                    break;
                }

                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    LOG.error(e);
                }
            }
        }

        metrics.skippedDataFiles().increment(liveFilesCount - metrics.resultDataFiles().value());
    }

    private List<FileScanTask> buildFileScanTask(TResultBatch resultBatch) {
        List<DataFile> dataFiles = deserializedMetadata(resultBatch);
        return toFileScanTasks(dataFiles);
    }

    private List<FileScanTask> toFileScanTasks(List<DataFile> dataFiles) {
        List<FileScanTask> scanTasks = new ArrayList<>();
        for (DataFile dataFile : dataFiles) {
            FileScanTask task = toFileScanTask(dataFile);
            scanTasks.add(task);
        }

        return scanTasks;
    }

    private FileScanTask toFileScanTask(DataFile dataFile) {
        String specString = specStringCache.get(dataFile.specId());
        ResidualEvaluator residuals = residualCache.get(dataFile.specId());

        DeleteFile[] deleteFiles = deleteFileIndex.forDataFile(dataFile);

        ScanMetricsUtil.fileTask(metrics, dataFile, deleteFiles);

        return new BaseFileScanTask(
                dataFile,
                deleteFiles,
                schemaString,
                specString,
                residuals);
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
        Metrics metrics = buildMetrics(thrift);

        List<Long> splitOffsets = thrift.getSplit_offsets();
        Integer sortId = thrift.getSort_id();

        int[] equalityFiledIds = thrift.isSetEquality_ids() ? ArrayUtil.toIntArray(thrift.getEquality_ids()) : null;
        if (content == FileContent.DATA) {
            GenericDataFile dataFile = new GenericDataFile(
                    specId,
                    filePath,
                    fileFormat,
                    partitionData,
                    fileLength,
                    metrics,
                    null,
                    splitOffsets,
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
                    splitOffsets,
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
        PartitionSpec spec = table.specs().get(specId);
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

    private Metrics buildMetrics(TIcebergMetadata thrift) throws StarRocksConnectorException {
        long recordCount = thrift.getRecord_count();
        try (Input input = new Input(org.apache.thrift.TBaseHelper.rightSize(thrift.column_stats).array())) {
            IcebergMetricsBean bean = kryoThreadLocal.get().readObject(input, IcebergMetricsBean.class);
            Map<Integer, Long> columnSizes = bean.getColumnSizes();
            Map<Integer, Long> valueCounts = bean.getValueCounts();
            Map<Integer, Long> nullValueCounts = bean.getNullValueCounts();
            Map<Integer, Long> nanValueCounts = bean.getNanValueCounts();
            Map<Integer, ByteBuffer> lowerBounds = null;
            Map<Integer, ByteBuffer> upperBounds = null;
            if (bean.getLowerBounds() != null) {
                lowerBounds = bean.getLowerBounds().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> ByteBuffer.wrap(entry.getValue())));
            }
            if (bean.getUpperBounds() != null) {
                upperBounds = bean.getUpperBounds().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> ByteBuffer.wrap(entry.getValue())));
            }
            return new Metrics(recordCount, columnSizes, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
        } catch (Exception e) {
            LOG.warn("build metrics failed", e);
            throw new StarRocksConnectorException("Failed to build iceberg metrics. msg: " + e.getMessage());
        }
    }

    public ConcurrentLinkedQueue<FileScanTask> getFileScanTaskQueue() {
        return fileScanTaskQueue;
    }

    public boolean parseFinish() {
        return parserFinished.get();
    }

    public StarRocksConnectorException getMetadataParserException() {
        return metadataCollectionException;
    }

    public boolean parseError() {
        if (metadataCollectionException != null) {
            LOG.error("not null");
        }
        return metadataCollectionException != null;
    }

    public void clear() {
        fileScanTaskQueue.clear();
    }
}
