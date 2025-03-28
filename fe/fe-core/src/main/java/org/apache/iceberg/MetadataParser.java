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
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.thrift.TIcebergMetadata;
import com.starrocks.thrift.TMetadataEntry;
import com.starrocks.thrift.TResultBatch;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

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
                    List<FileScanTask> scanTasks = parse(resultBatch);
                    return fileScanTaskQueue.addAll(scanTasks);
                }));
            }
        }

        if (context.getState().isError()) {
            String collectErrorMsg = "Failed to execute metadata collection job. ";
            Coordinator coord = job.getMetadataJobCoord();
            String errMsgToClient;
            if (coord == null) {
                errMsgToClient = context.getState().getErrorMessage();
            } else {
                errMsgToClient = coord.getExecStatus().getErrorMsg();
            }

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
                    LOG.error(e.getMessage(), e);
                }
            }
        }

        metrics.skippedDataFiles().increment(liveFilesCount - metrics.resultDataFiles().value());
    }

    private List<FileScanTask> parse(TResultBatch resultBatch) throws TTransportException {
        List<DataFile> dataFiles = buildIcebergDataFile(resultBatch);
        return dataFiles.stream().map(this::createFileScanTasks).collect(Collectors.toList());
    }

    private FileScanTask createFileScanTasks(DataFile dataFile) {
        String specString = specStringCache.get(dataFile.specId());
        ResidualEvaluator residuals = residualCache.get(dataFile.specId());

        // find matching delete files for data file
        DeleteFile[] deleteFiles = deleteFileIndex.forDataFile(dataFile);

        ScanMetricsUtil.fileTask(metrics, dataFile, deleteFiles);

        return new BaseFileScanTask(
                dataFile,
                deleteFiles,
                schemaString,
                specString,
                residuals);
    }

    private List<DataFile> buildIcebergDataFile(TResultBatch resultBatch) throws TTransportException {
        List<DataFile> dataFiles = new ArrayList<>();
        TDeserializer deserializer = ConfigurableSerDesFactory.getTDeserializer();
        for (ByteBuffer bb : resultBatch.rows) {
            TMetadataEntry metadataEntry = deserializeToMetadataThrift(deserializer, bb);
            DataFile baseFile = (DataFile) parseThriftToIcebergDataFile(metadataEntry);
            dataFiles.add(baseFile);
        }

        return dataFiles;
    }

    private TMetadataEntry deserializeToMetadataThrift(TDeserializer deserializer, ByteBuffer bb) {
        TMetadataEntry metadataEntry = new TMetadataEntry();
        byte[] bytes = new byte[bb.limit() - bb.position()];
        bb.get(bytes);
        try {
            deserializer.deserialize(metadataEntry, bytes);
        } catch (TException e) {
            LOG.error("Failed to deserialize resultBatch to thrift", e);
            throw new RuntimeException(e);
        }
        return metadataEntry;
    }

    // TODO(stephen): support distributed plan with delete file if necessary.
    private ContentFile<?> parseThriftToIcebergDataFile(TMetadataEntry entry) {
        if (!entry.isSetIceberg_metadata()) {
            throw new StarRocksConnectorException("not found iceberg metadata in the metadata collect job result");
        }

        TIcebergMetadata thrift = entry.getIceberg_metadata();

        // build file content
        FileContent content = FileContent.values()[thrift.content];

        // build file path
        String filePath = thrift.getFile_path();

        // build file format
        FileFormat fileFormat = FileFormat.fromString(thrift.getFile_format());

        // build spec id
        int specId = thrift.getSpec_id();

        // build partition data
        PartitionData partitionData = buildPartitionData(thrift);

        // build file size
        long fileLength = thrift.getFile_size_in_bytes();

        // build split offsets
        List<Long> splitOffsets = thrift.getSplit_offsets();

        // build sort id
        Integer sortId = thrift.getSort_id();

        // build iceberg metrics
        Metrics metrics = buildMetrics(thrift);

        // build equality field id
        int[] equalityFieldIds = thrift.isSetEquality_ids() ? ArrayUtil.toIntArray(thrift.getEquality_ids()) : null;

        // build key metadata
        ByteBuffer keyMetadata = thrift.isSetKey_metadata() ? ByteBuffer.wrap(thrift.getKey_metadata()) : null;

        BaseFile<?> baseFile;
        // TODO(stephen): add keyMetadata field
        if (content == FileContent.DATA) {
            baseFile = new GenericDataFile(
                    specId,
                    filePath,
                    fileFormat,
                    partitionData,
                    fileLength,
                    metrics,
                    keyMetadata,
                    splitOffsets,
                    null);
        } else {
            baseFile = new GenericDeleteFile(
                    specId,
                    content,
                    filePath,
                    fileFormat,
                    partitionData,
                    fileLength,
                    metrics,
                    equalityFieldIds,
                    sortId,
                    splitOffsets,
                    keyMetadata
            );
        }

        if (thrift.isSetFile_sequence_number()) {
            baseFile.setFileSequenceNumber(thrift.getFile_sequence_number());
        }

        if (thrift.isSetData_sequence_number()) {
            baseFile.setDataSequenceNumber(thrift.getData_sequence_number());
        }

        return baseFile;
    }

    private PartitionData buildPartitionData(TIcebergMetadata thrift) {
        if (!isPartitionedTable) {
            return EMPTY_PARTITION_DATA;
        }

        Integer specId = thrift.getSpec_id();
        PartitionSpec spec = table.specs().get(specId);
        PartitionData partitionDataTemplate = partitionDataTemplates.computeIfAbsent(specId,
                ignore -> new PartitionData(spec.partitionType()));

        CommonMetadataBean bean;
        try (Input input = new Input(org.apache.thrift.TBaseHelper.rightSize(thrift.partition_data).array())) {
            bean = kryoThreadLocal.get().readObject(input, CommonMetadataBean.class);
        }

        return new PartitionData(partitionDataTemplate, bean.getValues());
    }

    private Metrics buildMetrics(TIcebergMetadata thrift) throws StarRocksConnectorException {
        long recordCount = thrift.getRecord_count();
        if (!thrift.isSetColumn_stats()) {
            return new Metrics(recordCount, null, null, null, null, null, null);
        }

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
        return metadataCollectionException != null;
    }

    public void clear() {
        fileScanTaskQueue.clear();
    }
}
