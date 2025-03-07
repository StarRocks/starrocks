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

package com.starrocks.connector.iceberg;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.starrocks.connector.share.iceberg.CommonMetadataBean;
import com.starrocks.connector.share.iceberg.IcebergMetricsBean;
import com.starrocks.jni.connector.ColumnValue;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iceberg.util.ByteBuffers.toByteArray;
import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;

public class IcebergMetadataScanner extends AbstractIcebergMetadataScanner {

    protected static final List<String> SCAN_COLUMNS =
            ImmutableList.of(
                    "snapshot_id",
                    "file_path",
                    "file_ordinal",
                    "file_format",
                    "block_size_in_bytes",
                    "file_size_in_bytes",
                    "record_count",
                    "partition",
                    "key_metadata",
                    "split_offsets",
                    "sort_order_id");

    private static final List<String> STATS_COLUMNS =
            ImmutableList.of(
                    "value_counts",
                    "null_value_counts",
                    "nan_value_counts",
                    "lower_bounds",
                    "upper_bounds",
                    "column_sizes");

    protected static final List<String> SCAN_WITH_STATS_COLUMNS =
            ImmutableList.<String>builder().addAll(SCAN_COLUMNS).addAll(STATS_COLUMNS).build();

    protected static final List<String> DELETE_SCAN_COLUMNS =
            ImmutableList.of(
                    "snapshot_id",
                    "content",
                    "file_path",
                    "file_ordinal",
                    "file_format",
                    "block_size_in_bytes",
                    "file_size_in_bytes",
                    "record_count",
                    "partition",
                    "key_metadata",
                    "split_offsets",
                    "equality_ids");
    protected static final List<String> DELETE_SCAN_WITH_STATS_COLUMNS =
            ImmutableList.<String>builder().addAll(DELETE_SCAN_COLUMNS).addAll(STATS_COLUMNS).build();
    private final String manifestBean;
    private final String predicateInfo;
    private final boolean loadColumnStats;
    private Expression predicate;
    private ManifestFile manifestFile;
    private Kryo kryo;
    private ByteArrayOutputStream stream;
    private Output output;
    private CloseableIterator<? extends ContentFile<?>> reader;

    public IcebergMetadataScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
        this.predicateInfo = params.get("serialized_predicate");
        this.manifestBean = params.get("split_info");
        this.loadColumnStats = Boolean.parseBoolean(params.get("load_column_stats"));
    }

    @Override
    public void doOpen() {
        this.predicate = predicateInfo.isEmpty() ? Expressions.alwaysTrue() : deserializeFromBase64(predicateInfo);
        this.manifestFile = deserializeFromBase64(manifestBean);
        initSerializer();
    }

    @Override
    public int doGetNext() {
        int numRows = 0;
        for (; numRows < getTableSize(); numRows++) {
            if (!reader.hasNext()) {
                break;
            }
            ContentFile<?> file = reader.next();
            for (int i = 0; i < requiredFields.length; i++) {
                Object fieldData = get(requiredFields[i], file);
                if (fieldData == null) {
                    appendData(i, null);
                } else {
                    ColumnValue fieldValue = new IcebergMetadataColumnValue(fieldData);
                    appendData(i, fieldValue);
                }
            }
        }
        return numRows;
    }

    @Override
    public void doClose() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (output != null) {
            output.close();
        }
    }

    @Override
    protected void initReader() {
        Map<Integer, PartitionSpec> specs = table.specs();
        List<String> scanColumns;
        if (manifestFile.content() == ManifestContent.DATA) {
            scanColumns = loadColumnStats ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS;
            reader = ManifestFiles.read(manifestFile, table.io(), specs)
                    .select(scanColumns)
                    .filterRows(predicate)
                    .caseSensitive(false)
                    .iterator();
        } else {
            scanColumns = loadColumnStats ? DELETE_SCAN_WITH_STATS_COLUMNS : DELETE_SCAN_COLUMNS;
            reader = ManifestFiles.readDeleteManifest(manifestFile, table.io(), specs)
                    .select(scanColumns)
                    .filterRows(predicate)
                    .caseSensitive(false)
                    .iterator();
        }
    }

    private void initSerializer() {
        this.kryo = new Kryo();
        this.kryo.register(CommonMetadataBean.class);
        this.kryo.register(IcebergMetricsBean.class);
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        this.stream = new ByteArrayOutputStream();
        this.output = new Output(stream);
    }

    // TODO(stephen): use a unified schema on the com.starrocks.connector.share.iceberg
    private Object get(String columnName, ContentFile<?> file) {
        switch (columnName) {
            case "content":
                return file.content().id();
            case "file_path":
                return file.path().toString();
            case "file_format":
                return file.format().toString();
            case "spec_id":
                return file.specId();
            case "partition_data":
                return table.spec().isPartitioned() ? getPartitionData(file) : null;
            case "record_count":
                return file.recordCount();
            case "file_size_in_bytes":
                return file.fileSizeInBytes();
            case "split_offsets":
                return file.splitOffsets();
            case "sort_id":
                return file.sortOrderId();
            case "equality_ids":
                return file.equalityFieldIds() != null ? file.equalityFieldIds() : null;
            case "file_sequence_number":
                return file.fileSequenceNumber();
            case "data_sequence_number":
                return file.dataSequenceNumber();
            case "column_stats":
                return getIcebergMetrics(file);
            case "key_metadata":
                return file.keyMetadata() == null ? null : toByteArray(file.keyMetadata());
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }

    private byte[] getPartitionData(ContentFile<?> file) {
        stream.reset();
        PartitionSpec partitionSpec = table.specs().get(file.specId());
        StructLike partition = file.partition();
        Class<?>[] classes = partitionSpec.javaClasses();
        Object[] partitionData = new Object[partitionSpec.fields().size()];
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            partitionData[i] = partition.get(i, classes[i]);
        }
        try {
            CommonMetadataBean bean = new CommonMetadataBean();
            bean.setValues(partitionData);
            kryo.writeObject(output, bean);
        } finally {
            output.close();
        }

        return stream.toByteArray();
    }

    private byte[] getIcebergMetrics(ContentFile<?> file) {
        if (!loadColumnStats) {
            return null;
        }

        stream.reset();
        IcebergMetricsBean bean = new IcebergMetricsBean();
        bean.setColumnSizes(file.columnSizes());
        bean.setValueCounts(file.valueCounts());
        bean.setNullValueCounts(file.nullValueCounts());
        bean.setNanValueCounts(file.nanValueCounts());
        if (file.lowerBounds() != null) {
            bean.setLowerBounds(convertByteBufferMap(file.lowerBounds()));
        }
        if (file.upperBounds() != null) {
            bean.setUpperBounds(convertByteBufferMap(file.upperBounds()));
        }

        try {
            kryo.writeObject(output, bean);
        } finally {
            output.close();
        }

        return stream.toByteArray();
    }

    private Map<Integer, byte[]> convertByteBufferMap(Map<Integer, ByteBuffer> byteBufferMap) {
        return byteBufferMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> toByteArray(entry.getValue())));
    }
}
