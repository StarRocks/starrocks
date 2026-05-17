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

import com.google.common.collect.ImmutableList;
import com.starrocks.connector.share.iceberg.IcebergPartitionUtils;
import com.starrocks.connector.share.iceberg.ManifestFileBean;
import com.starrocks.connector.share.iceberg.PartitionStatsSplitBean;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.IcebergPartitionStatsHelper;
import org.apache.iceberg.ManifestEntryScanHelper;
import org.apache.iceberg.ManifestEntryScanHelper.LiveEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.StructProjection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;

public class IcebergPartitionsTableScanner extends AbstractIcebergMetadataScanner {
    private static final Logger LOG = LogManager.getLogger(IcebergPartitionsTableScanner.class);
    protected static final List<String> SCAN_COLUMNS =
            ImmutableList.of("content", "partition", "file_size_in_bytes", "record_count");

    private final String manifestBean;
    private final String predicateInfo;
    private ManifestFile manifestFile;
    private CloseableIterator<LiveEntry> entryReader;
    private List<PartitionField> partitionFields;
    private Integer spedId;
    private Schema schema;
    private GenericRecord reusedRecord;
    private Iterator<PartitionStats> statsIterator;
    private boolean usingStatsFile;
    private Types.StructType unifiedPartitionType;
    private Expression predicate;
    private boolean predicateAlwaysTrue;
    private Evaluator evaluator;
    // Maps specId -> reusable StructProjection wrapping a per-spec partition into a
    // unified-partition-type-shaped StructLike for the evaluator. Not thread-safe:
    // wrap() mutates the projection. Safe under one-scanner-per-thread.
    private final Map<Integer, StructProjection> manifestProjectionCache = new HashMap<>();

    public IcebergPartitionsTableScanner(int fetchSize, Map<String, String> params) {
        super(fetchSize, params);
        this.manifestBean = params.get("split_info");
        this.predicateInfo = params.getOrDefault("serialized_predicate", "");
    }

    @Override
    public void doOpen() {
        Object split = deserializeFromBase64(manifestBean);
        this.schema = table.schema();
        this.partitionFields = IcebergPartitionUtils.getAllPartitionFields(table);
        this.reusedRecord = GenericRecord.create(getResultType());
        this.unifiedPartitionType = Partitioning.partitionType(table);
        this.predicate = predicateInfo.isEmpty()
                ? Expressions.alwaysTrue()
                : deserializeFromBase64(predicateInfo);
        this.predicateAlwaysTrue = predicate == Expressions.alwaysTrue();
        if (!predicateAlwaysTrue) {
            this.evaluator = new Evaluator(unifiedPartitionType, predicate, false);
        }

        if (split instanceof PartitionStatsSplitBean) {
            this.usingStatsFile = true;
            PartitionStatsSplitBean statsSplit = (PartitionStatsSplitBean) split;
            LOG.debug("Iceberg partitions scanner using stats file. mode={}, stats_snapshot={}, target_snapshot={}, "
                            + "has_incremental={}",
                    statsSplit.getMode(), statsSplit.getStatsSnapshotId(), statsSplit.getTargetSnapshotId(),
                    statsSplit.hasIncrementalManifests());
            initStatsIterator(statsSplit);
        } else {
            this.manifestFile = (ManifestFile) split;
            this.spedId = manifestFile.partitionSpecId();
        }
    }

    @Override
    public int doGetNext() {
        int numRows = 0;
        while (numRows < getTableSize()) {
            if (usingStatsFile) {
                if (statsIterator == null || !statsIterator.hasNext()) {
                    break;
                }
                PartitionStats stat = statsIterator.next();
                if (!matchesPredicate(stat.specId(), stat.partition(), unifiedPartitionType)) {
                    continue;
                }
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = getFromStats(requiredFields[i], stat);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        ColumnValue fieldValue = new IcebergMetadataColumnValue(fieldData, timezone);
                        appendData(i, fieldValue);
                    }
                }
            } else {
                if (entryReader == null || !entryReader.hasNext()) {
                    break;
                }
                LiveEntry entry = entryReader.next();
                ContentFile<?> file = entry.file();
                PartitionSpec entrySpec = table.specs().get(file.specId());
                if (entrySpec == null
                        || !matchesPredicate(file.specId(), file.partition(), entrySpec.partitionType())) {
                    continue;
                }
                Snapshot snapshot = table.snapshot(entry.snapshotId());
                Long lastUpdatedAt = snapshot != null ? snapshot.timestampMillis() : null;
                Long lastUpdatedSnapshotId = snapshot != null ? snapshot.snapshotId() : null;
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = get(requiredFields[i], file, lastUpdatedAt, lastUpdatedSnapshotId);
                    if (fieldData == null) {
                        appendData(i, null);
                    } else {
                        ColumnValue fieldValue = new IcebergMetadataColumnValue(fieldData, timezone);
                        appendData(i, fieldValue);
                    }
                }
            }
            numRows++;
        }
        return numRows;
    }

    /** Not thread-safe; assumes one scanner instance per thread and synchronous wrap+eval. */
    private boolean matchesPredicate(int specId, StructLike partition, Types.StructType sourceType) {
        if (predicateAlwaysTrue) {
            return true;
        }
        StructLike evalData = partition;
        if (!unifiedPartitionType.equals(sourceType)) {
            // First arg = type of the row being wrapped (per-spec partition).
            // Second arg = projected/result type the Evaluator is bound to (unified).
            StructProjection projection = manifestProjectionCache.computeIfAbsent(
                    specId,
                    id -> StructProjection.createAllowMissing(sourceType, unifiedPartitionType));
            evalData = projection.wrap(partition);
        }
        return evaluator.eval(evalData);
    }

    @Override
    public void doClose() throws IOException {
        if (entryReader != null) {
            entryReader.close();
        }
        reusedRecord = null;
    }

    @Override
    protected void initReader() {
        if (usingStatsFile) {
            return;
        }
        Map<Integer, PartitionSpec> specs = table.specs();
        entryReader = ManifestEntryScanHelper.liveEntries(manifestFile, fileIO, specs, SCAN_COLUMNS).iterator();
    }

    private Types.StructType getResultType() {
        List<Types.NestedField> fields = new ArrayList<>();
        for (PartitionField partitionField : partitionFields) {
            int id = partitionField.fieldId();
            String name = partitionField.name();
            Type type = partitionField.transform().getResultType(schema.findType(partitionField.sourceId()));
            Types.NestedField nestedField = Types.NestedField.optional(id, name, type);
            fields.add(nestedField);
        }
        return Types.StructType.of(fields);
    }

    private Object get(String columnName, ContentFile<?> file, Long lastUpdatedAt, Long lastUpdatedSnapshotId) {
        FileContent content = file.content();
        PartitionSpec spec = table.specs().get(file.specId());
        PartitionData partitionData = (PartitionData) file.partition();
        switch (columnName) {
            case "partition_value":
                return getPartitionValues(partitionData, spec.partitionType());
            case "spec_id":
                return spedId;
            case "record_count":
                return content == FileContent.DATA ? file.recordCount() : 0;
            case "file_count":
                return content == FileContent.DATA ? 1L : 0L;
            case "total_data_file_size_in_bytes":
                return content == FileContent.DATA ? file.fileSizeInBytes() : 0;
            case "position_delete_record_count":
                return content == FileContent.POSITION_DELETES ? file.recordCount() : 0;
            case "position_delete_file_count":
                return content == FileContent.POSITION_DELETES ? 1L : 0L;
            case "equality_delete_record_count":
                return content == FileContent.EQUALITY_DELETES ? file.recordCount() : 0;
            case "equality_delete_file_count":
                return content == FileContent.EQUALITY_DELETES ? 1L : 0L;
            case "last_updated_at":
                return lastUpdatedAt;
            case "last_updated_snapshot_id":
                return lastUpdatedSnapshotId;
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }

    private Object getFromStats(String columnName, PartitionStats stat) {
        switch (columnName) {
            case "partition_value":
                return getPartitionValues(stat.partition(), unifiedPartitionType);
            case "spec_id":
                return stat.specId();
            case "record_count":
                return stat.dataRecordCount();
            case "file_count":
                return (long) stat.dataFileCount();
            case "total_data_file_size_in_bytes":
                return stat.totalDataFileSizeInBytes();
            case "position_delete_record_count":
                return stat.positionDeleteRecordCount();
            case "position_delete_file_count":
                return (long) stat.positionDeleteFileCount();
            case "equality_delete_record_count":
                return stat.equalityDeleteRecordCount();
            case "equality_delete_file_count":
                return (long) stat.equalityDeleteFileCount();
            case "last_updated_at":
                return stat.lastUpdatedAt();
            case "last_updated_snapshot_id":
                return stat.lastUpdatedSnapshotId();
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }

    private Object getPartitionValues(StructLike partitionData, Types.StructType partitionType) {
        List<Types.NestedField> fileFields = partitionType.fields();
        Map<Integer, Integer> fieldIdToPos = new HashMap<>();
        for (int i = 0; i < fileFields.size(); i++) {
            fieldIdToPos.put(fileFields.get(i).fieldId(), i);
        }

        for (PartitionField partitionField : partitionFields) {
            Integer fieldId = partitionField.fieldId();
            String name = partitionField.name();
            if (fieldIdToPos.containsKey(fieldId)) {
                int pos = fieldIdToPos.get(fieldId);
                Type fieldType = fileFields.get(pos).type();
                Object partitionValue = partitionData.get(pos, Object.class);
                if (partitionField.transform().isIdentity() && Types.TimestampType.withZone().equals(fieldType)) {
                    partitionValue = partitionValue == null ? null : ((long) partitionValue) / 1000;
                }
                reusedRecord.setField(name, partitionValue);
            } else {
                reusedRecord.setField(name, null);
            }
        }
        return reusedRecord;
    }

    private void initStatsIterator(PartitionStatsSplitBean statsSplit) {
        long startMs = System.currentTimeMillis();
        try {
            PartitionMap<PartitionStats> statsMap = IcebergPartitionStatsHelper.readPartitionStatsFileAsMap(
                    table, statsSplit.getStatsFilePath(), unifiedPartitionType);
            int baseCount = statsMap.size();
            if (statsSplit.isBaseMode()) {
                LOG.debug("Iceberg partitions stats base only. base_partitions={}, elapsed_ms={}",
                        baseCount, System.currentTimeMillis() - startMs);
                this.statsIterator = statsMap.values().iterator();
                return;
            }

            if (statsSplit.hasIncrementalManifests()) {
                List<ManifestFile> manifests = new ArrayList<>();
                for (ManifestFileBean bean : statsSplit.getIncrementalManifests()) {
                    manifests.add(bean);
                }
                LOG.debug("Iceberg partitions stats incremental apply. manifests_to_apply={}", manifests.size());
                long deltaReadStartMs = System.currentTimeMillis();
                PartitionMap<PartitionStats> incrementalMap =
                        IcebergPartitionStatsHelper.computeStatsFromManifests(
                                table, manifests, unifiedPartitionType);
                long deltaReadMs = System.currentTimeMillis() - deltaReadStartMs;
                int incrementalCount = incrementalMap.size();
                long mergeStartMs = System.currentTimeMillis();
                IcebergPartitionStatsHelper.mergeIncrementalStats(statsMap, incrementalMap);
                long mergeMs = System.currentTimeMillis() - mergeStartMs;
                LOG.debug("Iceberg partitions stats incremental applied. base_partitions={}, incremental_partitions={}, "
                                + "merged_partitions={}, delta_read_ms={}, merge_ms={}, elapsed_ms={}",
                        baseCount, incrementalCount, statsMap.size(),
                        deltaReadMs, mergeMs, System.currentTimeMillis() - startMs);
            } else {
                LOG.debug("Iceberg partitions stats file only. base_partitions={}, elapsed_ms={}",
                        baseCount, System.currentTimeMillis() - startMs);
            }
            this.statsIterator = statsMap.values().iterator();
        } catch (Exception e) {
            throw new RuntimeException("Failed to read partition stats", e);
        }
    }

}
