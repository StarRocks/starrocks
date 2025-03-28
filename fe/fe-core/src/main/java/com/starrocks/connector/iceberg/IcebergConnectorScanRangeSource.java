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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.ConnectorScanRangeSource;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TIcebergDeleteFile;
import com.starrocks.thrift.TIcebergFileContent;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.catalog.IcebergTable.DATA_SEQUENCE_NUMBER;
import static com.starrocks.catalog.IcebergTable.SPEC_ID;
import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

public class IcebergConnectorScanRangeSource extends ConnectorScanRangeSource {
    private static final Logger LOG = LogManager.getLogger(IcebergConnectorScanRangeSource.class);
    private final IcebergTable table;
    private final TupleDescriptor desc;
    private final IcebergMORParams morParams;
    private final RemoteFileInfoSource remoteFileInfoSource;
    private final AtomicLong partitionIdGen = new AtomicLong(0L);

    private final Map<Long, DescriptorTable.ReferencedPartitionInfo> referencedPartitions = new HashMap<>();
    private final Map<StructLike, Long> partitionKeyToId = Maps.newHashMap();

    // spec_id -> Map(partition_field_index_in_partitionSpec, PartitionField)
    private final Map<Integer, BiMap<Integer, PartitionField>> indexToFieldCache = Maps.newHashMap();

    // spec_id -> partitionSlotIds
    private final Map<Integer, List<Integer>> partitionSlotIdsCache = Maps.newHashMap();

    // spec_id -> partitionFieldIndexes
    private final Map<Integer, List<Integer>> indexesCache = Maps.newHashMap();
    private final Set<String> seenEqDeleteFiles = new HashSet<>();
    private final List<Integer> extendedColumnSlotIds = new ArrayList<>();

    public IcebergConnectorScanRangeSource(IcebergTable table,
                                           RemoteFileInfoSource remoteFileInfoSource,
                                           IcebergMORParams morParams,
                                           TupleDescriptor desc) {
        this.table = table;
        this.remoteFileInfoSource = remoteFileInfoSource;
        this.morParams = morParams;
        this.desc = desc;
    }

    @Override
    public boolean sourceHasMoreOutput() {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.getScanFiles")) {
            return remoteFileInfoSource.hasMoreOutput();
        }
    }

    @Override
    public List<TScanRangeLocations> getSourceOutputs(int maxSize) {
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ICEBERG.getScanFiles")) {
            List<TScanRangeLocations> res = new ArrayList<>();
            while (hasMoreOutput() && res.size() < maxSize) {
                RemoteFileInfo remoteFileInfo = remoteFileInfoSource.getOutput();
                IcebergRemoteFileInfo icebergRemoteFileInfo = remoteFileInfo.cast();
                FileScanTask fileScanTask = icebergRemoteFileInfo.getFileScanTask();
                res.addAll(toScanRanges(fileScanTask));
            }
            return res;
        }
    }

    private List<TScanRangeLocations> toScanRanges(FileScanTask fileScanTask) {
        long partitionId;
        try {
            partitionId = addPartition(fileScanTask);
        } catch (AnalysisException e) {
            throw new StarRocksConnectorException("add scan range partition failed", e);
        }

        try {
            if (morParams.getScanTaskType() != IcebergMORParams.ScanTaskType.EQ_DELETE) {
                return buildScanRanges(fileScanTask, partitionId);
            } else {
                return buildDeleteFileScanRanges(fileScanTask, partitionId);
            }
        } catch (Exception e) {
            LOG.error("build scan range failed", e);
            throw new StarRocksConnectorException("build scan range failed", e);
        }
    }

    private List<TScanRangeLocations> buildScanRanges(FileScanTask task, Long partitionId) throws AnalysisException {
        THdfsScanRange hdfsScanRange = buildScanRange(task, task.file(), partitionId);

        List<TIcebergDeleteFile> posDeleteFiles = new ArrayList<>();
        for (DeleteFile deleteFile : task.deletes()) {
            FileContent content = deleteFile.content();
            if (content == FileContent.EQUALITY_DELETES) {
                continue;
            }

            TIcebergDeleteFile target = new TIcebergDeleteFile();
            target.setFull_path(deleteFile.path().toString());
            target.setFile_content(TIcebergFileContent.POSITION_DELETES);
            target.setLength(deleteFile.fileSizeInBytes());
            posDeleteFiles.add(target);
        }

        if (!posDeleteFiles.isEmpty()) {
            hdfsScanRange.setDelete_files(posDeleteFiles);
        }

        return Lists.newArrayList(buildTScanRangeLocations(hdfsScanRange));
    }

    private List<TScanRangeLocations> buildDeleteFileScanRanges(FileScanTask task, Long partitionId) throws AnalysisException {
        List<TScanRangeLocations> res = new ArrayList<>();
        for (DeleteFile file : task.deletes()) {
            if (file.content() != FileContent.EQUALITY_DELETES) {
                continue;
            }

            if (!seenEqDeleteFiles.contains(file.path().toString())) {
                THdfsScanRange hdfsScanRange = buildScanRange(task, file, partitionId);
                res.add(buildTScanRangeLocations(hdfsScanRange));
                seenEqDeleteFiles.add(file.path().toString());
            }
        }
        return res;
    }
    protected THdfsScanRange buildScanRange(FileScanTask task, ContentFile<?> file, Long partitionId) throws AnalysisException {
        DescriptorTable.ReferencedPartitionInfo referencedPartitionInfo = referencedPartitions.get(partitionId);
        THdfsScanRange hdfsScanRange = new THdfsScanRange();
        if (file.path().toString().startsWith(table.getTableLocation())) {
            hdfsScanRange.setRelative_path(file.path().toString().substring(table.getTableLocation().length()));
        } else {
            hdfsScanRange.setFull_path(file.path().toString());
        }

        hdfsScanRange.setOffset(file.content() == FileContent.DATA ? task.start() : 0);
        hdfsScanRange.setLength(file.content() == FileContent.DATA ? task.length() : file.fileSizeInBytes());

        if (!partitionSlotIdsCache.containsKey(file.specId())) {
            hdfsScanRange.setPartition_id(-1);
        } else {
            hdfsScanRange.setPartition_id(partitionId);
            hdfsScanRange.setIdentity_partition_slot_ids(partitionSlotIdsCache.get(file.specId()));
        }

        hdfsScanRange.setFile_length(file.fileSizeInBytes());
        // Iceberg data file cannot be overwritten
        hdfsScanRange.setModification_time(0);
        hdfsScanRange.setFile_format(IcebergApiConverter.getHdfsFileFormat(file.format()).toThrift());

        THdfsPartition tPartition = new THdfsPartition();
        tPartition.setPartition_key_exprs(referencedPartitionInfo.getKey().getKeys().stream()
                .map(Expr::treeToThrift)
                .collect(Collectors.toList()));

        hdfsScanRange.setPartition_value(tPartition);
        hdfsScanRange.setTable_id(table.getId());

        // fill extended column value
        List<SlotDescriptor> slots = desc.getSlots();
        Map<Integer, TExpr> extendedColumns = new HashMap<>();
        for (SlotDescriptor slot : slots) {
            String name = slot.getColumn().getName();
            if (name.equalsIgnoreCase(DATA_SEQUENCE_NUMBER) || name.equalsIgnoreCase(SPEC_ID)) {
                LiteralExpr value;
                if (name.equalsIgnoreCase(DATA_SEQUENCE_NUMBER)) {
                    value = LiteralExpr.create(String.valueOf(file.dataSequenceNumber()), Type.BIGINT);
                } else {
                    value = LiteralExpr.create(String.valueOf(file.specId()), Type.INT);
                }

                extendedColumns.put(slot.getId().asInt(), value.treeToThrift());
                if (!extendedColumnSlotIds.contains(slot.getId().asInt())) {
                    extendedColumnSlotIds.add(slot.getId().asInt());
                }
            }
        }

        hdfsScanRange.setExtended_columns(extendedColumns);
        return hdfsScanRange;
    }

    protected TScanRangeLocations buildTScanRangeLocations(THdfsScanRange hdfsScanRange) {
        TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
        TScanRange scanRange = new TScanRange();
        scanRange.setHdfs_scan_range(hdfsScanRange);
        scanRangeLocations.setScan_range(scanRange);

        TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
        scanRangeLocations.addToLocations(scanRangeLocation);
        return scanRangeLocations;
    }

    private long addPartition(FileScanTask task) throws AnalysisException {
        PartitionSpec spec = task.spec();

        StructLike partition = task.partition();
        if (partitionKeyToId.containsKey(partition)) {
            return partitionKeyToId.get(partition);
        }

        BiMap<Integer, PartitionField> indexToPartitionField = indexToFieldCache.computeIfAbsent(spec.specId(),
                ignore -> getIdentityPartitions(task.spec()));

        List<Integer> partitionFieldIndexes = indexesCache.computeIfAbsent(spec.specId(),
                ignore -> getPartitionFieldIndexes(spec, indexToPartitionField));
        PartitionKey partitionKey = getPartitionKey(partition, task.spec(), partitionFieldIndexes, indexToPartitionField);
        long partitionId = partitionIdGen.getAndIncrement();

        Path filePath = new Path(URLDecoder.decode(task.file().path().toString(), StandardCharsets.UTF_8));
        DescriptorTable.ReferencedPartitionInfo referencedPartitionInfo =
                new DescriptorTable.ReferencedPartitionInfo(partitionId, partitionKey,
                        filePath.getParent().toString());

        partitionKeyToId.put(partition, partitionId);
        referencedPartitions.put(partitionId, referencedPartitionInfo);
        if (!partitionSlotIdsCache.containsKey(spec.specId())) {
            partitionSlotIdsCache.put(spec.specId(), buildPartitionSlotIds(task.spec()));
        }

        return partitionId;
    }

    private List<Integer> buildPartitionSlotIds(PartitionSpec spec) {
        return spec.fields().stream()
                .map(x -> desc.getColumnSlot(x.name()))
                .filter(Objects::nonNull)
                .map(SlotDescriptor::getId)
                .map(SlotId::asInt)
                .collect(Collectors.toList());
    }

    private List<Integer> getPartitionFieldIndexes(PartitionSpec spec, BiMap<Integer, PartitionField> indexToField) {
        return spec.fields().stream()
                .filter(x -> desc.getColumnSlot(x.name()) != null)
                .map(x -> indexToField.inverse().get(x))
                .collect(Collectors.toList());
    }

    public BiMap<Integer, PartitionField> getIdentityPartitions(PartitionSpec partitionSpec) {
        // TODO: expose transform information in Iceberg library
        BiMap<Integer, PartitionField> columns = HashBiMap.create();
        if (!ConnectContext.get().getSessionVariable().getEnableIcebergIdentityColumnOptimize()) {
            return columns;
        }
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (field.transform().isIdentity()) {
                columns.put(i, field);
            }
        }
        return columns;
    }

    private PartitionKey getPartitionKey(StructLike partition, PartitionSpec spec, List<Integer> indexes,
                                           BiMap<Integer, PartitionField> indexToField) throws AnalysisException {
        List<String> partitionValues = new ArrayList<>();
        List<Column> cols = new ArrayList<>();
        indexes.forEach((index) -> {
            PartitionField field = indexToField.get(index);
            int id = field.sourceId();
            org.apache.iceberg.types.Type type = spec.schema().findType(id);
            Class<?> javaClass = type.typeId().javaClass();

            String partitionValue;

            // currently starrocks date literal only support local datetime
            if (type.equals(Types.TimestampType.withZone())) {
                Long value = PartitionUtil.getPartitionValue(partition, index, javaClass);
                if (value == null) {
                    partitionValue = "null";
                } else {
                    partitionValue = ChronoUnit.MICROS.addTo(Instant.ofEpochSecond(0).atZone(
                            TimeUtils.getTimeZone().toZoneId()), value).toLocalDateTime().toString();
                }
            } else {
                partitionValue = field.transform().toHumanString(type, PartitionUtil.getPartitionValue(
                        partition, index, javaClass));
            }

            partitionValues.add(partitionValue);

            cols.add(table.getColumn(field.name()));
        });

        return PartitionUtil.createPartitionKey(partitionValues, cols, Table.TableType.ICEBERG);
    }

    public List<Integer> getExtendedColumnSlotIds() {
        return extendedColumnSlotIds;
    }

    public int selectedPartitionCount() {
        return partitionKeyToId.size();
    }

    public Set<String> getSeenEqDeleteFiles() {
        return seenEqDeleteFiles;
    }
}
