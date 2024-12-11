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

package com.starrocks.planner;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
<<<<<<< HEAD
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.Connector;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TIcebergDeleteFile;
import com.starrocks.thrift.TIcebergFileContent;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
=======
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.iceberg.IcebergConnectorScanRangeSource;
import com.starrocks.connector.iceberg.IcebergGetRemoteFilesParams;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergRemoteSourceTrigger;
import com.starrocks.connector.iceberg.QueueIcebergRemoteFileInfoSource;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.THdfsScanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Deque;
import java.util.List;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

public class IcebergScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

<<<<<<< HEAD
    private IcebergTable icebergTable; // table definition in starRocks

    private HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();

    private List<TScanRangeLocations> result = new ArrayList<>();

    // Scalar operators to filter iceberg metadata during plan files
    private ScalarOperator predicate = null;

    private Set<String> equalityDeleteColumns = new HashSet<>();

    private final HashMultimap<String, Long> hostToBeId = HashMultimap.create();
    private long totalBytes = 0;

    private boolean isFinalized = false;
    private CloudConfiguration cloudConfiguration = null;
    private final List<Integer> deleteColumnSlotIds = new ArrayList<>();
    private final TupleDescriptor equalityDeleteTupleDesc;

    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName, TupleDescriptor equalityDeleteTupleDesc) {
        super(id, desc, planNodeName);
        this.icebergTable = (IcebergTable) desc.getTable();
        this.equalityDeleteTupleDesc = equalityDeleteTupleDesc;
        setupCloudCredential();
    }

=======
    protected final IcebergTable icebergTable;
    private final HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();
    private ScalarOperator icebergJobPlanningPredicate = null;
    private CloudConfiguration cloudConfiguration = null;
    protected Optional<Long> snapshotId;
    private IcebergConnectorScanRangeSource scanRangeSource = null;
    private final List<IcebergMORParams> tableFullMORParams;
    private final IcebergMORParams morParams;
    private int selectedPartitionCount = -1;

    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName,
                           List<IcebergMORParams> tableFullMORParams, IcebergMORParams morParams) {
        super(id, desc, planNodeName);
        this.icebergTable = (IcebergTable) desc.getTable();
        this.tableFullMORParams = tableFullMORParams;
        this.morParams = morParams;
        setupCloudCredential();
    }

    @Override
    public boolean hasMoreScanRanges() {
        if (scanRangeSource == null) {
            return false;
        }

        return scanRangeSource.hasMoreOutput();
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        if (snapshotId.isEmpty() || scanRangeSource == null) {
            return List.of();
        }

        if (maxScanRangeLength == 0) {
            return scanRangeSource.getAllOutputs();
        }
        return scanRangeSource.getOutputs((int) maxScanRangeLength);
    }

    public void setupScanRangeLocations(boolean enableIncrementalScanRanges) throws StarRocksException {
        Preconditions.checkNotNull(snapshotId, "snapshot id is null");
        if (snapshotId.isEmpty()) {
            LOG.warn(String.format("Table %s has no snapshot!", icebergTable.getCatalogTableName()));
            return;
        }

        GetRemoteFilesParams params =
                IcebergGetRemoteFilesParams.newBuilder()
                        .setAllParams(tableFullMORParams)
                        .setParams(morParams)
                        .setTableVersionRange(TableVersionRange.withEnd(snapshotId))
                        .setPredicate(icebergJobPlanningPredicate)
                        .build();

        RemoteFileInfoSource remoteFileInfoSource;
        if (enableIncrementalScanRanges) {
            remoteFileInfoSource = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFilesAsync(icebergTable, params);
        } else {
            List<RemoteFileInfo> splits = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFiles(icebergTable, params);
            if (splits.isEmpty()) {
                LOG.warn("There is no scan tasks after planFies on {}.{} and predicate: [{}]",
                        icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), icebergJobPlanningPredicate);
                return;
            }
            remoteFileInfoSource = new RemoteFileInfoDefaultSource(splits);
            if (morParams != IcebergMORParams.EMPTY) {
                boolean needToCheckEqualityIds = tableFullMORParams.size() != 3;
                IcebergRemoteSourceTrigger trigger = new IcebergRemoteSourceTrigger(
                        remoteFileInfoSource, morParams, needToCheckEqualityIds);
                Deque<RemoteFileInfo> remoteFileInfoDeque = trigger.getQueue(morParams);
                remoteFileInfoSource = new QueueIcebergRemoteFileInfoSource(trigger, remoteFileInfoDeque);
            }
        }

        scanRangeSource = new IcebergConnectorScanRangeSource(icebergTable, remoteFileInfoSource, morParams, desc);
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    private void setupCloudCredential() {
        String catalogName = icebergTable.getCatalogName();
        if (catalogName == null) {
            return;
        }

        // Hard coding here
        // Try to get tabular signed temporary credential
        CloudConfiguration tabularTempCloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForTabular(icebergTable.getNativeTable().io().properties());
        if (tabularTempCloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            // If we get CloudConfiguration succeed from iceberg FileIO's properties, we just using it.
            cloudConfiguration = tabularTempCloudConfiguration;
        } else {
<<<<<<< HEAD
            Connector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
=======
            CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            Preconditions.checkState(connector != null,
                    String.format("connector of catalog %s should not be null", catalogName));
            cloudConfiguration = connector.getMetadata().getCloudConfiguration();
            Preconditions.checkState(cloudConfiguration != null,
                    String.format("cloudConfiguration of catalog %s should not be null", catalogName));
        }
    }

<<<<<<< HEAD
    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
        getAliveBackends();
    }

    private void getAliveBackends() throws UserException {
        ImmutableCollection<ComputeNode> computeNodes =
                ImmutableList.copyOf(GlobalStateMgr.getCurrentSystemInfo().getComputeNodes());

        for (ComputeNode computeNode : computeNodes) {
            if (computeNode.isAlive()) {
                hostToBeId.put(computeNode.getHost(), computeNode.getId());
            }
        }
        if (hostToBeId.isEmpty()) {
            throw new UserException(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR);
        }
    }

    public void preProcessIcebergPredicate(ScalarOperator predicate) {
        this.predicate = predicate;
    }

    /**
     * Append columns need to read for equality delete files
     */
    public void appendEqualityColumns(PhysicalIcebergScanOperator node,
                                      ColumnRefFactory columnRefFactory,
                                      ExecPlan context) {
        Table referenceTable = node.getTable();
        Set<String> scanNodeColumns = node.getColRefToColumnMetaMap().values().stream()
                .map(Column::getName).collect(Collectors.toSet());
        Set<String> appendEqualityColumns = equalityDeleteColumns.stream()
                .filter(name -> !scanNodeColumns.contains(name)).collect(Collectors.toSet());
        Map<String, Column> nameToColumns = referenceTable.getFullSchema().stream()
                .collect(Collectors.toMap(Column::getName, item -> item));
        for (String eqName : appendEqualityColumns) {
            if (nameToColumns.containsKey(eqName)) {
                Column column = nameToColumns.get(eqName);
                Field field;
                TableName tableName = desc.getRef().getName();
                if (referenceTable.getFullSchema().contains(column)) {
                    field = new Field(column.getName(), column.getType(), tableName,
                            new SlotRef(tableName, column.getName(), column.getName()), true);
                } else {
                    field = new Field(column.getName(), column.getType(), tableName,
                            new SlotRef(tableName, column.getName(), column.getName()), false);
                }
                ColumnRefOperator columnRef = columnRefFactory.create(field.getName(),
                        field.getType(), column.isAllowNull());
                SlotDescriptor slotDescriptor =
                        context.getDescTbl().addSlotDescriptor(desc, new SlotId(columnRef.getId()));
                slotDescriptor.setColumn(column);
                slotDescriptor.setIsNullable(column.isAllowNull());
                slotDescriptor.setIsMaterialized(true);
                context.getColRefToExpr().put(columnRef, new SlotRef(columnRef.toString(), slotDescriptor));
            }
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }

    public static BiMap<Integer, PartitionField> getIdentityPartitions(PartitionSpec partitionSpec) {
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
            partitionValue = field.transform().toHumanString(type,
                    PartitionUtil.getPartitionValue(partition, index, javaClass));

            // currently starrocks date literal only support local datetime
            if (type.equals(Types.TimestampType.withZone())) {
                partitionValue = ChronoUnit.MICROS.addTo(Instant.ofEpochSecond(0).atZone(TimeUtils.getTimeZone().toZoneId()),
                        PartitionUtil.getPartitionValue(partition, index, javaClass)).toLocalDateTime().toString();
            }

            partitionValues.add(partitionValue);

            cols.add(icebergTable.getColumn(field.name()));
        });

        return PartitionUtil.createPartitionKey(partitionValues, cols, Table.TableType.ICEBERG);
    }

    public void setupScanRangeLocations(DescriptorTable descTbl) throws UserException {
        Optional<Snapshot> snapshot = Optional.ofNullable(icebergTable.getNativeTable().currentSnapshot());
        if (!snapshot.isPresent()) {
            LOG.warn(String.format("Table %s has no snapshot!", icebergTable.getRemoteTableName()));
            return;
        }

        String catalogName = icebergTable.getCatalogName();
        long snapshotId = snapshot.get().snapshotId();

        List<RemoteFileInfo> splits = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                catalogName, icebergTable, null, snapshotId, predicate, null);

        if (splits.isEmpty()) {
            LOG.warn("There is no scan tasks after planFies on {}.{} and predicate: [{}]",
                    icebergTable.getRemoteDbName(), icebergTable.getRemoteTableName(), predicate);
            return;
        }

        RemoteFileDesc remoteFileDesc = splits.get(0).getFiles().get(0);
        if (remoteFileDesc == null) {
            LOG.warn("There is no scan tasks after planFies on {}.{} and predicate: [{}]",
                    icebergTable.getRemoteDbName(), icebergTable.getRemoteTableName(), predicate);
            return;
        }

        Map<StructLike, Long> partitionKeyToId = Maps.newHashMap();
        Map<Long, List<Integer>> idToPartitionSlots = Maps.newHashMap();
        List<Integer> currentEqualityIds = new ArrayList<>();
        for (FileScanTask task : remoteFileDesc.getIcebergScanTasks()) {
            DataFile file = task.file();
            LOG.debug("Scan with file " + file.path() + ", file record count " + file.recordCount());
            if (file.fileSizeInBytes() == 0) {
                continue;
            }

            StructLike partition = task.file().partition();
            long partitionId = 0;
            if (!partitionKeyToId.containsKey(partition)) {
                partitionId = icebergTable.nextPartitionId();
                partitionKeyToId.put(partition, partitionId);
                BiMap<Integer, PartitionField> indexToField = getIdentityPartitions(task.spec());
                if (!indexToField.isEmpty()) {
                    List<Integer> partitionSlotIds = task.spec().fields().stream()
                            .map(x -> desc.getColumnSlot(x.name()))
                            .filter(Objects::nonNull)
                            .map(SlotDescriptor::getId)
                            .map(SlotId::asInt)
                            .collect(Collectors.toList());
                    List<Integer> indexes = task.spec().fields().stream()
                            .filter(x -> desc.getColumnSlot(x.name()) != null)
                            .map(x -> indexToField.inverse().get(x))
                            .collect(Collectors.toList());
                    PartitionKey partitionKey = getPartitionKey(partition, task.spec(), indexes, indexToField);

                    DescriptorTable.ReferencedPartitionInfo partitionInfo =
                            new DescriptorTable.ReferencedPartitionInfo(partitionId, partitionKey);

                    descTbl.addReferencedPartitions(icebergTable, partitionInfo);
                    idToPartitionSlots.put(partitionId, partitionSlotIds);
                }
            }

            partitionId = partitionKeyToId.get(partition);

            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

            THdfsScanRange hdfsScanRange = new THdfsScanRange();
            if (file.path().toString().startsWith(icebergTable.getTableLocation())) {
                hdfsScanRange.setRelative_path(file.path().toString().substring(icebergTable.getTableLocation().length()));
            } else {
                hdfsScanRange.setFull_path(file.path().toString());
            }
            hdfsScanRange.setOffset(task.start());
            hdfsScanRange.setLength(task.length());
            // For iceberg table we do not need partition id
            if (!idToPartitionSlots.containsKey(partitionId)) {
                hdfsScanRange.setPartition_id(-1);
            } else {
                hdfsScanRange.setPartition_id(partitionId);
                hdfsScanRange.setIdentity_partition_slot_ids(idToPartitionSlots.get(partitionId));
            }
            hdfsScanRange.setFile_length(file.fileSizeInBytes());
            // Iceberg data file cannot be overwritten
            hdfsScanRange.setModification_time(0);
            hdfsScanRange.setFile_format(IcebergApiConverter.getHdfsFileFormat(file.format()).toThrift());

            List<TIcebergDeleteFile> deleteFiles = new ArrayList<>();
            for (DeleteFile deleteFile : task.deletes()) {
                FileContent content = deleteFile.content();
                if (content == FileContent.EQUALITY_DELETES) {
                    List<Integer> taskEqualityFieldIds = deleteFile.equalityFieldIds();
                    if (taskEqualityFieldIds.isEmpty()) {
                        continue;
                    }
                    if (!currentEqualityIds.isEmpty() && !currentEqualityIds.equals(taskEqualityFieldIds)) {
                        throw new StarRocksConnectorException("Schema change of equality columns changed is not supported");
                    }

                    if (currentEqualityIds.isEmpty()) {
                        currentEqualityIds = taskEqualityFieldIds;
                        prepareRequiredColumnsForDeletes(currentEqualityIds);
                    }
                }

                TIcebergDeleteFile target = new TIcebergDeleteFile();
                target.setFull_path(deleteFile.path().toString());
                target.setFile_content(content == FileContent.EQUALITY_DELETES ? TIcebergFileContent.EQUALITY_DELETES :
                        TIcebergFileContent.POSITION_DELETES);
                target.setLength(deleteFile.fileSizeInBytes());
                deleteFiles.add(target);
            }

            if (!deleteFiles.isEmpty()) {
                hdfsScanRange.setDelete_files(deleteFiles);
            }

            if (!deleteColumnSlotIds.isEmpty()) {
                hdfsScanRange.setDelete_column_slot_ids(deleteColumnSlotIds);
            }

            TScanRange scanRange = new TScanRange();
            scanRange.setHdfs_scan_range(hdfsScanRange);
            scanRangeLocations.setScan_range(scanRange);

            TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
            scanRangeLocations.addToLocations(scanRangeLocation);

            result.add(scanRangeLocations);
        }

        if (!currentEqualityIds.isEmpty()) {
            icebergTable.setIdentifierFieldIds(ImmutableSet.copyOf(currentEqualityIds));
        }

        scanNodePredicates.setSelectedPartitionIds(partitionKeyToId.values());
=======
    public void setCloudConfiguration(CloudConfiguration cloudConfiguration) {
        this.cloudConfiguration = cloudConfiguration;
    }

    public void preProcessIcebergPredicate(ScalarOperator predicate) {
        this.icebergJobPlanningPredicate = predicate;
    }

    // for unit tests
    public ScalarOperator getIcebergJobPlanningPredicate() {
        return icebergJobPlanningPredicate;
    }

    // for unit tests
    public List<Integer> getExtendedColumnSlotIds() {
        return scanRangeSource.getExtendedColumnSlotIds();
    }

    public Set<String> getSeenEqualityDeleteFiles() {
        return scanRangeSource.getSeenEqDeleteFiles();
    }

    // for unit tests
    public IcebergMORParams getMORParams() {
        return morParams;
    }

    // for unit tests
    public List<IcebergMORParams> getTableFullMORParams() {
        return tableFullMORParams;
    }

    public void setSnapshotId(Optional<Long> snapshotId) {
        this.snapshotId = snapshotId;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

<<<<<<< HEAD
    private void prepareRequiredColumnsForDeletes(List<Integer> equalityIds) {
        Schema schema = icebergTable.getNativeTable().schema();
        List<String> equalityColNames = equalityIds.stream().map(schema::findColumnName).collect(Collectors.toList());
        int startSlotId = Integer.MAX_VALUE - 10000;
        for (String eqColName : equalityColNames) {
            SlotDescriptor slotDesc = desc.getColumnSlot(eqColName);
            if (slotDesc != null) {
                slotDesc = new SlotDescriptor(slotDesc.getId(), equalityDeleteTupleDesc, slotDesc);
                slotDesc.setIsOutputColumn(false);
            } else {
                int slotId = startSlotId++;
                SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(slotId), equalityDeleteTupleDesc);
                Column column = icebergTable.getColumn(eqColName);
                slotDescriptor.setType(column.getType());
                slotDescriptor.setIsNullable(true);
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setIsOutputColumn(false);
                slotDescriptor.setColumn(column);
                slotDesc = slotDescriptor;
            }
            deleteColumnSlotIds.add(slotDesc.getId().asInt());
            equalityDeleteTupleDesc.addSlot(slotDesc);
        }
    }

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.add("icebergTable=", icebergTable.getName());
        return helper.toString();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

<<<<<<< HEAD
        output.append(prefix).append("TABLE: ").append(icebergTable.getNativeTable()).append("\n");
=======
        output.append(prefix).append("TABLE: ")
                .append(icebergTable.getCatalogDBName())
                .append(".")
                .append(icebergTable.getName())
                .append("\n");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

        if (null != sortColumn) {
            output.append(prefix).append("SORT COLUMN: ").append(sortColumn).append("\n");
        }
        if (!conjuncts.isEmpty()) {
            output.append(prefix).append("PREDICATES: ").append(
                    getExplainString(conjuncts)).append("\n");
        }
        if (!scanNodePredicates.getMinMaxConjuncts().isEmpty()) {
            output.append(prefix).append("MIN/MAX PREDICATES: ").append(
                    getExplainString(scanNodePredicates.getMinMaxConjuncts())).append("\n");
        }

        output.append(prefix).append(String.format("cardinality=%s", cardinality));
        output.append("\n");

        output.append(prefix).append(String.format("avgRowSize=%s", avgRowSize));
        output.append("\n");

<<<<<<< HEAD
        output.append(prefix).append(String.format("numNodes=%s", numNodes));
        output.append("\n");

        if (detailLevel == TExplainLevel.VERBOSE) {
=======
        if (detailLevel == TExplainLevel.VERBOSE) {
            HdfsScanNode.appendDataCacheOptionsInExplain(output, prefix, dataCacheOptions);

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                Type type = slotDescriptor.getOriginType();
                if (type.isComplexType()) {
                    output.append(prefix)
                            .append(String.format("Pruned type: %d <-> [%s]\n", slotDescriptor.getId().asInt(), type));
                }
            }
        }

        if (detailLevel == TExplainLevel.VERBOSE && !isResourceMappingCatalog(icebergTable.getCatalogName())) {
<<<<<<< HEAD
            List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    icebergTable.getCatalogName(), icebergTable.getRemoteDbName(), icebergTable.getRemoteTableName());

            output.append(prefix).append(
                    String.format("partitions=%s/%s", scanNodePredicates.getSelectedPartitionIds().size(),
                            partitionNames.size() == 0 ? 1 : partitionNames.size()));
=======
            ConnectorMetadatRequestContext requestContext = new ConnectorMetadatRequestContext();
            requestContext.setTableVersionRange(TableVersionRange.withEnd(snapshotId));
            List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    icebergTable.getCatalogName(), icebergTable.getCatalogDBName(),
                    icebergTable.getCatalogTableName(), requestContext);

            if (selectedPartitionCount == -1) {
                if (scanRangeSource != null) {
                    // we have to consume all scan ranges to know how many partition been selected.
                    while (scanRangeSource.hasMoreOutput()) {
                        scanRangeSource.getOutputs(1000);
                    }
                    selectedPartitionCount = scanRangeSource.selectedPartitionCount();
                } else {
                    selectedPartitionCount = 0;
                }
            }

            output.append(prefix).append(
                    String.format("partitions=%s/%s", selectedPartitionCount,
                            partitionNames.isEmpty() ? 1 : partitionNames.size()));
            output.append("\n");
        }

        if (morParams.getScanTaskType() == IcebergMORParams.ScanTaskType.EQ_DELETE) {
            List<String> identifierColumnNames = morParams.getEqualityIds().stream()
                    .map(id -> icebergTable.getNativeTable().schema().findColumnName(id))
                    .collect(Collectors.toList());
            output.append(prefix).append("Iceberg identifier columns: ").append(identifierColumnNames);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            output.append("\n");
        }

        return output.toString();
    }

    @Override
<<<<<<< HEAD
    public int getNumInstances() {
        return result.size();
    }

    @Override
    public void computeStats(Analyzer analyzer) {
        if (cardinality > 0) {
            avgRowSize = totalBytes / (float) cardinality;
            if (hasLimit()) {
                cardinality = Math.min(cardinality, limit);
            }

            numNodes = Math.min(hostToBeId.size(), result.size());
        }
        // even current node scan has no data, at least one backend will be assigned when the fragment actually execute
        numNodes = numNodes <= 0 ? 1 : numNodes;
        // when node scan has no data, cardinality should be 0 instead of a invalid value after computeStats()
        cardinality = cardinality == -1 ? 0 : cardinality;
    }

    @Override
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        msg.hdfs_scan_node = tHdfsScanNode;

        String sqlPredicates = getExplainString(conjuncts);
        msg.hdfs_scan_node.setSql_predicates(sqlPredicates);
<<<<<<< HEAD

        msg.hdfs_scan_node.setTable_name(icebergTable.getRemoteTableName());

        if (!deleteColumnSlotIds.isEmpty()) {
            msg.hdfs_scan_node.setMor_tuple_id(equalityDeleteTupleDesc.getId().asInt());
        }

        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        HdfsScanNode.setMinMaxConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
    }

    @Override
    public boolean canUsePipeLine() {
        return true;
=======
        if (scanRangeSource != null) {
            msg.hdfs_scan_node.setExtended_slot_ids(scanRangeSource.getExtendedColumnSlotIds());
        }
        msg.hdfs_scan_node.setTable_name(icebergTable.getName());
        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        HdfsScanNode.setMinMaxConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
        HdfsScanNode.setDataCacheOptionsToThrift(tHdfsScanNode, dataCacheOptions);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
<<<<<<< HEAD
}
=======

    @Override
    protected boolean supportTopNRuntimeFilter() {
        return !icebergTable.isV2Format();
    }
}
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
