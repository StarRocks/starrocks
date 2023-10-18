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
import com.google.common.collect.Maps;
import com.starrocks.analysis.Analyzer;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.connector.CatalogConnector;
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
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
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
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

public class IcebergScanNode extends ScanNode {
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    private IcebergTable srIcebergTable; // table definition in starRocks

    private HDFSScanNodePredicates scanNodePredicates = new HDFSScanNodePredicates();

    private List<TScanRangeLocations> result = new ArrayList<>();

    // Scalar operators to filter iceberg metadata during plan files
    private ScalarOperator predicate = null;

    private Set<String> equalityDeleteColumns = new HashSet<>();

    private long totalBytes = 0;

    private boolean isFinalized = false;
    private CloudConfiguration cloudConfiguration = null;

    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
        srIcebergTable = (IcebergTable) desc.getTable();
        setupCloudCredential();
    }

    private void setupCloudCredential() {
        String catalogName = srIcebergTable.getCatalogName();
        if (catalogName == null) {
            return;
        }

        // Hard coding here
        // Try to get tabular signed temporary credential
        CloudConfiguration tabularTempCloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForTabular(srIcebergTable.getNativeTable().io().properties());
        if (tabularTempCloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            // If we get CloudConfiguration succeed from iceberg FileIO's properties, we just using it.
            cloudConfiguration = tabularTempCloudConfiguration;
        } else {
            CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
            Preconditions.checkState(connector != null,
                    String.format("connector of catalog %s should not be null", catalogName));
            cloudConfiguration = connector.getMetadata().getCloudConfiguration();
            Preconditions.checkState(cloudConfiguration != null,
                    String.format("cloudConfiguration of catalog %s should not be null", catalogName));
        }
    }

    @Override
    public void init(Analyzer analyzer) throws UserException {
        super.init(analyzer);
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
                throw new StarRocksConnectorException("Iceberg equality delete is not supported");
            }
        }
    }

    @Override
    public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
        return result;
    }

    public static BiMap<Integer, PartitionField> getIdentityPartitions(PartitionSpec partitionSpec)
    {

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
            partitionValues.add(partitionValue);

            cols.add(srIcebergTable.getColumn(field.name()));
        });

        return PartitionUtil.createPartitionKey(partitionValues, cols, Table.TableType.ICEBERG);
    }

    public void setupScanRangeLocations(DescriptorTable descTbl) throws UserException {
        Optional<Snapshot> snapshot = Optional.ofNullable(srIcebergTable.getNativeTable().currentSnapshot());
        if (!snapshot.isPresent()) {
            LOG.warn(String.format("Table %s has no snapshot!", srIcebergTable.getRemoteTableName()));
            return;
        }

        String catalogName = srIcebergTable.getCatalogName();
        long snapshotId = snapshot.get().snapshotId();

        List<RemoteFileInfo> splits = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                catalogName, srIcebergTable, null, snapshotId, predicate, null, -1);

        if (splits.isEmpty()) {
            LOG.warn("There is no scan tasks after planFies on {}.{} and predicate: [{}]",
                    srIcebergTable.getRemoteDbName(), srIcebergTable.getRemoteTableName(), predicate);
            return;
        }

        RemoteFileDesc remoteFileDesc = splits.get(0).getFiles().get(0);
        if (remoteFileDesc == null) {
            LOG.warn("There is no scan tasks after planFies on {}.{} and predicate: [{}]",
                    srIcebergTable.getRemoteDbName(), srIcebergTable.getRemoteTableName(), predicate);
            return;
        }

        Map<StructLike, Long> partitionKeyToId = Maps.newHashMap();
        Map<Long, List<Integer>> idToPartitionSlots = Maps.newHashMap();
        for (FileScanTask task : remoteFileDesc.getIcebergScanTasks()) {
            DataFile file = task.file();
            LOG.debug("Scan with file " + file.path() + ", file record count " + file.recordCount());
            if (file.fileSizeInBytes() == 0) {
                continue;
            }

            StructLike partition = task.file().partition();
            long partitionId = 0;
            if (!partitionKeyToId.containsKey(partition)) {
                partitionId = srIcebergTable.nextPartitionId();
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

                    descTbl.addReferencedPartitions(srIcebergTable, partitionInfo);
                    idToPartitionSlots.put(partitionId, partitionSlotIds);
                }
            }

            partitionId = partitionKeyToId.get(partition);

            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();

            THdfsScanRange hdfsScanRange = new THdfsScanRange();
            if (file.path().toString().startsWith(srIcebergTable.getTableLocation())) {
                hdfsScanRange.setRelative_path(file.path().toString().substring(srIcebergTable.getTableLocation().length()));
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

            hdfsScanRange.setDelete_files(task.deletes().stream().map(source -> {
                TIcebergDeleteFile target = new TIcebergDeleteFile();
                target.setFull_path(source.path().toString());
                target.setFile_content(
                        source.content() == FileContent.EQUALITY_DELETES ? TIcebergFileContent.EQUALITY_DELETES :
                                TIcebergFileContent.POSITION_DELETES);
                target.setLength(source.fileSizeInBytes());

                if (source.content() == FileContent.EQUALITY_DELETES) {
                    source.equalityFieldIds().forEach(fieldId -> {
                        equalityDeleteColumns.add(
                                srIcebergTable.getNativeTable().schema().findColumnName(fieldId));
                    });
                }

                return target;
            }).collect(Collectors.toList()));
            TScanRange scanRange = new TScanRange();
            scanRange.setHdfs_scan_range(hdfsScanRange);
            scanRangeLocations.setScan_range(scanRange);

            TScanRangeLocation scanRangeLocation = new TScanRangeLocation(new TNetworkAddress("-1", -1));
            scanRangeLocations.addToLocations(scanRangeLocation);

            result.add(scanRangeLocations);
        }

        scanNodePredicates.setSelectedPartitionIds(partitionKeyToId.values());
    }

    public HDFSScanNodePredicates getScanNodePredicates() {
        return scanNodePredicates;
    }

    @Override
    protected String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(super.debugString());
        helper.add("icebergTable=", srIcebergTable.getName());
        return helper.toString();
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();

        output.append(prefix).append("TABLE: ").append(srIcebergTable.getNativeTable()).append("\n");

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

        if (detailLevel == TExplainLevel.VERBOSE) {
            for (SlotDescriptor slotDescriptor : desc.getSlots()) {
                Type type = slotDescriptor.getOriginType();
                if (type.isComplexType()) {
                    output.append(prefix)
                            .append(String.format("Pruned type: %d <-> [%s]\n", slotDescriptor.getId().asInt(), type));
                }
            }
        }

        if (detailLevel == TExplainLevel.VERBOSE && !isResourceMappingCatalog(srIcebergTable.getCatalogName())) {
            List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    srIcebergTable.getCatalogName(), srIcebergTable.getRemoteDbName(), srIcebergTable.getRemoteTableName());

            output.append(prefix).append(
                    String.format("partitions=%s/%s", scanNodePredicates.getSelectedPartitionIds().size(),
                            partitionNames.size() == 0 ? 1 : partitionNames.size()));
            output.append("\n");
        }

        return output.toString();
    }

    @Override
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
        }
        // when node scan has no data, cardinality should be 0 instead of a invalid value after computeStats()
        cardinality = cardinality == -1 ? 0 : cardinality;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.HDFS_SCAN_NODE;
        THdfsScanNode tHdfsScanNode = new THdfsScanNode();
        tHdfsScanNode.setTuple_id(desc.getId().asInt());
        msg.hdfs_scan_node = tHdfsScanNode;

        String sqlPredicates = getExplainString(conjuncts);
        msg.hdfs_scan_node.setSql_predicates(sqlPredicates);

        msg.hdfs_scan_node.setTable_name(srIcebergTable.getRemoteTableName());

        HdfsScanNode.setScanOptimizeOptionToThrift(tHdfsScanNode, this);
        HdfsScanNode.setCloudConfigurationToThrift(tHdfsScanNode, cloudConfiguration);
        HdfsScanNode.setMinMaxConjunctsToThrift(tHdfsScanNode, this, this.getScanNodePredicates());
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    protected boolean supportTopNRuntimeFilter() {
        return !srIcebergTable.isV2Format();
    }
}
