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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/OlapTableSink.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.load.Load;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TOlapTableIndexSchema;
import com.starrocks.thrift.TOlapTableIndexTablets;
import com.starrocks.thrift.TOlapTableLocationParam;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TOlapTablePartitionParam;
import com.starrocks.thrift.TOlapTableSchemaParam;
import com.starrocks.thrift.TOlapTableSink;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TTabletLocation;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWriteQuorumType;
import com.starrocks.transaction.TransactionState;
import com.starrocks.warehouse.Warehouse;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class OlapTableSink extends DataSink {
    private static final Logger LOG = LogManager.getLogger(OlapTableSink.class);

    private final int clusterId;
    // input variables
    private OlapTable dstTable;
    private final TupleDescriptor tupleDescriptor;
    // specified partition ids. this list should not be empty and should contains all related partition ids
    private List<Long> partitionIds;

    // set after init called
    private TDataSink tDataSink;

    private final boolean enablePipelineLoad;
    private final TWriteQuorumType writeQuorum;
    private final boolean enableReplicatedStorage;

    private boolean nullExprInAutoIncrement;
    private boolean missAutoIncrementColumn;
    private int autoIncrementSlotId;
    private boolean enableAutomaticPartition;
    private TPartialUpdateMode partialUpdateMode;

    public OlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, List<Long> partitionIds,
                         TWriteQuorumType writeQuorum, boolean enableReplicatedStorage,
                         boolean nullExprInAutoIncrement, boolean enableAutomaticPartition) {
        this(dstTable, tupleDescriptor, partitionIds, true, writeQuorum, enableReplicatedStorage,
                nullExprInAutoIncrement, enableAutomaticPartition);
    }

    public OlapTableSink(OlapTable dstTable, TupleDescriptor tupleDescriptor, List<Long> partitionIds,
                         boolean enablePipelineLoad, TWriteQuorumType writeQuorum, boolean enableReplicatedStorage,
                         boolean nullExprInAutoIncrement, boolean enableAutomaticPartition) {
        this.dstTable = dstTable;
        this.tupleDescriptor = tupleDescriptor;
        Preconditions.checkState(!CollectionUtils.isEmpty(partitionIds));
        this.partitionIds = partitionIds;
        this.clusterId = dstTable.getClusterId();
        this.enablePipelineLoad = enablePipelineLoad;
        this.writeQuorum = writeQuorum;
        this.enableReplicatedStorage = enableReplicatedStorage;
        this.nullExprInAutoIncrement = nullExprInAutoIncrement;
        this.missAutoIncrementColumn = false;
        this.enableAutomaticPartition = enableAutomaticPartition;
        this.autoIncrementSlotId = -1;
        if (tupleDescriptor != null) {
            for (int i = 0; i < this.tupleDescriptor.getSlots().size(); ++i) {
                SlotDescriptor slot = this.tupleDescriptor.getSlots().get(i);
                if (slot.getColumn().isAutoIncrement()) {
                    this.autoIncrementSlotId = i;
                    break;
                }
            }
        }
        this.partialUpdateMode = TPartialUpdateMode.UNKNOWN_MODE;
    }

    public void init(TUniqueId loadId, long txnId, long dbId, long loadChannelTimeoutS)
            throws AnalysisException {
        TOlapTableSink tSink = new TOlapTableSink();
        tSink.setLoad_id(loadId);
        tSink.setTxn_id(txnId);
        tSink.setNull_expr_in_auto_increment(nullExprInAutoIncrement);
        tSink.setMiss_auto_increment_column(missAutoIncrementColumn);
        tSink.setAuto_increment_slot_id(autoIncrementSlotId);
        TransactionState txnState =
                GlobalStateMgr.getCurrentGlobalTransactionMgr()
                        .getTransactionState(dbId, txnId);
        if (txnState != null) {
            tSink.setTxn_trace_parent(txnState.getTraceParent());
            tSink.setLabel(txnState.getLabel());
        }
        tSink.setDb_id(dbId);
        tSink.setLoad_channel_timeout_s(loadChannelTimeoutS);
        tSink.setIs_lake_table(dstTable.isCloudNativeTableOrMaterializedView() ||
                dstTable.isOlapExternalTable() && ((ExternalOlapTable)dstTable).isSourceTableCloudNativeTableOrMaterializedView());
        tSink.setKeys_type(dstTable.getKeysType().toThrift());
        tSink.setWrite_quorum_type(writeQuorum);
        tSink.setEnable_replicated_storage(enableReplicatedStorage);
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db != null) {
            tSink.setDb_name(db.getFullName());
        }
        tDataSink = new TDataSink(TDataSinkType.DATA_SPLIT_SINK);
        tDataSink.setType(TDataSinkType.OLAP_TABLE_SINK);
        tDataSink.setOlap_table_sink(tSink);
        tSink.setEnable_replicated_storage(enableReplicatedStorage);

        for (Long partitionId : partitionIds) {
            Partition part = dstTable.getPartition(partitionId);
            if (part == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_PARTITION, partitionId, dstTable.getName());
            }
        }
    }

    public void setMissAutoIncrementColumn() {
        this.missAutoIncrementColumn = true;
    }

    public void updateLoadId(TUniqueId newLoadId) {
        tDataSink.getOlap_table_sink().setLoad_id(newLoadId);
    }

    public void setPartialUpdateMode(TPartialUpdateMode mode) {
        this.partialUpdateMode = mode;
    }

    public void complete(String mergeCondition) throws UserException {
        TOlapTableSink tSink = tDataSink.getOlap_table_sink();
        if (mergeCondition != null && !mergeCondition.isEmpty()) {
            tSink.setMerge_condition(mergeCondition);
        }
        complete();
    }

    class AssociateTableMeta {
        private final MaterializedIndexMeta indexMeta;
        private final OlapTable targetTable;
        private final Partition partition;
        private AssociateTableMeta(MaterializedIndexMeta indexMeta, OlapTable olapTable,
                                   Partition partition) {
            this.indexMeta = indexMeta;
            this.targetTable = olapTable;
            this.partition = partition;
        }

        public MaterializedIndexMeta getIndexMeta() {
            return indexMeta;
        }

        public OlapTable getTargetTable() {
            return targetTable;
        }

        public Partition getPartition() {
            return partition;
        }
    }

    // must called after tupleDescriptor is computed
    public void complete() throws UserException {
        TOlapTableSink tSink = tDataSink.getOlap_table_sink();

        tSink.setTable_id(dstTable.getId());
        tSink.setTable_name(dstTable.getName());
        tSink.setTuple_id(tupleDescriptor.getId().asInt());
        int numReplicas = 1;
        Optional<Partition> optionalPartition = dstTable.getPartitions().stream().findFirst();
        if (optionalPartition.isPresent()) {
            long partitionId = optionalPartition.get().getId();
            numReplicas = dstTable.getPartitionInfo().getReplicationNum(partitionId);
        }
        tSink.setNum_replicas(numReplicas);
        tSink.setNeed_gen_rollup(dstTable.shouldLoadToNewRollup());
        tSink.setSchema(createSchema(tSink.getDb_id(), dstTable));

        if (dstTable.isEnableColocateMVIndex()) {
            tSink.setEnable_colocate_mv_index(true);
        }
        if (dstTable.hasAssociatedTables()) {
            // prepare associate tables
            Database db = GlobalStateMgr.getCurrentState().getDb(tSink.getDb_id());
            Preconditions.checkNotNull(db);
            Map<Long, List<AssociateTableMeta>> idToAssociatedMetas = prepareAssociatedTables(db, dstTable);
            tSink.setPartition(createPartition(tSink.getDb_id(), dstTable, idToAssociatedMetas));
            tSink.setLocation(createLocation(idToAssociatedMetas, dstTable));
        } else {
            tSink.setPartition(createPartition(tSink.getDb_id(), dstTable));
            tSink.setLocation(createLocation(dstTable));
        }
        tSink.setNodes_info(GlobalStateMgr.getCurrentState().createNodesInfo(clusterId));
        tSink.setPartial_update_mode(this.partialUpdateMode);
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "OLAP TABLE SINK\n");
        strBuilder.append(prefix + "  TABLE: " + dstTable.getName() + "\n");
        strBuilder.append(prefix + "  TUPLE ID: " + tupleDescriptor.getId() + "\n");
        strBuilder.append(prefix + "  " + DataPartition.RANDOM.getExplainString(explainLevel));
        return strBuilder.toString();
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.RANDOM;
    }

    @Override
    public TDataSink toThrift() {
        return tDataSink;
    }

    private TOlapTableSchemaParam createSchema(long dbId, OlapTable table) {
        TOlapTableSchemaParam schemaParam = new TOlapTableSchemaParam();
        schemaParam.setDb_id(dbId);
        schemaParam.setTable_id(table.getId());
        schemaParam.setVersion(0);

        schemaParam.tuple_desc = tupleDescriptor.toThrift();
        for (SlotDescriptor slotDesc : tupleDescriptor.getSlots()) {
            schemaParam.addToSlot_descs(slotDesc.toThrift());
        }

        for (Map.Entry<Long, MaterializedIndexMeta> pair : table.getIndexIdToMeta().entrySet()) {
            MaterializedIndexMeta indexMeta = pair.getValue();
            List<String> columns = Lists.newArrayList();
            columns.addAll(indexMeta.getSchema().stream().map(Column::getName).collect(Collectors.toList()));
            if (table.getKeysType() == KeysType.PRIMARY_KEYS) {
                columns.add(Load.LOAD_OP_COLUMN);
            }
            TOlapTableIndexSchema indexSchema = new TOlapTableIndexSchema(pair.getKey(), columns,
                    indexMeta.getSchemaHash());
            if (indexMeta.getWhereClause() != null) {
                if (!indexMeta.isWhereClauseAnalyzed()) {
                    String dbName = MetaUtils.getDatabase(dbId).getFullName();
                    ExpressionAnalyzer.analyzeExpression(indexMeta.getWhereClause(), new AnalyzeState(),
                            new Scope(RelationId.anonymous(),
                                    new RelationFields(table.getBaseSchema()
                                            .stream()
                                            .map(col
                                                    -> new Field(col.getName(), col.getType(),
                                                    new TableName(dbName, table.getName()), null))
                                            .collect(Collectors.toList()))),
                            new ConnectContext());
                    indexMeta.setWhereClauseAnalyzed(true);
                }

                Map<String, SlotDescriptor> descMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

                Expr whereClause = indexMeta.getWhereClause();

                for (SlotDescriptor slot : tupleDescriptor.getSlots()) {
                    descMap.put(slot.getColumn().getName(), slot);
                }

                List<SlotRef> slots = new ArrayList<>();
                whereClause.collect(SlotRef.class, slots);

                for (SlotRef slot : slots) {
                    SlotDescriptor slotDesc = descMap.get(slot.getColumnName());
                    slot.setDesc(slotDesc);
                }

                indexSchema.setWhere_clause(whereClause.treeToThrift());
                LOG.info("OlapTableSink Where clause: {}", indexMeta.getWhereClause().explain());

            }
            schemaParam.addToIndexes(indexSchema);
        }
        return schemaParam;
    }

    private List<String> getDistColumns(DistributionInfo distInfo, OlapTable table) throws UserException {
        List<String> distColumns = Lists.newArrayList();
        switch (distInfo.getType()) {
            case HASH: {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distInfo;
                for (Column column : hashDistributionInfo.getDistributionColumns()) {
                    distColumns.add(column.getName());
                }
                break;
            }
            case RANDOM: {
                break;
            }
            default:
                throw new UserException("unsupported distributed type, type=" + distInfo.getType());
        }
        return distColumns;
    }

    private Map<Long, List<AssociateTableMeta>> prepareAssociatedTables(Database db,
                                                                        OlapTable olapTable) throws AnalysisException {
        Map<Long, List<AssociateTableMeta>> idToTargetPartitionMap = Maps.newHashMap();
        List<Partition> basePartitions = Lists.newArrayList();
        if (olapTable.getPartitionInfo().isPartitioned()) {
            for (Long partitionId : partitionIds) {
                Partition partition = olapTable.getPartition(partitionId);
                basePartitions.add(partition);
            }
        } else {
            basePartitions.add(olapTable.getPartitions().iterator().next());
        }
        for (Partition partition : basePartitions) {
            int basePartitionTabletSize = partition.getBaseIndex().getTablets().size();
            // Attach the associated table's tablet indexes' into the partition thrift.
            for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getIndexIdToMeta().entrySet()) {
                MaterializedIndexMeta indexMeta = entry.getValue();
                AssociateTableMeta meta;
                if (indexMeta.isLogical()) {
                    Long targetTableId = indexMeta.getTargetTableId();
                    Long targetIndexId = indexMeta.getTargetTableIndexId();
                    OlapTable targetTable = (OlapTable) db.getTable(targetTableId);
                    Partition targetPartition;
                    if (targetTable.getPartitionInfo().isPartitioned()) {
                        targetPartition = targetTable.getPartition(partition.getName());
                        if (targetPartition == null) {
                            throw new AnalysisException(
                                    String.format("Associated table's partition should be kept the same with the base " +
                                                    "table, partition %s is not found in target table %s", partition.getName(),
                                            targetTable.getName()));
                        }
                    } else {
                        targetPartition = targetTable.getPartitions().iterator().next();
                        if (targetPartition == null) {
                            throw new AnalysisException(
                                    String.format("Associated table's partition should be kept the same with the base " +
                                            "table, partition is not found in target table %s", targetTable.getName()));
                        }
                    }
                    Preconditions.checkNotNull(targetPartition);
                    MaterializedIndex targetIndex = targetPartition.getBaseIndex();
                    if (targetIndex.getId() != targetIndexId) {
                        throw new AnalysisException(
                                String.format("Only support associated table's(%s) base partition id %s, but now " +
                                        "the partition id is %s", targetTable.getName(), targetIndex.getId(), targetIndexId));
                    }
                    long targetPartitionId = targetPartition.getId();
                    if (targetIndex.getTablets().size() != basePartitionTabletSize) {
                        throw new AnalysisException(
                                String.format("Associated table's(%s) partition %s's tablets' size %s should be " +
                                                "equal base table partition's size %s", targetTable.getName(), targetPartitionId,
                                        targetIndex.getTablets().size(), basePartitionTabletSize));
                    }
                    meta = new AssociateTableMeta(indexMeta, targetTable, targetPartition);
                } else {
                    meta = new AssociateTableMeta(indexMeta, olapTable, partition);
                }
                idToTargetPartitionMap.computeIfAbsent(partition.getId(), k -> Lists.newArrayList()).add(meta);
            }
        }
        return idToTargetPartitionMap;
    }

    private TOlapTablePartitionParam createPartition(long dbId, OlapTable table) throws UserException {
        return createPartition(dbId, table, null);
    }

    private TOlapTablePartitionParam createPartition(long dbId, OlapTable table,
                                                     Map<Long, List<AssociateTableMeta>> idToAssociatedMetas) throws UserException {
        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        partitionParam.setDb_id(dbId);
        partitionParam.setTable_id(table.getId());
        partitionParam.setVersion(0);
        partitionParam.setEnable_automatic_partition(enableAutomaticPartition);

        PartitionType partType = table.getPartitionInfo().getType();
        if (idToAssociatedMetas != null && idToAssociatedMetas.size() > 0) {
            Preconditions.checkState(table.hasAssociatedTables());
            partitionParam.setEnable_associated_tables(true);
        }

        switch (partType) {
            case RANGE:
            case EXPR_RANGE:
            case EXPR_RANGE_V2: {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
                for (Column partCol : rangePartitionInfo.getPartitionColumns()) {
                    partitionParam.addToPartition_columns(partCol.getName());
                }
                DistributionInfo selectedDistInfo = null;
                for (Long partitionId : partitionIds) {
                    Partition partition = table.getPartition(partitionId);
                    TOlapTablePartition tPartition = new TOlapTablePartition();
                    tPartition.setId(partition.getId());
                    setRangeKeys(rangePartitionInfo, partition, tPartition);
                    setIndexAndBucketNums(idToAssociatedMetas, partition, tPartition);
                    partitionParam.addToPartitions(tPartition);
                    selectedDistInfo = setDistributedColumns(partitionParam, selectedDistInfo, partition, table);
                }
                if (rangePartitionInfo instanceof ExpressionRangePartitionInfo) {
                    ExpressionRangePartitionInfo exprPartitionInfo = (ExpressionRangePartitionInfo) rangePartitionInfo;
                    partitionParam.setPartition_exprs(Expr.treesToThrift(exprPartitionInfo.getPartitionExprs()));
                } else if (rangePartitionInfo instanceof ExpressionRangePartitionInfoV2) {
                    ExpressionRangePartitionInfoV2 expressionRangePartitionInfoV2 = (ExpressionRangePartitionInfoV2) rangePartitionInfo;
                    partitionParam.setPartition_exprs(Expr.treesToThrift(expressionRangePartitionInfoV2.getPartitionExprs()));
                }
                break;
            }
            case LIST:
                ListPartitionInfo listPartitionInfo = (ListPartitionInfo) table.getPartitionInfo();
                for (Column partCol : listPartitionInfo.getPartitionColumns()) {
                    partitionParam.addToPartition_columns(partCol.getName());
                }
                DistributionInfo selectedDistInfo = null;
                for (Long partitionId : partitionIds) {
                    Partition partition = table.getPartition(partitionId);
                    TOlapTablePartition tPartition = new TOlapTablePartition();
                    tPartition.setId(partition.getId());
                    setListPartitionValues(listPartitionInfo, partition, tPartition);
                    setIndexAndBucketNums(idToAssociatedMetas, partition, tPartition);
                    partitionParam.addToPartitions(tPartition);
                    selectedDistInfo = setDistributedColumns(partitionParam, selectedDistInfo, partition, table);
                }
                break;
            case UNPARTITIONED: {
                // there is no partition columns for single partition
                Preconditions.checkArgument(table.getPartitions().size() == 1,
                        "Number of table partitions is not 1 for unpartitioned table, partitionNum="
                                + table.getPartitions().size());
                Partition partition = null;
                if (partitionIds != null) {
                    Preconditions.checkState(partitionIds.size() == 1,
                            "invalid partitionIds size:{}", partitionIds.size());
                    partition = table.getPartition(partitionIds.get(0));
                } else {
                    partition = table.getPartitions().iterator().next();
                }

                TOlapTablePartition tPartition = new TOlapTablePartition();
                tPartition.setId(partition.getId());
                // No lowerBound and upperBound for this range
                setIndexAndBucketNums(idToAssociatedMetas, partition, tPartition);
                partitionParam.addToPartitions(tPartition);
                partitionParam.setDistributed_columns(
                        getDistColumns(partition.getDistributionInfo(), table));
                break;
            }
            default: {
                throw new UserException("unsupported partition for OlapTable, partition=" + partType);
            }
        }
        return partitionParam;
    }

    private List<TExprNode> literalExprsToTExprNodes(List<LiteralExpr> values) {
        return values.stream()
                .map(value -> value.treeToThrift().getNodes().get(0))
                .collect(Collectors.toList());
    }

    private void setListPartitionValues(ListPartitionInfo listPartitionInfo, Partition partition,
                                        TOlapTablePartition tPartition) {
        List<List<TExprNode>> inKeysExprNodes = new ArrayList<>();

        List<List<LiteralExpr>> multiValues = listPartitionInfo.getMultiLiteralExprValues().get(partition.getId());
        if (multiValues != null && !multiValues.isEmpty()) {
            inKeysExprNodes = multiValues.stream()
                    .map(this::literalExprsToTExprNodes)
                    .collect(Collectors.toList());
            tPartition.setIn_keys(inKeysExprNodes);
        }

        List<LiteralExpr> values = listPartitionInfo.getLiteralExprValues().get(partition.getId());
        if (values != null && !values.isEmpty()) {
            inKeysExprNodes = values.stream()
                    .map(value -> this.literalExprsToTExprNodes(Lists.newArrayList(value)))
                    .collect(Collectors.toList());
        }

        if (!inKeysExprNodes.isEmpty()) {
            tPartition.setIn_keys(inKeysExprNodes);
        }
    }

    private void setRangeKeys(RangePartitionInfo rangePartitionInfo, Partition partition,
                              TOlapTablePartition tPartition) {
        int partColNum = rangePartitionInfo.getPartitionColumns().size();
        Range<PartitionKey> range = rangePartitionInfo.getRange(partition.getId());
        // set start keys
        if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
            for (int i = 0; i < partColNum; i++) {
                tPartition.addToStart_keys(
                        range.lowerEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
            }
        }
        // set end keys
        if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
            for (int i = 0; i < partColNum; i++) {
                tPartition.addToEnd_keys(
                        range.upperEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
            }
        }

        if (partition.getName().startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
            tPartition.setIs_shadow_partition(true);
        }
    }

    private void setIndexAndBucketNums(Map<Long, List<AssociateTableMeta>> idToAssociatedMetas,
                                       Partition partition,
                                       TOlapTablePartition tPartition) {
        if (idToAssociatedMetas == null || idToAssociatedMetas.isEmpty()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                        index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                tPartition.setNum_buckets(index.getTablets().size());
            }
        } else {
            for (AssociateTableMeta associateTableMeta: idToAssociatedMetas.get(partition.getId())) {
                if (associateTableMeta.getIndexMeta().isLogical()) {
                    // Attach the associated table's tablet indexes' into the partition thrift.
                    Partition targetPartition = associateTableMeta.getPartition();
                    MaterializedIndexMeta indexMeta = associateTableMeta.getIndexMeta();
                    MaterializedIndex targetIndex = targetPartition.getBaseIndex();
                    tPartition.addToIndexes(new TOlapTableIndexTablets(indexMeta.getIndexId(), Lists.newArrayList(
                            targetIndex.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                    long targetPartitionId = targetPartition.getId();
                    tPartition.putToAssociated_partition_ids(indexMeta.getIndexId(), targetPartitionId);
                } else {
                    for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                                index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
                        tPartition.setNum_buckets(index.getTablets().size());
                        if (idToAssociatedMetas.containsKey(partition.getId())) {
                            tPartition.putToAssociated_partition_ids(index.getId(), partition.getId());
                        }
                    }
                }
            }
        }
    }

    private DistributionInfo setDistributedColumns(TOlapTablePartitionParam partitionParam,
                                                   DistributionInfo selectedDistInfo,
                                                   Partition partition, OlapTable table) throws UserException {
        DistributionInfo distInfo = partition.getDistributionInfo();
        if (selectedDistInfo == null) {
            partitionParam.setDistributed_columns(getDistColumns(distInfo, table));
            return distInfo;
        } else {
            if (selectedDistInfo.getType() != distInfo.getType()) {
                throw new UserException("different distribute types in two different partitions, type1="
                        + selectedDistInfo.getType() + ", type2=" + distInfo.getType());
            }
        }
        return selectedDistInfo;
    }

    private TOlapTableLocationParam createLocation(OlapTable table) throws UserException {
        return createLocation(null, table);
    }

    private TOlapTableLocationParam createLocation(Map<Long, List<AssociateTableMeta>> idToAssociatedMetas,
                                                   OlapTable table) throws UserException {
        TOlapTableLocationParam locationParam = new TOlapTableLocationParam();
        // replica -> path hash
        Multimap<Long, Long> allBePathsMap = HashMultimap.create();
        Map<Long, Long> bePrimaryMap = new HashMap<>();
        SystemInfoService infoService = GlobalStateMgr.getCurrentState()
                .getOrCreateSystemInfo(clusterId);

        if (idToAssociatedMetas == null || idToAssociatedMetas.isEmpty()) {
            for (Long partitionId : partitionIds) {
                Partition partition = table.getPartition(partitionId);
                int quorum = table.getPartitionInfo().getQuorumNum(partition.getId(), table.writeQuorum());
                for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    createIndexLocation(infoService, table, index, allBePathsMap, bePrimaryMap, quorum, locationParam);
                }
            }
        } else {
            for (Long partitionId : partitionIds) {
                Preconditions.checkState(idToAssociatedMetas.containsKey(partitionId));
                for (AssociateTableMeta associateTableMeta : idToAssociatedMetas.get(partitionId)) {
                    if (associateTableMeta.getIndexMeta().isLogical()) {
                        OlapTable targetTable = associateTableMeta.getTargetTable();
                        // add associated tables partition
                        Partition targetPartition = associateTableMeta.getPartition();
                        int targetQuorum = targetTable.getPartitionInfo().getQuorumNum(targetPartition.getId(), targetTable.writeQuorum());
                        MaterializedIndex targetIndex = targetPartition.getBaseIndex();
                        createIndexLocation(infoService, targetTable, targetIndex, allBePathsMap, bePrimaryMap, targetQuorum, locationParam);
                    } else {
                        Partition partition = table.getPartition(partitionId);
                        int quorum = table.getPartitionInfo().getQuorumNum(partition.getId(), table.writeQuorum());
                        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                            createIndexLocation(infoService, table, index, allBePathsMap, bePrimaryMap, quorum, locationParam);
                        }
                    }
                }
            }
        }

        // check if disk capacity reach limit
        // this is for load process, so use high watermark to check
        Status st = GlobalStateMgr.getCurrentSystemInfo().checkExceedDiskCapacityLimit(allBePathsMap, true);
        if (!st.ok()) {
            throw new DdlException(st.getErrorMsg());
        }
        return locationParam;
    }

    private void createIndexLocation(SystemInfoService infoService,
                                     OlapTable table, MaterializedIndex index,
                                     Multimap<Long, Long> allBePathsMap,
                                     Map<Long, Long> bePrimaryMap,
                                     int quorum,
                                     TOlapTableLocationParam locationParam) throws UserException {
        for (Tablet tablet : index.getTablets()) {
            if (table.isCloudNativeTableOrMaterializedView()) {
                Warehouse warehouse = GlobalStateMgr.getCurrentWarehouseMgr().getDefaultWarehouse();
                long workerGroupId = warehouse.getAnyAvailableCluster().getWorkerGroupId();
                locationParam.addToTablets(new TTabletLocation(tablet.getId(),
                        Lists.newArrayList(((LakeTablet) tablet).getPrimaryComputeNodeId(workerGroupId))));
            } else {
                // we should ensure the replica backend is alive
                // otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                LocalTablet localTablet = (LocalTablet) tablet;
                Multimap<Replica, Long> bePathsMap =
                        localTablet.getNormalReplicaBackendPathMap(table.getClusterId());
                if (bePathsMap.keySet().size() < quorum) {
                    throw new UserException(InternalErrorCode.REPLICA_FEW_ERR,
                            "Tablet lost replicas. Check if any backend is down or not. tablet_id: "
                                    + tablet.getId() + ", backends: " +
                                    Joiner.on(",").join(localTablet.getBackends()));
                }

                List<Replica> replicas = Lists.newArrayList(bePathsMap.keySet());

                if (enableReplicatedStorage) {
                    int lowUsageIndex = -1;
                    for (int i = 0; i < replicas.size(); i++) {
                        Replica replica = replicas.get(i);
                        if (lowUsageIndex == -1 && !replica.getLastWriteFail()
                                && !infoService.getBackend(replica.getBackendId()).getLastWriteFail()) {
                            lowUsageIndex = i;
                        }
                        if (lowUsageIndex != -1
                                && bePrimaryMap.getOrDefault(replica.getBackendId(), (long) 0) < bePrimaryMap
                                .getOrDefault(replicas.get(lowUsageIndex).getBackendId(), (long) 0)
                                && !replica.getLastWriteFail()
                                && !infoService.getBackend(replica.getBackendId()).getLastWriteFail()) {
                            lowUsageIndex = i;
                        }
                    }

                    if (lowUsageIndex != -1) {
                        bePrimaryMap.put(replicas.get(lowUsageIndex).getBackendId(),
                                bePrimaryMap.getOrDefault(replicas.get(lowUsageIndex).getBackendId(), (long) 0)
                                        + 1);
                        // replicas[0] will be the primary replica
                        Collections.swap(replicas, 0, lowUsageIndex);
                    } else {
                        LOG.warn("Tablet {} replicas {} all has write fail flag", tablet.getId(), replicas);
                    }
                }

                locationParam
                        .addToTablets(
                                new TTabletLocation(tablet.getId(), replicas.stream().map(Replica::getBackendId)
                                        .collect(Collectors.toList())));
                for (Map.Entry<Replica, Long> entry : bePathsMap.entries()) {
                    allBePathsMap.put(entry.getKey().getBackendId(), entry.getValue());
                }
            }
        }
    }

    public boolean canUsePipeLine() {
        return Config.enable_pipeline_load && enablePipelineLoad;
    }

    public int getClusterId() {
        return clusterId;
    }

    public OlapTable getDstTable() {
        return dstTable;
    }

    public void setDstTable(OlapTable table) {
        this.dstTable = table;
    }

    public void setPartitionIds(List<Long> partitionIds) {
        this.partitionIds = partitionIds;
    }

    public TupleDescriptor getTupleDescriptor() {
        return tupleDescriptor;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public TWriteQuorumType getWriteQuorum() {
        return writeQuorum;
    }

    public boolean isEnableReplicatedStorage() {
        return enableReplicatedStorage;
    }

    public boolean missAutoIncrementColumn() {
        return this.missAutoIncrementColumn;
    }
}

