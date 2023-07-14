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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/OlapTable.java

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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.alter.AlterJobV2Builder;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.OlapTableAlterJobV2Builder;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.backup.Status;
import com.starrocks.backup.Status.ErrCode;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.LocalTablet.TabletStatus;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.Partition.PartitionState;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.clone.TabletSchedCtx;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.Pair;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.DropAutoIncrementMapTask;
import com.starrocks.task.DropReplicaTask;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TOlapTable;
import com.starrocks.thrift.TStorageFormat;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TWriteQuorumType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.Adler32;

/**
 * Internal representation of tableFamilyGroup-related metadata. A OlaptableFamilyGroup contains several tableFamily.
 * Note: when you add a new olap table property, you should modify TableProperty class
 * ATTN: serialize by gson is used by MaterializedView
 */
public class OlapTable extends Table {
    private static final Logger LOG = LogManager.getLogger(OlapTable.class);

    public enum OlapTableState {
        NORMAL,
        ROLLUP,
        SCHEMA_CHANGE,
        @Deprecated
        BACKUP,
        RESTORE,
        RESTORE_WITH_LOAD,
        /*
         * this state means table is under PENDING alter operation(SCHEMA_CHANGE or ROLLUP), and is not
         * stable. The tablet scheduler will continue fixing the tablets of this table. And the state will
         * change back to SCHEMA_CHANGE or ROLLUP after table is stable, and continue doing alter operation.
         * This state is an in-memory state and no need to persist.
         */
        WAITING_STABLE
    }

    @SerializedName(value = "clusterId")
    @Deprecated
    protected int clusterId;

    @SerializedName(value = "state")
    protected OlapTableState state;

    // index id -> index meta
    @SerializedName(value = "indexIdToMeta")
    protected Map<Long, MaterializedIndexMeta> indexIdToMeta = Maps.newHashMap();
    // index name -> index id
    @SerializedName(value = "indexNameToId")
    protected Map<String, Long> indexNameToId = Maps.newHashMap();

    @SerializedName(value = "keysType")
    protected KeysType keysType;

    @SerializedName(value = "partitionInfo")
    protected PartitionInfo partitionInfo;

    @SerializedName(value = "idToPartition")
    protected Map<Long, Partition> idToPartition = new HashMap<>();
    protected Map<String, Partition> nameToPartition = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    @SerializedName(value = "defaultDistributionInfo")
    protected DistributionInfo defaultDistributionInfo;

    // all info about temporary partitions are save in "tempPartitions"
    @SerializedName(value = "tempPartitions")
    protected TempPartitions tempPartitions = new TempPartitions();

    // bloom filter columns
    @SerializedName(value = "bfColumns")
    protected Set<String> bfColumns;

    @SerializedName(value = "bfFpp")
    protected double bfFpp;

    @SerializedName(value = "colocateGroup")
    protected String colocateGroup;

    @SerializedName(value = "colocateMv")
    protected Set<String> colocateMaterializedViewNames = Sets.newHashSet();
    @SerializedName(value = "isInColocateMvGroup")
    protected boolean isInColocateMvGroup = false;

    @SerializedName(value = "indexes")
    protected TableIndexes indexes;

    // In former implementation, base index id is same as table id.
    // But when refactoring the process of alter table job, we find that
    // using same id is not suitable for our new framework.
    // So we add this 'baseIndexId' to explicitly specify the base index id,
    // which should be different with table id.
    // The init value is -1, which means there is not partition and index at all.
    @SerializedName(value = "baseIndexId")
    protected long baseIndexId = -1;

    @SerializedName(value = "tableProperty")
    protected TableProperty tableProperty;

    protected BinlogConfig curBinlogConfig;

    // After ensuring that all binlog config of tablets in BE have taken effect,
    // apply for a transaction id as binlogtxnId.
    // The purpose is to ensure that in the case of concurrent imports,
    // need to wait for the completion of concurrent imports,
    // that is, all transactions which id is smaller than binlogTxnId have been finished/aborted,
    // then binlog is available
    protected long binlogTxnId = -1;

    // Record the alter, schema change, MV update time
    public AtomicLong lastSchemaUpdateTime = new AtomicLong(-1);
    // Record the start and end time for data load version update phase
    public AtomicLong lastVersionUpdateStartTime = new AtomicLong(-1);
    public AtomicLong lastVersionUpdateEndTime = new AtomicLong(0);

    public OlapTable() {
        this(TableType.OLAP);
    }

    public OlapTable(TableType type) {
        // for persist
        super(type);

        this.clusterId = GlobalStateMgr.getCurrentState().getClusterId();

        this.bfColumns = null;
        this.bfFpp = 0;

        this.colocateGroup = null;

        this.indexes = null;

        this.tableProperty = null;
    }

    public OlapTable(long id, String tableName, List<Column> baseSchema, KeysType keysType,
                     PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo) {
        this(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, null);
    }

    public OlapTable(long id, String tableName, List<Column> baseSchema, KeysType keysType,
                     PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo, TableIndexes indexes) {
        this(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo,
                GlobalStateMgr.getCurrentState().getClusterId(), indexes, TableType.OLAP);
    }

    public OlapTable(long id, String tableName, List<Column> baseSchema, KeysType keysType,
                     PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo,
                     int clusterId, TableIndexes indexes) {
        this(id, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo,
                clusterId, indexes, TableType.OLAP);
    }

    public OlapTable(long id, String tableName, List<Column> baseSchema, KeysType keysType,
                     PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo,
                     int clusterId, TableIndexes indexes, TableType tableType) {
        super(id, tableName, tableType, baseSchema);

        this.clusterId = clusterId;
        this.state = OlapTableState.NORMAL;

        this.keysType = keysType;
        this.partitionInfo = partitionInfo;

        this.defaultDistributionInfo = defaultDistributionInfo;

        this.bfColumns = null;
        this.bfFpp = 0;

        this.colocateGroup = null;

        this.indexes = indexes;

        this.tableProperty = null;
    }

    // Only Copy necessary metadata for query.
    // We don't do deep copy, because which is very expensive;
    public void copyOnlyForQuery(OlapTable olapTable) {
        olapTable.id = this.id;
        olapTable.name = this.name;
        olapTable.fullSchema = Lists.newArrayList(this.fullSchema);
        olapTable.nameToColumn = Maps.newHashMap(this.nameToColumn);
        olapTable.relatedMaterializedViews = Sets.newHashSet(this.relatedMaterializedViews);
        olapTable.state = this.state;
        olapTable.indexNameToId = Maps.newHashMap(this.indexNameToId);
        olapTable.indexIdToMeta = Maps.newHashMap(this.indexIdToMeta);
        olapTable.keysType = this.keysType;
        olapTable.partitionInfo = new PartitionInfo();
        if (this.partitionInfo instanceof RangePartitionInfo) {
            olapTable.partitionInfo = new RangePartitionInfo((RangePartitionInfo) this.partitionInfo);
        } else if (this.partitionInfo instanceof SinglePartitionInfo) {
            olapTable.partitionInfo = this.partitionInfo;
        }
        olapTable.defaultDistributionInfo = this.defaultDistributionInfo;
        Map<Long, Partition> idToPartitions = new HashMap<>(this.idToPartition.size());
        Map<String, Partition> nameToPartitions = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<Long, Partition> kv : this.idToPartition.entrySet()) {
            Partition copiedPartition = kv.getValue().shallowCopy();
            idToPartitions.put(kv.getKey(), copiedPartition);
            nameToPartitions.put(kv.getValue().getName(), copiedPartition);
        }
        olapTable.idToPartition = idToPartitions;
        olapTable.nameToPartition = nameToPartitions;
        olapTable.baseIndexId = this.baseIndexId;
        if (this.tableProperty != null) {
            olapTable.tableProperty = this.tableProperty.copy();
        }
    }

    public BinlogConfig getCurBinlogConfig() {
        if (tableProperty != null) {
            return tableProperty.getBinlogConfig();
        }
        return null;
    }

    public void setCurBinlogConfig(BinlogConfig curBinlogConfig) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(Maps.newHashMap());
        }
        tableProperty.modifyTableProperties(curBinlogConfig.toProperties());
        tableProperty.setBinlogConfig(curBinlogConfig);
    }

    public boolean containsBinlogConfig() {
        if (tableProperty == null ||
                tableProperty.getBinlogConfig() == null ||
                tableProperty.getBinlogConfig().getVersion() == BinlogConfig.INVALID) {
            return false;
        }
        return true;
    }

    public long getBinlogTxnId() {
        return binlogTxnId;
    }

    public void setBinlogTxnId(long binlogTxnId) {
        this.binlogTxnId = binlogTxnId;
    }

    public void setTableProperty(TableProperty tableProperty) {
        this.tableProperty = tableProperty;
    }

    public TableProperty getTableProperty() {
        return this.tableProperty;
    }

    public boolean dynamicPartitionExists() {
        return tableProperty != null
                && tableProperty.getDynamicPartitionProperty() != null
                && tableProperty.getDynamicPartitionProperty().isExist();
    }

    public void setBaseIndexId(long baseIndexId) {
        this.baseIndexId = baseIndexId;
    }

    public long getBaseIndexId() {
        return baseIndexId;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setState(OlapTableState state) {
        this.state = state;
    }

    public OlapTableState getState() {
        return state;
    }

    public List<Index> getIndexes() {
        if (indexes == null) {
            return Lists.newArrayList();
        }
        return indexes.getIndexes();
    }

    public void checkAndSetName(String newName, boolean onlyCheck) throws DdlException {
        // check if rollup has same name
        for (String idxName : getIndexNameToId().keySet()) {
            if (idxName.equals(newName)) {
                throw new DdlException("New name conflicts with rollup index name: " + idxName);
            }
        }
        if (!onlyCheck) {
            setName(newName);
        }
    }

    public void setName(String newName) {
        // change name in indexNameToId
        if (this.name != null) {
            long baseIndexId = indexNameToId.remove(this.name);
            indexNameToId.put(newName, baseIndexId);
        }

        // change name
        this.name = newName;

        // change single partition name
        if (this.partitionInfo != null && this.partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            if (getPartitions().stream().findFirst().isPresent()) {
                Optional<Partition> optPartition = getPartitions().stream().findFirst();
                Preconditions.checkState(optPartition.isPresent());
                Partition partition = optPartition.get();
                partition.setName(newName);
                nameToPartition.clear();
                nameToPartition.put(newName, partition);
            }
        }

        // change ExpressionRangePartitionInfo
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Preconditions.checkState(expressionRangePartitionInfo.getPartitionExprs().size() == 1);
            expressionRangePartitionInfo.renameTableName(newName);
        }
    }

    public boolean hasMaterializedIndex(String indexName) {
        return indexNameToId.containsKey(indexName);
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion,
                             int schemaHash, short shortKeyColumnCount, TStorageType storageType, KeysType keysType) {
        setIndexMeta(indexId, indexName, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType, keysType,
                null, null);
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion,
                             int schemaHash, short shortKeyColumnCount, TStorageType storageType, KeysType keysType,
                             OriginStatement origStmt) {
        setIndexMeta(indexId, indexName, schema, schemaVersion, schemaHash, shortKeyColumnCount, storageType, keysType,
                origStmt, null);
    }

    public void setIndexMeta(long indexId, String indexName, List<Column> schema, int schemaVersion,
                             int schemaHash, short shortKeyColumnCount, TStorageType storageType, KeysType keysType,
                             OriginStatement origStmt, List<Integer> sortColumns) {
        // Nullable when meta comes from schema change log replay.
        // The replay log only save the index id, so we need to get name by id.
        if (indexName == null) {
            indexName = getIndexNameById(indexId);
            Preconditions.checkState(indexName != null);
        }
        // Nullable when meta is less then VERSION_74
        if (keysType == null) {
            keysType = this.keysType;
        }
        // Nullable when meta comes from schema change
        if (storageType == null) {
            MaterializedIndexMeta oldIndexMeta = indexIdToMeta.get(indexId);
            Preconditions.checkState(oldIndexMeta != null);
            storageType = oldIndexMeta.getStorageType();
            Preconditions.checkState(storageType != null);
        } else {
            // The new storage type must be TStorageType.COLUMN
            Preconditions.checkState(storageType == TStorageType.COLUMN);
        }

        MaterializedIndexMeta indexMeta = new MaterializedIndexMeta(indexId, schema, schemaVersion,
                schemaHash, shortKeyColumnCount, storageType, keysType, origStmt, sortColumns);
        indexIdToMeta.put(indexId, indexMeta);
        indexNameToId.put(indexName, indexId);
    }

    public boolean hasMaterializedView() {
        Optional<Partition> partition = idToPartition.values().stream().findFirst();
        return partition.map(Partition::hasMaterializedView).orElse(false);
    }

    // rebuild the full schema of table
    // the order of columns in fullSchema is meaningless
    public void rebuildFullSchema() {
        fullSchema.clear();
        nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column baseColumn : indexIdToMeta.get(baseIndexId).getSchema()) {
            fullSchema.add(baseColumn);
            nameToColumn.put(baseColumn.getName(), baseColumn);
        }
        for (MaterializedIndexMeta indexMeta : indexIdToMeta.values()) {
            for (Column column : indexMeta.getSchema()) {
                if (!nameToColumn.containsKey(column.getName())) {
                    fullSchema.add(column);
                    nameToColumn.put(column.getName(), column);
                }
            }
        }
        LOG.debug("after rebuild full schema. table {}, schema: {}", id, fullSchema);
    }

    public boolean deleteIndexInfo(String indexName) {
        if (!indexNameToId.containsKey(indexName)) {
            return false;
        }

        long indexId = this.indexNameToId.remove(indexName);
        this.indexIdToMeta.remove(indexId);
        // Some column of deleted index should be removed during `deleteIndexInfo` such as `mv_bitmap_union_c1`
        // If deleted index id == base index id, the schema will not be rebuilt.
        // The reason is that the base index has been removed from indexIdToMeta while the new base index hasn't changed.
        // The schema could not be rebuild in here with error base index id.
        if (indexId != baseIndexId) {
            rebuildFullSchema();
        }
        return true;
    }

    public Map<String, Long> getIndexNameToId() {
        return indexNameToId;
    }

    public Long getIndexIdByName(String indexName) {
        return indexNameToId.get(indexName);
    }

    public Long getSegmentV2FormatIndexId() {
        String v2RollupIndexName = MaterializedViewHandler.NEW_STORAGE_FORMAT_INDEX_NAME_PREFIX + getName();
        return indexNameToId.get(v2RollupIndexName);
    }

    public String getIndexNameById(long indexId) {
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            if (entry.getValue() == indexId) {
                return entry.getKey();
            }
        }
        return null;
    }

    public Map<Long, MaterializedIndexMeta> getVisibleIndexIdToMeta() {
        Map<Long, MaterializedIndexMeta> visibleMVs = Maps.newHashMap();
        List<MaterializedIndex> mvs = getVisibleIndex();
        for (MaterializedIndex mv : mvs) {
            visibleMVs.put(mv.getId(), indexIdToMeta.get(mv.getId()));
        }
        return visibleMVs;
    }

    public List<MaterializedIndex> getVisibleIndex() {
        Optional<Partition> firstPartition = idToPartition.values().stream().findFirst();
        if (firstPartition.isPresent()) {
            Partition partition = firstPartition.get();
            return partition.getMaterializedIndices(IndexExtState.VISIBLE);
        }
        return Lists.newArrayList();
    }

    public Column getVisibleColumn(String columnName) {
        for (MaterializedIndexMeta meta : getVisibleIndexIdToMeta().values()) {
            for (Column column : meta.getSchema()) {
                if (column.getName().equalsIgnoreCase(columnName)) {
                    return column;
                }
            }
        }
        return null;
    }

    // this is only for schema change.
    public void renameIndexForSchemaChange(String name, String newName) {
        long idxId = indexNameToId.remove(name);
        indexNameToId.put(newName, idxId);
    }

    public void renameColumnNamePrefix(long idxId) {
        List<Column> columns = indexIdToMeta.get(idxId).getSchema();
        for (Column column : columns) {
            column.setName(Column.removeNamePrefix(column.getName()));
        }
    }

    public Status resetIdsForRestore(GlobalStateMgr globalStateMgr, Database db, int restoreReplicationNum) {
        // table id
        id = globalStateMgr.getNextId();

        // copy an origin index id to name map
        Map<Long, String> origIdxIdToName = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            origIdxIdToName.put(entry.getValue(), entry.getKey());
        }

        // reset all 'indexIdToXXX' map
        for (Map.Entry<Long, String> entry : origIdxIdToName.entrySet()) {
            long newIdxId = globalStateMgr.getNextId();
            if (entry.getValue().equals(name)) {
                // base index
                baseIndexId = newIdxId;
            }
            indexIdToMeta.put(newIdxId, indexIdToMeta.remove(entry.getKey()));
            indexIdToMeta.get(newIdxId).setIndexIdForRestore(newIdxId);
            indexNameToId.put(entry.getValue(), newIdxId);
        }

        // generate a partition name to id map
        Map<String, Long> origPartNameToId = Maps.newHashMap();
        for (Partition partition : idToPartition.values()) {
            origPartNameToId.put(partition.getName(), partition.getId());
        }

        // reset partition info and idToPartition map
        if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            for (Map.Entry<String, Long> entry : origPartNameToId.entrySet()) {
                long newPartId = globalStateMgr.getNextId();
                rangePartitionInfo.idToDataProperty.put(newPartId,
                        rangePartitionInfo.idToDataProperty.remove(entry.getValue()));
                rangePartitionInfo.idToReplicationNum.remove(entry.getValue());
                rangePartitionInfo.idToReplicationNum.put(newPartId,
                        (short) restoreReplicationNum);
                rangePartitionInfo.getIdToRange(false).put(newPartId,
                        rangePartitionInfo.getIdToRange(false).remove(entry.getValue()));

                rangePartitionInfo.idToInMemory
                        .put(newPartId, rangePartitionInfo.idToInMemory.remove(entry.getValue()));
                idToPartition.put(newPartId, idToPartition.remove(entry.getValue()));
            }
        } else {
            // Single partitioned
            long newPartId = globalStateMgr.getNextId();
            for (Map.Entry<String, Long> entry : origPartNameToId.entrySet()) {
                partitionInfo.idToDataProperty.put(newPartId, partitionInfo.idToDataProperty.remove(entry.getValue()));
                partitionInfo.idToReplicationNum.remove(entry.getValue());
                partitionInfo.idToReplicationNum.put(newPartId, (short) restoreReplicationNum);
                partitionInfo.idToInMemory.put(newPartId, partitionInfo.idToInMemory.remove(entry.getValue()));
                idToPartition.put(newPartId, idToPartition.remove(entry.getValue()));
            }
        }

        // for each partition, reset rollup index map
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            Partition partition = entry.getValue();
            for (Map.Entry<Long, String> entry2 : origIdxIdToName.entrySet()) {
                MaterializedIndex idx = partition.getIndex(entry2.getKey());
                long newIdxId = indexNameToId.get(entry2.getValue());
                int schemaHash = indexIdToMeta.get(newIdxId).getSchemaHash();
                idx.setIdForRestore(newIdxId);
                if (newIdxId != baseIndexId) {
                    // not base table, reset
                    partition.deleteRollupIndex(entry2.getKey());
                    partition.createRollupIndex(idx);
                }

                // generate new tablets in origin tablet order
                int tabletNum = idx.getTablets().size();
                idx.clearTabletsForRestore();
                Status status = createTabletsForRestore(tabletNum, idx, globalStateMgr,
                        partitionInfo.getReplicationNum(entry.getKey()), partition.getVisibleVersion(),
                        schemaHash, partition.getId(), partition.getShardGroupId());
                if (!status.ok()) {
                    return status;
                }
            }

            // reset partition id
            partition.setIdForRestore(entry.getKey());
        }

        // reset replication number for olaptable
        setReplicationNum((short) restoreReplicationNum);

        return Status.OK;
    }

    public Status createTabletsForRestore(int tabletNum, MaterializedIndex index, GlobalStateMgr globalStateMgr,
                                          int replicationNum, long version, int schemaHash,
                                          long partitionId, long shardGroupId) {
        for (int i = 0; i < tabletNum; i++) {
            long newTabletId = globalStateMgr.getNextId();
            LocalTablet newTablet = new LocalTablet(newTabletId);
            index.addTablet(newTablet, null /* tablet meta */, false/* update inverted index*/);

            // replicas
            List<Long> beIds = GlobalStateMgr.getCurrentSystemInfo()
                    .seqChooseBackendIds(replicationNum, true, true);
            if (CollectionUtils.isEmpty(beIds)) {
                return new Status(ErrCode.COMMON_ERROR, "failed to find "
                        + replicationNum
                        + " different hosts to create table: " + name);
            }
            for (Long beId : beIds) {
                long newReplicaId = globalStateMgr.getNextId();
                Replica replica = new Replica(newReplicaId, beId, ReplicaState.NORMAL,
                        version, schemaHash);
                newTablet.addReplica(replica, false/* update inverted index*/);
            }
        }
        return Status.OK;
    }

    public Map<Long, MaterializedIndexMeta> getIndexIdToMeta() {
        return indexIdToMeta;
    }

    public Map<Long, MaterializedIndexMeta> getCopiedIndexIdToMeta() {
        return new HashMap<>(indexIdToMeta);
    }

    public MaterializedIndexMeta getIndexMetaByIndexId(long indexId) {
        return indexIdToMeta.get(indexId);
    }

    public List<Long> getIndexIdListExceptBaseIndex() {
        List<Long> result = Lists.newArrayList();
        for (Long indexId : indexIdToMeta.keySet()) {
            if (indexId != baseIndexId) {
                result.add(indexId);
            }
        }
        return result;
    }

    // schema
    public Map<Long, List<Column>> getIndexIdToSchema() {
        Map<Long, List<Column>> result = Maps.newHashMap();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getSchema());
        }
        return result;
    }

    public List<Column> getSchemaByIndexId(Long indexId) {
        MaterializedIndexMeta meta = indexIdToMeta.get(indexId);
        if (meta != null) {
            return meta.getSchema();
        }
        return new ArrayList<Column>();
    }

    public List<Column> getKeyColumns() {
        return getColumns().stream().filter(Column::isKey).collect(Collectors.toList());
    }

    public List<Column> getKeyColumnsByIndexId(Long indexId) {
        ArrayList<Column> keyColumns = Lists.newArrayList();
        List<Column> allColumns = this.getSchemaByIndexId(indexId);
        for (Column column : allColumns) {
            if (column.isKey()) {
                keyColumns.add(column);
            }
        }

        return keyColumns;
    }

    // schemaHash
    public Map<Long, Integer> getIndexIdToSchemaHash() {
        Map<Long, Integer> result = Maps.newHashMap();
        for (Map.Entry<Long, MaterializedIndexMeta> entry : indexIdToMeta.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getSchemaHash());
        }
        return result;
    }

    public int getSchemaHashByIndexId(Long indexId) {
        MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
        if (indexMeta == null) {
            return -1;
        }
        return indexMeta.getSchemaHash();
    }

    public TStorageType getStorageTypeByIndexId(Long indexId) {
        MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
        if (indexMeta == null) {
            return TStorageType.COLUMN;
        }
        return indexMeta.getStorageType();
    }

    public KeysType getKeysType() {
        return keysType;
    }

    public KeysType getKeysTypeByIndexId(long indexId) {
        MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
        Preconditions.checkNotNull(indexMeta, "index id:" + indexId + " meta is null");
        return indexMeta.getKeysType();
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void sendDropAutoIncrementMapTask() {
        Set<Long> fullBackendId = Sets.newHashSet();
        for (Partition partition : this.getAllPartitions()) {
            List<MaterializedIndex> allIndices =
                    partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
            for (MaterializedIndex materializedIndex : allIndices) {
                for (Tablet tablet : materializedIndex.getTablets()) {
                    List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
                    for (Replica replica : replicas) {
                        long backendId = replica.getBackendId();
                        fullBackendId.add(backendId);
                    }
                }
            }
        }

        AgentBatchTask batchTask = new AgentBatchTask();

        for (long backendId : fullBackendId) {
            DropAutoIncrementMapTask dropAutoIncrementMapTask = new DropAutoIncrementMapTask(backendId, this.id,
                    GlobalStateMgr.getCurrentState().getNextId());
            batchTask.addTask(dropAutoIncrementMapTask);
        }

        if (batchTask.getTaskNum() > 0) {
            MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<>(batchTask.getTaskNum());
            for (AgentTask task : batchTask.getAllTasks()) {
                latch.addMark(task.getBackendId(), -1L);
                ((DropAutoIncrementMapTask) task).setLatch(latch);
                AgentTaskQueue.addTask(task);
            }
            AgentTaskExecutor.submit(batchTask);

            // estimate timeout, at most 10 min
            long timeout = 60L * 1000L;
            boolean ok = false;
            try {
                LOG.info("begin to send drop auto increment map tasks to BE, total {} tasks. timeout: {}",
                        batchTask.getTaskNum(), timeout);
                ok = latch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
            }

            if (!ok) {
                LOG.warn("drop auto increment map tasks failed");
            }

        }
    }

    // partition Name -> Range
    public Map<String, Range<PartitionKey>> getRangePartitionMap() {
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        Map<String, Range<PartitionKey>> rangePartitionMap = Maps.newHashMap();
        for (Map.Entry<Long, Partition> partitionEntry : idToPartition.entrySet()) {
            Long partitionId = partitionEntry.getKey();
            String partitionName = partitionEntry.getValue().getName();
            // FE and BE at the same time ignore the hidden partition at the same time
            if (partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                continue;
            }
            rangePartitionMap.put(partitionName, rangePartitionInfo.getRange(partitionId));
        }
        return rangePartitionMap;
    }

    public List<String> getPartitionColumnNames() {
        List<String> partitionColumnNames = Lists.newArrayList();
        if (partitionInfo instanceof SinglePartitionInfo) {
            return partitionColumnNames;
        } else if (partitionInfo instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            for (Column column : partitionColumns) {
                partitionColumnNames.add(column.getName());
            }
            return partitionColumnNames;
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            List<Column> partitionColumns = listPartitionInfo.getPartitionColumns();
            for (Column column : partitionColumns) {
                partitionColumnNames.add(column.getName());
            }
            return partitionColumnNames;
        }
        throw new SemanticException("unknown partition info:" + partitionInfo.getClass().getName());
    }

    public void setDefaultDistributionInfo(DistributionInfo distributionInfo) {
        defaultDistributionInfo = distributionInfo;
    }

    public DistributionInfo getDefaultDistributionInfo() {
        return defaultDistributionInfo;
    }

    @Override
    public Set<String> getDistributionColumnNames() {
        Set<String> distributionColumnNames = Sets.newHashSet();
        if (defaultDistributionInfo instanceof RandomDistributionInfo) {
            return distributionColumnNames;
        }
        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) defaultDistributionInfo;
        List<Column> partitionColumns = hashDistributionInfo.getDistributionColumns();
        for (Column column : partitionColumns) {
            distributionColumnNames.add(column.getName().toLowerCase());
        }
        return distributionColumnNames;
    }

    public void renamePartition(String partitionName, String newPartitionName) {
        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            // bug fix
            Optional<Partition> optionalPartition = idToPartition.values().stream().findFirst();
            if (optionalPartition.isPresent()) {
                Partition partition = optionalPartition.get();
                partition.setName(newPartitionName);
                nameToPartition.clear();
                nameToPartition.put(newPartitionName, partition);
                LOG.info("rename partition {} in table {}", newPartitionName, name);
            }
        } else {
            Partition partition = nameToPartition.remove(partitionName);
            partition.setName(newPartitionName);
            nameToPartition.put(newPartitionName, partition);
        }
    }

    public void addPartition(Partition partition) {
        idToPartition.put(partition.getId(), partition);
        nameToPartition.put(partition.getName(), partition);
    }

    // This is a private method.
    // Call public "dropPartitionAndReserveTablet" and "dropPartition"
    private void dropPartition(long dbId, String partitionName, boolean isForceDrop, boolean reserveTablets) {
        // 1. If "isForceDrop" is false, the partition will be added to the GlobalStateMgr Recyle bin, and all tablets of this
        //    partition will not be deleted.
        // 2. If "ifForceDrop" is true, the partition will be dropped the immediately, but whether to drop the tablets
        //    of this partition depends on "reserveTablets"
        //    If "reserveTablets" is true, the tablets of this partition will not to delete.
        //    Otherwise, the tablets of this partition will be deleted immediately.
        Partition partition = nameToPartition.get(partitionName);
        if (partition != null) {
            if (partitionInfo.isRangePartition()) {
                idToPartition.remove(partition.getId());
                nameToPartition.remove(partitionName);
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                if (!isForceDrop) {
                    // recycle range partition
                    GlobalStateMgr.getCurrentRecycleBin().recyclePartition(dbId, id, partition,
                            rangePartitionInfo.getRange(partition.getId()),
                            rangePartitionInfo.getDataProperty(partition.getId()),
                            rangePartitionInfo.getReplicationNum(partition.getId()),
                            rangePartitionInfo.getIsInMemory(partition.getId()),
                            rangePartitionInfo.getStorageCacheInfo(partition.getId()));
                } else if (!reserveTablets) {
                    GlobalStateMgr.getCurrentState().onErasePartition(partition);
                }
                // drop partition info
                rangePartitionInfo.dropPartition(partition.getId());
            } else if (partitionInfo.getType() == PartitionType.LIST) {
                ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
                if (!isForceDrop) {
                    throw new SemanticException("List partition does not support recycle bin, " +
                            "you can use force drop to drop it.");
                } else if (!reserveTablets) {
                    idToPartition.remove(partition.getId());
                    nameToPartition.remove(partitionName);
                    GlobalStateMgr.getCurrentState().onErasePartition(partition);
                }
                // drop partition info
                listPartitionInfo.dropPartition(partition.getId());
            }
            GlobalStateMgr.getCurrentAnalyzeMgr().dropPartition(partition.getId());
        }
    }

    public void dropPartitionAndReserveTablet(String partitionName) {
        dropPartition(-1, partitionName, true, true);
    }

    public void dropPartition(long dbId, String partitionName, boolean isForceDrop) {
        dropPartition(dbId, partitionName, isForceDrop, !isForceDrop);
    }

    /*
     * A table may contain both formal and temporary partitions.
     * There are several methods to get the partition of a table.
     * Typically divided into two categories:
     *
     * 1. Get partition by id
     * 2. Get partition by name
     *
     * According to different requirements, the caller may want to obtain
     * a formal partition or a temporary partition. These methods are
     * described below in order to obtain the partition by using the correct method.
     *
     * 1. Get by name
     *
     * This type of request usually comes from a user with partition names. Such as
     * `select * from tbl partition(p1);`.
     * This type of request has clear information to indicate whether to obtain a
     * formal or temporary partition.
     * Therefore, we need to get the partition through this method:
     *
     * `getPartition(String partitionName, boolean isTemp)`
     *
     * To avoid modifying too much code, we leave the `getPartition(String
     * partitionName)`, which is same as:
     *
     * `getPartition(partitionName, false)`
     *
     * 2. Get by id
     *
     * This type of request usually means that the previous step has obtained
     * certain partition ids in some way,
     * so we only need to get the corresponding partition through this method:
     *
     * `getPartition(long partitionId)`.
     *
     * This method will try to get both formal partitions and temporary partitions.
     *
     * 3. Get all partition instances
     *
     * Depending on the requirements, the caller may want to obtain all formal
     * partitions,
     * all temporary partitions, or all partitions. Therefore we provide 3 methods,
     * the caller chooses according to needs.
     *
     * `getPartitions()`
     * `getTempPartitions()`
     * `getAllPartitions()`
     *
     */

    // get partition by name, not including temp partitions
    @Override
    public Partition getPartition(String partitionName) {
        return getPartition(partitionName, false);
    }

    // get partition by name
    public Partition getPartition(String partitionName, boolean isTempPartition) {
        if (isTempPartition) {
            return tempPartitions.getPartition(partitionName);
        } else {
            return nameToPartition.get(partitionName);
        }
    }

    // get partition by id, including temp partitions
    public Partition getPartition(long partitionId) {
        Partition partition = idToPartition.get(partitionId);
        if (partition == null) {
            partition = tempPartitions.getPartition(partitionId);
        }
        return partition;
    }

    // get all partitions except temp partitions
    @Override
    public Collection<Partition> getPartitions() {
        return idToPartition.values();
    }

    public int getNumberOfPartitions() {
        return idToPartition.size();
    }

    // get only temp partitions
    public Collection<Partition> getTempPartitions() {
        return tempPartitions.getAllPartitions();
    }

    // get all partitions including temp partitions
    public Collection<Partition> getAllPartitions() {
        List<Partition> partitions = Lists.newArrayList(idToPartition.values());
        partitions.addAll(tempPartitions.getAllPartitions());
        return partitions;
    }

    public List<Long> getAllPartitionIds() {
        return new ArrayList<>(idToPartition.keySet());
    }

    public Collection<Partition> getRecentPartitions(int recentPartitionNum) {
        List<Partition> partitions = Lists.newArrayList(idToPartition.values());
        Collections.sort(partitions, new Comparator<Partition>() {
            @Override
            public int compare(Partition h1, Partition h2) {
                return (int) (h2.getVisibleVersion() - h1.getVisibleVersion());
            }
        });
        return partitions.subList(0, recentPartitionNum);
    }

    // get all partitions' name except the temp partitions
    public Set<String> getPartitionNames() {
        return Sets.newHashSet(nameToPartition.keySet());
    }

    public Map<String, Range<PartitionKey>> getValidPartitionMap(int lastPartitionNum) throws AnalysisException {
        Map<String, Range<PartitionKey>> rangePartitionMap = getRangePartitionMap();
        // less than 0 means not set
        if (lastPartitionNum < 0) {
            return rangePartitionMap;
        }

        int partitionNum = rangePartitionMap.size();
        if (lastPartitionNum > partitionNum) {
            return rangePartitionMap;
        }

        List<Column> partitionColumns = ((RangePartitionInfo) partitionInfo).getPartitionColumns();
        Column partitionColumn = partitionColumns.get(0);
        Type partitionType = partitionColumn.getType();

        List<Range<PartitionKey>> sortedRange = rangePartitionMap.values().stream()
                .sorted(RangeUtils.RANGE_COMPARATOR).collect(Collectors.toList());
        int startIndex;
        if (partitionType.isNumericType()) {
            startIndex = partitionNum - lastPartitionNum;
        } else if (partitionType.isDateType()) {
            LocalDateTime currentDateTime = LocalDateTime.now();
            PartitionValue currentPartitionValue = new PartitionValue(currentDateTime.format(DateUtils.DATE_FORMATTER_UNIX));
            PartitionKey currentPartitionKey = PartitionKey.createPartitionKey(
                    ImmutableList.of(currentPartitionValue), partitionColumns);
            // For date types, ttl number should not consider future time
            int futurePartitionNum = 0;
            for (int i = sortedRange.size(); i > 0; i--) {
                PartitionKey lowerEndpoint = sortedRange.get(i - 1).lowerEndpoint();
                if (lowerEndpoint.compareTo(currentPartitionKey) > 0) {
                    futurePartitionNum++;
                } else {
                    break;
                }
            }

            if (partitionNum - lastPartitionNum - futurePartitionNum <= 0) {
                return rangePartitionMap;
            } else {
                startIndex = partitionNum - lastPartitionNum - futurePartitionNum;
            }
        } else {
            throw new AnalysisException("Unsupported partition type: " + partitionType);
        }

        LiteralExpr startExpr = sortedRange.get(startIndex).lowerEndpoint().
                getKeys().get(0);
        LiteralExpr endExpr = sortedRange.get(partitionNum - 1).upperEndpoint().getKeys().get(0);
        String start = AnalyzerUtils.parseLiteralExprToDateString(startExpr, 0);
        String end = AnalyzerUtils.parseLiteralExprToDateString(endExpr, 0);

        Map<String, Range<PartitionKey>> result = Maps.newHashMap();
        Range<PartitionKey> rangeToInclude = SyncPartitionUtils.createRange(start, end, partitionColumn);
        for (Map.Entry<String, Range<PartitionKey>> entry : rangePartitionMap.entrySet()) {
            Range<PartitionKey> rangeToCheck = entry.getValue();
            int lowerCmp = rangeToInclude.lowerEndpoint().compareTo(rangeToCheck.upperEndpoint());
            int upperCmp = rangeToInclude.upperEndpoint().compareTo(rangeToCheck.lowerEndpoint());
            if (!(lowerCmp >= 0 || upperCmp <= 0)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public Set<String> getBfColumns() {
        return bfColumns;
    }

    public Set<String> getCopiedBfColumns() {
        if (bfColumns == null) {
            return null;
        }
        return Sets.newHashSet(bfColumns);
    }

    public List<Index> getCopiedIndexes() {
        if (indexes == null) {
            return Lists.newArrayList();
        }
        return indexes.getCopiedIndexes();
    }

    public double getBfFpp() {
        return bfFpp;
    }

    public void setBloomFilterInfo(Set<String> bfColumns, double bfFpp) {
        this.bfColumns = bfColumns;
        this.bfFpp = bfFpp;
    }

    public void setIndexes(List<Index> indexes) {
        if (this.indexes == null) {
            this.indexes = new TableIndexes(null);
        }
        this.indexes.setIndexes(indexes);
    }

    public String getColocateGroup() {
        return colocateGroup;
    }

    public void setColocateGroup(String colocateGroup) {
        this.colocateGroup = colocateGroup;
    }

    public Set<String> getColocateMaterializedViewNames() {
        return colocateMaterializedViewNames;
    }

    public void setColocateMaterializedViewNames(Set<String> colocateMaterializedViewNames) {
        this.colocateMaterializedViewNames = colocateMaterializedViewNames;
    }

    public boolean isInColocateMvGroup() {
        return isInColocateMvGroup;
    }

    public void setInColocateMvGroup(boolean inColocateMvGroup) {
        this.isInColocateMvGroup = inColocateMvGroup;
    }

    public void addColocateMaterializedView(String mvName) {
        colocateMaterializedViewNames.add(mvName);
    }

    // 1. remove the materialized view name from the set colocateMaterializedViewNames
    // 2. the base table will be removed from the colocate group
    // only the currently deleted materialized view is the only colocate mv of the base table
    public void removeColocateMaterializedView(String mvName) {
        if (colocateMaterializedViewNames.contains(mvName)) {
            if (colocateMaterializedViewNames.size() == 1 && isInColocateMvGroup()) {
                ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentColocateIndex();
                colocateTableIndex.removeTable(this.id, null, false /* isReplay */);
                setInColocateMvGroup(false);
                setColocateGroup(null);
            }
            colocateMaterializedViewNames.remove(mvName);
        }
    }

    // this will be called when rollupJobV2 is finished
    public void addTableToColocateGroupIfSet(Long dbId, String rollupIndexName) {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentColocateIndex();
        if (!colocateTableIndex.isColocateTable(this.id) && colocateMaterializedViewNames.contains(rollupIndexName)) {
            String dbName = GlobalStateMgr.getCurrentState().getDb(dbId).getFullName();
            String groupName = dbName + ":" + rollupIndexName;
            try {
                colocateTableIndex.addTableToGroup(dbId, this, groupName, null, false /* isReplay */);
            } catch (DdlException e) {
                // should not happen, just log an error here
                LOG.error(e.getMessage());
            }
            setInColocateMvGroup(true);
            setColocateGroup(groupName);

            ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(this.id);
            List<List<Long>> backendsPerBucketSeq = colocateTableIndex.getBackendsPerBucketSeq(groupId);
            ColocatePersistInfo info =
                    ColocatePersistInfo.createForAddTable(groupId, this.id, backendsPerBucketSeq);
            GlobalStateMgr.getCurrentState().getEditLog().logColocateAddTable(info);
        }
    }

    // when the state of rollupJobV2 is canceled
    // just remove the materialized view from the set
    // for the materialized view is added to the set before the rollupJobV2 running
    public void removeMaterializedViewWhenJobCanceled(String rollupIndexName) {
        colocateMaterializedViewNames.remove(rollupIndexName);
    }

    // when the table is creating new rollup and enter finishing state, should tell be not auto load to new rollup
    // it is used for stream load
    // the caller should get db lock when call this method
    public boolean shouldLoadToNewRollup() {
        return false;
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        TOlapTable tOlapTable = new TOlapTable(getName());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.OLAP_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setOlapTable(tOlapTable);
        return tTableDescriptor;
    }

    public long getRowCount() {
        long rowCount = 0;
        for (Map.Entry<Long, Partition> entry : idToPartition.entrySet()) {
            rowCount += entry.getValue().getBaseIndex().getRowCount();
        }
        return rowCount;
    }

    public int getSignature(int signatureVersion, List<String> partNames, boolean isRestore) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);

        // table name
        adler32.update(name.getBytes(StandardCharsets.UTF_8));
        LOG.debug("signature. table name: {}", name);
        // type
        adler32.update(type.name().getBytes(StandardCharsets.UTF_8));
        LOG.debug("signature. table type: {}", type.name());

        // all indices(should be in order)
        Set<String> indexNames = Sets.newTreeSet();
        indexNames.addAll(indexNameToId.keySet());
        for (String indexName : indexNames) {
            long indexId = indexNameToId.get(indexName);
            adler32.update(indexName.getBytes(StandardCharsets.UTF_8));
            LOG.debug("signature. index name: {}", indexName);
            MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
            // schema hash
            // schema hash will change after finish schema change. It is make no sense
            // that check the schema hash here when doing restore
            if (!isRestore) {
                adler32.update(indexMeta.getSchemaHash());
                LOG.debug("signature. index schema hash: {}", indexMeta.getSchemaHash());
            }
            // short key column count
            adler32.update(indexMeta.getShortKeyColumnCount());
            LOG.debug("signature. index short key: {}", indexMeta.getShortKeyColumnCount());
            // storage type
            adler32.update(indexMeta.getStorageType().name().getBytes(StandardCharsets.UTF_8));
            LOG.debug("signature. index storage type: {}", indexMeta.getStorageType());
        }

        // bloom filter
        if (bfColumns != null && !bfColumns.isEmpty()) {
            for (String bfCol : bfColumns) {
                adler32.update(bfCol.getBytes());
                LOG.debug("signature. bf col: {}", bfCol);
            }
            adler32.update(String.valueOf(bfFpp).getBytes());
            LOG.debug("signature. bf fpp: {}", bfFpp);
        }

        // partition type
        adler32.update(partitionInfo.getType().name().getBytes(StandardCharsets.UTF_8));
        LOG.debug("signature. partition type: {}", partitionInfo.getType().name());
        // partition columns
        if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            adler32.update(Util.schemaHash(0, partitionColumns, null, 0));
            LOG.debug("signature. partition col hash: {}", Util.schemaHash(0, partitionColumns, null, 0));
        }

        // partition and distribution
        Collections.sort(partNames, String.CASE_INSENSITIVE_ORDER);
        for (String partName : partNames) {
            Partition partition = getPartition(partName);
            Preconditions.checkNotNull(partition, partName);
            adler32.update(partName.getBytes(StandardCharsets.UTF_8));
            LOG.debug("signature. partition name: {}", partName);
            DistributionInfo distributionInfo = partition.getDistributionInfo();
            adler32.update(distributionInfo.getType().name().getBytes(StandardCharsets.UTF_8));
            if (distributionInfo.getType() == DistributionInfoType.HASH) {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                adler32.update(Util.schemaHash(0, hashDistributionInfo.getDistributionColumns(), null, 0));
                LOG.debug("signature. distribution col hash: {}",
                        Util.schemaHash(0, hashDistributionInfo.getDistributionColumns(), null, 0));
                adler32.update(hashDistributionInfo.getBucketNum());
                LOG.debug("signature. bucket num: {}", hashDistributionInfo.getBucketNum());
            }
        }

        LOG.debug("signature: {}", Math.abs((int) adler32.getValue()));
        return Math.abs((int) adler32.getValue());
    }

<<<<<<< HEAD
    // get intersect partition names with the given table "anotherTbl". not including temp partitions
=======
    // This function is only used for getting the err msg for restore job
    public List<Pair<Integer, String>> getSignatureSequence(int signatureVersion, List<String> partNames) {
        List<Pair<Integer, String>> checkSumList = Lists.newArrayList();
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);

        // table name
        adler32.update(name.getBytes(StandardCharsets.UTF_8));
        checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "Table name is inconsistent"));
        // type
        adler32.update(type.name().getBytes(StandardCharsets.UTF_8));
        LOG.info("test getBytes", type.name().getBytes(StandardCharsets.UTF_8));
        checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "Table type is inconsistent"));

        // all indices(should be in order)
        Set<String> indexNames = Sets.newTreeSet();
        indexNames.addAll(indexNameToId.keySet());
        for (String indexName : indexNames) {
            long indexId = indexNameToId.get(indexName);
            adler32.update(indexName.getBytes(StandardCharsets.UTF_8));
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "indexName is inconsistent"));
            MaterializedIndexMeta indexMeta = indexIdToMeta.get(indexId);
            // short key column count
            adler32.update(indexMeta.getShortKeyColumnCount());
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "short key column count is inconsistent"));
            // storage type
            adler32.update(indexMeta.getStorageType().name().getBytes(StandardCharsets.UTF_8));
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "storage type is inconsistent"));
        }

        // bloom filter
        if (bfColumns != null && !bfColumns.isEmpty()) {
            for (String bfCol : bfColumns) {
                adler32.update(bfCol.getBytes());
                checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "bloom filter is inconsistent"));
            }
            adler32.update(String.valueOf(bfFpp).getBytes());
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "bloom filter is inconsistent"));
        }

        // partition type
        adler32.update(partitionInfo.getType().name().getBytes(StandardCharsets.UTF_8));
        checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "partition type is inconsistent"));
        // partition columns
        if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            adler32.update(Util.schemaHash(0, partitionColumns, null, 0));
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "partition columns is inconsistent"));
        }

        // partition and distribution
        Collections.sort(partNames, String.CASE_INSENSITIVE_ORDER);
        for (String partName : partNames) {
            Partition partition = getPartition(partName);
            Preconditions.checkNotNull(partition, partName);
            adler32.update(partName.getBytes(StandardCharsets.UTF_8));
            checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "partition name is inconsistent"));
            DistributionInfo distributionInfo = partition.getDistributionInfo();
            adler32.update(distributionInfo.getType().name().getBytes(StandardCharsets.UTF_8));
            if (distributionInfo.getType() == DistributionInfoType.HASH) {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                adler32.update(Util.schemaHash(0, hashDistributionInfo.getDistributionColumns(), null, 0));
                checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "partition distribution col hash is inconsistent"));
                adler32.update(hashDistributionInfo.getBucketNum());
                checkSumList.add(new Pair(Math.abs((int) adler32.getValue()), "bucket num is inconsistent"));
            }
        }

        return checkSumList;
    }

    // get intersect partition names with the given table "anotherTbl". not
    // including temp partitions
>>>>>>> 732a44b11e ([Enhancement] add more error msg for restore if schema check fail (#26794) (#26857))
    public Status getIntersectPartNamesWith(OlapTable anotherTbl, List<String> intersectPartNames) {
        if (this.getPartitionInfo().getType() != anotherTbl.getPartitionInfo().getType()) {
            return new Status(ErrCode.COMMON_ERROR, "Table's partition type is different");
        }

        Set<String> intersect = this.getPartitionNames();
        intersect.retainAll(anotherTbl.getPartitionNames());
        intersectPartNames.addAll(intersect);
        return Status.OK;
    }

    @Override
    public boolean isPartitioned() {
        int numSegs = 0;
        for (Partition part : getPartitions()) {
            numSegs += part.getDistributionInfo().getBucketNum();
            if (numSegs > 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        // state
        Text.writeString(out, state.name());

        // indices' schema
        int counter = indexNameToId.size();
        out.writeInt(counter);
        for (Map.Entry<String, Long> entry : indexNameToId.entrySet()) {
            String indexName = entry.getKey();
            long indexId = entry.getValue();
            Text.writeString(out, indexName);
            out.writeLong(indexId);
            indexIdToMeta.get(indexId).write(out);
        }

        Text.writeString(out, keysType.name());
        Text.writeString(out, partitionInfo.getType().name());
        partitionInfo.write(out);
        Text.writeString(out, defaultDistributionInfo.getType().name());
        defaultDistributionInfo.write(out);

        // partitions
        int partitionCount = idToPartition.size();
        out.writeInt(partitionCount);
        for (Partition partition : idToPartition.values()) {
            partition.write(out);
        }

        // bloom filter columns
        if (bfColumns == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(bfColumns.size());
            for (String bfColumn : bfColumns) {
                Text.writeString(out, bfColumn);
            }
            out.writeDouble(bfFpp);
        }

        //colocateTable
        if (colocateGroup == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, colocateGroup);
        }

        out.writeLong(baseIndexId);

        // write indexes
        if (indexes != null) {
            out.writeBoolean(true);
            indexes.write(out);
        } else {
            out.writeBoolean(false);
        }

        // tableProperty
        if (tableProperty == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            tableProperty.write(out);
        }
        tempPartitions.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        this.state = OlapTableState.valueOf(Text.readString(in));

        // indices's schema
        int counter = in.readInt();
        // tmp index meta list
        List<MaterializedIndexMeta> tmpIndexMetaList = Lists.newArrayList();
        for (int i = 0; i < counter; i++) {
            String indexName = Text.readString(in);
            long indexId = in.readLong();
            this.indexNameToId.put(indexName, indexId);

            if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_75) {
                // schema
                int colCount = in.readInt();
                List<Column> schema = new LinkedList<>();
                for (int j = 0; j < colCount; j++) {
                    Column column = Column.read(in);
                    schema.add(column);
                }

                // storage type
                TStorageType storageType = TStorageType.valueOf(Text.readString(in));

                // indices's schema version
                int schemaVersion = in.readInt();

                // indices's schema hash
                int schemaHash = in.readInt();

                // indices's short key column count
                short shortKeyColumnCount = in.readShort();

                // The keys type in here is incorrect
                MaterializedIndexMeta indexMeta = new MaterializedIndexMeta(indexId, schema, schemaVersion, schemaHash,
                        shortKeyColumnCount, storageType, KeysType.AGG_KEYS, null);
                tmpIndexMetaList.add(indexMeta);
            } else {
                MaterializedIndexMeta indexMeta = MaterializedIndexMeta.read(in);
                indexIdToMeta.put(indexId, indexMeta);
            }
        }

        // partition and distribution info
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_30) {
            keysType = KeysType.valueOf(Text.readString(in));
        } else {
            keysType = KeysType.AGG_KEYS;
        }

        // add the correct keys type in tmp index meta
        for (MaterializedIndexMeta indexMeta : tmpIndexMetaList) {
            indexMeta.setKeysType(keysType);
            indexIdToMeta.put(indexMeta.getIndexId(), indexMeta);
        }

        PartitionType partType = PartitionType.valueOf(Text.readString(in));
        if (partType == PartitionType.UNPARTITIONED) {
            partitionInfo = SinglePartitionInfo.read(in);
        } else if (partType == PartitionType.RANGE) {
            partitionInfo = RangePartitionInfo.read(in);
        } else if (partType == PartitionType.LIST) {
            partitionInfo = ListPartitionInfo.read(in);
        } else if (partType == PartitionType.EXPR_RANGE) {
            partitionInfo = ExpressionRangePartitionInfo.read(in);
        } else if (partType == PartitionType.EXPR_RANGE_V2) {
            partitionInfo = ExpressionRangePartitionInfoV2.read(in);
        } else {
            throw new IOException("invalid partition type: " + partType);
        }

        DistributionInfoType distriType = DistributionInfoType.valueOf(Text.readString(in));
        if (distriType == DistributionInfoType.HASH) {
            defaultDistributionInfo = HashDistributionInfo.read(in);
        } else if (distriType == DistributionInfoType.RANDOM) {
            defaultDistributionInfo = RandomDistributionInfo.read(in);
        } else {
            throw new IOException("invalid distribution type: " + distriType);
        }

        int partitionCount = in.readInt();
        for (int i = 0; i < partitionCount; ++i) {
            Partition partition = Partition.read(in);
            idToPartition.put(partition.getId(), partition);
            nameToPartition.put(partition.getName(), partition);
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_9) {
            if (in.readBoolean()) {
                int bfColumnCount = in.readInt();
                bfColumns = Sets.newHashSet();
                for (int i = 0; i < bfColumnCount; i++) {
                    bfColumns.add(Text.readString(in));
                }

                bfFpp = in.readDouble();
            }
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_46) {
            if (in.readBoolean()) {
                colocateGroup = Text.readString(in);
            }
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_57) {
            baseIndexId = in.readLong();
        } else {
            // the old table use table id as base index id
            baseIndexId = id;
        }

        // read indexes
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_70) {
            if (in.readBoolean()) {
                this.indexes = TableIndexes.read(in);
            }
        }
        // tableProperty
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_71) {
            if (in.readBoolean()) {
                tableProperty = TableProperty.read(in);
            }
        }
        // temp partitions
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_74) {
            tempPartitions = TempPartitions.read(in);
            if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_77) {
                RangePartitionInfo tempRangeInfo = tempPartitions.getPartitionInfo();
                if (tempRangeInfo != null) {
                    for (long partitionId : tempRangeInfo.getIdToRange(false).keySet()) {
                        ((RangePartitionInfo) this.partitionInfo).addPartition(partitionId, true,
                                tempRangeInfo.getRange(partitionId), tempRangeInfo.getDataProperty(partitionId),
                                tempRangeInfo.getReplicationNum(partitionId), tempRangeInfo.getIsInMemory(partitionId));
                    }
                }
                tempPartitions.unsetPartitionInfo();
            }
        }

        // In the present, the fullSchema could be rebuilt by schema change while the properties is changed by MV.
        // After that, some properties of fullSchema and nameToColumn may be not same as properties of base columns.
        // So, here we need to rebuild the fullSchema to ensure the correctness of the properties.
        rebuildFullSchema();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // In the present, the fullSchema could be rebuilt by schema change while the properties is changed by MV.
        // After that, some properties of fullSchema and nameToColumn may be not same as properties of base columns.
        // So, here we need to rebuild the fullSchema to ensure the correctness of the properties.
        rebuildFullSchema();

        // Recover nameToPartition from idToPartition
        nameToPartition = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Partition partition : idToPartition.values()) {
            nameToPartition.put(partition.getName(), partition);
        }

        // The table may be restored from another cluster, it should be set to current cluster id.
        clusterId = GlobalStateMgr.getCurrentState().getClusterId();

        lastSchemaUpdateTime = new AtomicLong(-1);
        // Record the start and end time for data load version update phase
        lastVersionUpdateStartTime = new AtomicLong(-1);
        lastVersionUpdateEndTime = new AtomicLong(0);
    }

    public OlapTable selectiveCopy(Collection<String> reservedPartitions, boolean resetState, IndexExtState extState) {
        OlapTable copied = new OlapTable();
        if (!DeepCopy.copy(this, copied, OlapTable.class)) {
            LOG.warn("failed to copy olap table: " + getName());
            return null;
        }
        return selectiveCopyInternal(copied, reservedPartitions, resetState, extState);
    }

    protected OlapTable selectiveCopyInternal(OlapTable copied, Collection<String> reservedPartitions,
                                              boolean resetState,
                                              IndexExtState extState) {
        if (resetState) {
            // remove shadow index from copied table
            List<MaterializedIndex> shadowIndex =
                    copied.getPartitions().stream().findFirst()
                            .map(p -> p.getMaterializedIndices(IndexExtState.SHADOW)).orElse(Lists.newArrayList());
            for (MaterializedIndex deleteIndex : shadowIndex) {
                LOG.debug("copied table delete shadow index : {}", deleteIndex.getId());
                copied.deleteIndexInfo(copied.getIndexNameById(deleteIndex.getId()));
            }
            copied.setState(OlapTableState.NORMAL);
            for (Partition partition : copied.getPartitions()) {
                // remove shadow index from partition
                for (MaterializedIndex deleteIndex : shadowIndex) {
                    partition.deleteRollupIndex(deleteIndex.getId());
                }
                partition.setState(PartitionState.NORMAL);
                for (MaterializedIndex idx : partition.getMaterializedIndices(extState)) {
                    idx.setState(IndexState.NORMAL);
                    if (copied.isCloudNativeTableOrMaterializedView()) {
                        continue;
                    }
                    for (Tablet tablet : idx.getTablets()) {
                        for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                            replica.setState(ReplicaState.NORMAL);
                        }
                    }
                }
            }
        }

        if (reservedPartitions == null || reservedPartitions.isEmpty()) {
            // reserve all
            return copied;
        }

        Set<String> reservedPartitionSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        reservedPartitionSet.addAll(reservedPartitions);

        for (String partName : copied.getPartitionNames()) {
            if (!reservedPartitionSet.contains(partName)) {
                copied.dropPartitionAndReserveTablet(partName);
            }
        }

        return copied;
    }

    /*
     * this method is currently used for truncating table(partitions).
     * the new partition has new id, so we need to change all 'id-related' members
     *
     * return the old partition.
     */
    public Partition replacePartition(Partition newPartition) {
        Partition oldPartition = nameToPartition.remove(newPartition.getName());
        idToPartition.remove(oldPartition.getId());

        idToPartition.put(newPartition.getId(), newPartition);
        nameToPartition.put(newPartition.getName(), newPartition);

        DataProperty dataProperty = partitionInfo.getDataProperty(oldPartition.getId());
        short replicationNum = partitionInfo.getReplicationNum(oldPartition.getId());
        boolean isInMemory = partitionInfo.getIsInMemory(oldPartition.getId());
        StorageCacheInfo storageCacheInfo = partitionInfo.getStorageCacheInfo(oldPartition.getId());

        if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            Range<PartitionKey> range = rangePartitionInfo.getRange(oldPartition.getId());
            rangePartitionInfo.dropPartition(oldPartition.getId());
            rangePartitionInfo.addPartition(newPartition.getId(), false, range, dataProperty,
                    replicationNum, isInMemory, storageCacheInfo);
        } else if (partitionInfo.getType() == PartitionType.LIST) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            List<String> values = listPartitionInfo.getIdToValues().get(oldPartition.getId());
            List<List<String>> multiValues = listPartitionInfo.getIdToMultiValues().get(oldPartition.getId());
            listPartitionInfo.dropPartition(oldPartition.getId());
            try {
                listPartitionInfo.addPartition(newPartition.getId(), dataProperty, replicationNum, isInMemory,
                        storageCacheInfo, values, multiValues);
            } catch (AnalysisException ex) {
                LOG.warn("failed to add list partition", ex);
                throw new SemanticException(ex.getMessage());
            }
        } else {
            partitionInfo.dropPartition(oldPartition.getId());
            partitionInfo.addPartition(newPartition.getId(), dataProperty, replicationNum, isInMemory, storageCacheInfo);
        }

        return oldPartition;
    }

    public long getDataSize() {
        long dataSize = 0;
        for (Partition partition : getAllPartitions()) {
            dataSize += partition.getDataSize();
        }
        return dataSize;
    }

    public long getReplicaCount() {
        long replicaCount = 0;
        for (Partition partition : getAllPartitions()) {
            replicaCount += partition.getReplicaCount();
        }
        return replicaCount;
    }

    public void checkStableAndNormal() throws DdlException {
        if (state != OlapTableState.NORMAL) {
            throw new DdlException("Table[" + name + "]'s state is " + state.toString() + " not NORMAL."
                    + "Do not allow create materialized view");
        }
        // check if all tablets are healthy, and no tablet is in tablet scheduler
        long unhealthyTabletId = checkAndGetUnhealthyTablet(GlobalStateMgr.getCurrentSystemInfo(),
                GlobalStateMgr.getCurrentState().getTabletScheduler());
        if (unhealthyTabletId != TabletInvertedIndex.NOT_EXIST_VALUE) {
            throw new DdlException("Table [" + name + "] is not stable. "
                    + "Unhealthy (or doing balance) tablet id: " + unhealthyTabletId + ". "
                    + "Some tablets of this table may not be healthy or are being scheduled. "
                    + "You need to repair the table first or stop cluster balance.");
        }
    }

    public long checkAndGetUnhealthyTablet(SystemInfoService infoService, TabletScheduler tabletScheduler) {
        List<Long> aliveBeIdsInCluster = infoService.getBackendIds(true);
        for (Partition partition : idToPartition.values()) {
            long visibleVersion = partition.getVisibleVersion();
            short replicationNum = partitionInfo.getReplicationNum(partition.getId());
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : mIndex.getTablets()) {
                    LocalTablet localTablet = (LocalTablet) tablet;
                    if (tabletScheduler.containsTablet(tablet.getId())) {
                        return localTablet.getId();
                    }

                    Pair<TabletStatus, TabletSchedCtx.Priority> statusPair = localTablet.getHealthStatusWithPriority(
                            infoService, visibleVersion, replicationNum,
                            aliveBeIdsInCluster);
                    if (statusPair.first != TabletStatus.HEALTHY) {
                        LOG.info("table {} is not stable because tablet {} status is {}. replicas: {}",
                                id, tablet.getId(), statusPair.first, localTablet.getImmutableReplicas());
                        return localTablet.getId();
                    }
                }
            }
        }
        return TabletInvertedIndex.NOT_EXIST_VALUE;
    }

    // arbitrarily choose a partition, and get the buckets backends sequence from base index.
    public List<List<Long>> getArbitraryTabletBucketsSeq() throws DdlException {
        List<List<Long>> backendsPerBucketSeq = Lists.newArrayList();
        Optional<Partition> optionalPartition = idToPartition.values().stream().findFirst();
        if (optionalPartition.isPresent()) {
            Partition partition = optionalPartition.get();
            short replicationNum = partitionInfo.getReplicationNum(partition.getId());
            MaterializedIndex baseIdx = partition.getBaseIndex();
            for (Long tabletId : baseIdx.getTabletIdsInOrder()) {
                LocalTablet tablet = (LocalTablet) baseIdx.getTablet(tabletId);
                List<Long> replicaBackendIds = tablet.getNormalReplicaBackendIds();
                if (replicaBackendIds.size() < replicationNum) {
                    // this should not happen, but in case, throw an exception to terminate this process
                    throw new DdlException("Normal replica number of tablet " + tabletId + " is: "
                            + replicaBackendIds.size() + ", which is less than expected: " + replicationNum);
                }
                backendsPerBucketSeq.add(replicaBackendIds.subList(0, replicationNum));
            }
        }
        return backendsPerBucketSeq;
    }

    /**
     * Get the proximate row count of this table, if you need accurate row count should select count(*) from table.
     *
     * @return proximate row count
     */
    public long proximateRowCount() {
        long totalCount = 0;
        for (Partition partition : getPartitions()) {
            long version = partition.getVisibleVersion();
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    totalCount += tablet.getRowCount(version);
                }
            }
        }
        return totalCount;
    }

    @Override
    public List<Column> getBaseSchema() {
        return getSchemaByIndexId(baseIndexId);
    }

    public Column getBaseColumn(String columnName) {
        for (Column column : getBaseSchema()) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return column;
            }
        }
        return null;
    }

    public int getKeysNum() {
        int keysNum = 0;
        for (Column column : getBaseSchema()) {
            if (column.isKey()) {
                keysNum += 1;
            }
        }
        return keysNum;
    }

    public boolean isKeySet(Set<String> keyColumns) {
        Set<String> tableKeyColumns = getKeyColumns().stream()
                .map(column -> column.getName().toLowerCase()).collect(Collectors.toSet());
        return tableKeyColumns.equals(keyColumns);
    }

    public void setReplicationNum(Short replicationNum) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, replicationNum.toString());
        tableProperty.buildReplicationNum();
    }

    public Short getDefaultReplicationNum() {
        if (tableProperty != null) {
            return tableProperty.getReplicationNum();
        }
        return RunMode.defaultReplicationNum();
    }

    public Boolean isInMemory() {
        if (tableProperty != null) {
            return tableProperty.isInMemory();
        }
        return false;
    }

    public void setIsInMemory(boolean isInMemory) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_INMEMORY, Boolean.valueOf(isInMemory).toString());
        tableProperty.buildInMemory();
    }

    public Boolean enablePersistentIndex() {
        if (tableProperty != null) {
            return tableProperty.enablePersistentIndex();
        }
        return false;
    }

    // Determine which situation supports importing and automatically creating partitions
    public Boolean supportedAutomaticPartition() {
        return partitionInfo.isAutomaticPartition();
    }

    public Boolean isBinlogEnabled() {
        if (tableProperty == null || tableProperty.getBinlogConfig() == null) {
            return false;
        }
        return tableProperty.getBinlogConfig().getBinlogEnable();
    }

    public long getBinlogVersion() {
        if (tableProperty == null || tableProperty.getBinlogConfig() == null) {
            return BinlogConfig.INVALID;
        }
        return tableProperty.getBinlogConfig().getVersion();
    }

    public void setEnablePersistentIndex(boolean enablePersistentIndex) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX,
                        Boolean.valueOf(enablePersistentIndex).toString());
        tableProperty.buildEnablePersistentIndex();
    }

    public Boolean enableReplicatedStorage() {
        if (tableProperty != null) {
            return tableProperty.enableReplicatedStorage();
        }
        return false;
    }

    public void setEnableReplicatedStorage(boolean enableReplicatedStorage) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE,
                        Boolean.valueOf(enableReplicatedStorage).toString());
        tableProperty.buildReplicatedStorage();
    }

    public TWriteQuorumType writeQuorum() {
        if (tableProperty != null) {
            return tableProperty.writeQuorum();
        }
        return TWriteQuorumType.MAJORITY;
    }

    public void setWriteQuorum(String writeQuorum) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM,
                        writeQuorum);
        tableProperty.buildWriteQuorum();
    }

    public void setStorageMedium(TStorageMedium storageMedium) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty
                .modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, storageMedium.name());
    }

    public String getStorageMedium() {
        return tableProperty.getProperties().
                getOrDefault(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, TStorageMedium.HDD.name());
    }

    public boolean hasDelete() {
        if (tableProperty == null) {
            return false;
        }
        return tableProperty.hasDelete();
    }

    public void setHasDelete() {
        if (tableProperty == null) {
            return;
        }
        tableProperty.setHasDelete(true);
    }

    public boolean hasForbitGlobalDict() {
        if (tableProperty == null) {
            return false;
        }
        return tableProperty.hasForbitGlobalDict();
    }

    public void setHasForbitGlobalDict(boolean hasForbitGlobalDict) {
        if (tableProperty == null) {
            return;
        }
        tableProperty.setHasForbitGlobalDict(hasForbitGlobalDict);
    }

    // return true if partition with given name already exist, both in partitions and temp partitions.
    // return false otherwise
    public boolean checkPartitionNameExist(String partitionName) {
        if (nameToPartition.containsKey(partitionName)) {
            return true;
        }
        return tempPartitions.hasPartition(partitionName);
    }

    // if includeTempPartition is true, check if temp partition with given name exist,
    // if includeTempPartition is false, check if normal partition with given name exist.
    // return true if exist, otherwise, return false;
    public boolean checkPartitionNameExist(String partitionName, boolean isTempPartition) {
        if (isTempPartition) {
            return tempPartitions.hasPartition(partitionName);
        } else {
            return nameToPartition.containsKey(partitionName);
        }
    }

    // drop temp partition. if needDropTablet is true, tablets of this temp partition
    // will be dropped from tablet inverted index.
    public void dropTempPartition(String partitionName, boolean needDropTablet) {
        Partition partition = getPartition(partitionName, true);
        if (partition != null) {
            partitionInfo.dropPartition(partition.getId());
            tempPartitions.dropPartition(partitionName, needDropTablet);
        }
    }

    /*
     * replace partitions in 'partitionNames' with partitions in 'tempPartitionNames'.
     * If strictRange is true, the replaced ranges must be exactly same.
     * What is "exactly same"?
     *      1. {[0, 10), [10, 20)} === {[0, 20)}
     *      2. {[0, 10), [15, 20)} === {[0, 10), [15, 18), [18, 20)}
     *      3. {[0, 10), [15, 20)} === {[0, 10), [15, 20)}
     *      4. {[0, 10), [15, 20)} !== {[0, 20)}
     *
     * If useTempPartitionName is false and replaced partition number are equal,
     * the replaced partitions' name will remain unchanged.
     * What is "remain unchange"?
     *      1. replace partition (p1, p2) with temporary partition (tp1, tp2). After replacing, the partition
     *         names are still p1 and p2.
     *
     */
    public void replaceTempPartitions(List<String> partitionNames, List<String> tempPartitionNames,
                                      boolean strictRange, boolean useTempPartitionName) throws DdlException {
        if (partitionInfo instanceof RangePartitionInfo) {
            RangePartitionInfo rangeInfo = (RangePartitionInfo) partitionInfo;

            if (strictRange) {
                // check if range of partitions and temp partitions are exactly same
                List<Range<PartitionKey>> rangeList = Lists.newArrayList();
                List<Range<PartitionKey>> tempRangeList = Lists.newArrayList();
                for (String partName : partitionNames) {
                    Partition partition = nameToPartition.get(partName);
                    Preconditions.checkNotNull(partition);
                    rangeList.add(rangeInfo.getRange(partition.getId()));
                }

                for (String partName : tempPartitionNames) {
                    Partition partition = tempPartitions.getPartition(partName);
                    Preconditions.checkNotNull(partition);
                    tempRangeList.add(rangeInfo.getRange(partition.getId()));
                }
                RangeUtils.checkRangeListsMatch(rangeList, tempRangeList);
            } else {
                // check after replacing, whether the range will conflict
                Set<Long> replacePartitionIds = Sets.newHashSet();
                for (String partName : partitionNames) {
                    Partition partition = nameToPartition.get(partName);
                    Preconditions.checkNotNull(partition);
                    replacePartitionIds.add(partition.getId());
                }
                List<Range<PartitionKey>> replacePartitionRanges = Lists.newArrayList();
                for (String partName : tempPartitionNames) {
                    Partition partition = tempPartitions.getPartition(partName);
                    Preconditions.checkNotNull(partition);
                    replacePartitionRanges.add(rangeInfo.getRange(partition.getId()));
                }
                List<Range<PartitionKey>> sortedRangeList = rangeInfo.getRangeList(replacePartitionIds, false);
                RangeUtils.checkRangeConflict(sortedRangeList, replacePartitionRanges);
            }
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ListPartitionInfo listInfo = (ListPartitionInfo) partitionInfo;
            List<Partition> partitionList = new ArrayList<>();
            for (String partName : partitionNames) {
                Partition partition = nameToPartition.get(partName);
                Preconditions.checkNotNull(partition);
                partitionList.add(partition);
            }
            List<Partition> tempPartitionList = new ArrayList<>();
            for (String partName : tempPartitionNames) {
                Partition tempPartition = tempPartitions.getPartition(partName);
                Preconditions.checkNotNull(tempPartition);
                tempPartitionList.add(tempPartition);
            }
            if (strictRange) {
                CatalogUtils.checkTempPartitionStrictMatch(partitionList, tempPartitionList, listInfo);
            } else {
                CatalogUtils.checkTempPartitionConflict(partitionList, tempPartitionList, listInfo);
            }
        }

        // begin to replace
        // 1. drop old partitions
        for (String partitionName : partitionNames) {
            // This will also drop all tablets of the partition from TabletInvertedIndex
            dropPartition(-1, partitionName, true);
        }

        // 2. add temp partitions' range info to rangeInfo, and remove them from tempPartitionInfo
        for (String partitionName : tempPartitionNames) {
            Partition partition = tempPartitions.getPartition(partitionName);
            // add
            addPartition(partition);
            // drop
            tempPartitions.dropPartition(partitionName, false);
            // move the range from idToTempRange to idToRange
            partitionInfo.moveRangeFromTempToFormal(partition.getId());
        }

        // change the name so that after replacing, the partition name remain unchanged
        if (!useTempPartitionName && partitionNames.size() == tempPartitionNames.size()) {
            for (int i = 0; i < tempPartitionNames.size(); i++) {
                renamePartition(tempPartitionNames.get(i), partitionNames.get(i));
            }
        }
    }

    // used for unpartitioned table in insert overwrite
    // replace partition with temp partition
    public void replacePartition(String sourcePartitionName, String tempPartitionName) {
        if (partitionInfo.getType() != PartitionType.UNPARTITIONED) {
            return;
        }
        // drop source partition
        Partition srcPartition = nameToPartition.get(sourcePartitionName);
        if (srcPartition != null) {
            idToPartition.remove(srcPartition.getId());
            nameToPartition.remove(sourcePartitionName);
            partitionInfo.dropPartition(srcPartition.getId());
            GlobalStateMgr.getCurrentState().onErasePartition(srcPartition);
        }

        Partition partition = tempPartitions.getPartition(tempPartitionName);
        // add
        addPartition(partition);
        // drop
        tempPartitions.dropPartition(tempPartitionName, false);

        // rename partition
        renamePartition(tempPartitionName, sourcePartitionName);
    }

    public void addTempPartition(Partition partition) {
        tempPartitions.addPartition(partition);
    }

    public void dropAllTempPartitions() {
        for (Partition partition : tempPartitions.getAllPartitions()) {
            partitionInfo.dropPartition(partition.getId());
        }
        tempPartitions.dropAll();
    }

    public boolean existTempPartitions() {
        return !tempPartitions.isEmpty();
    }

    public void setStorageFormat(TStorageFormat storageFormat) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT, storageFormat.name());
        tableProperty.buildStorageFormat();
    }

    public TStorageFormat getStorageFormat() {
        if (tableProperty == null) {
            return TStorageFormat.DEFAULT;
        }
        return tableProperty.getStorageFormat();
    }

    public void setStorageVolume(String storageVolume) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME, storageVolume);
        tableProperty.buildStorageVolume();
    }

    public String getStorageVolume() {
        if (tableProperty == null) {
            return RunMode.allowCreateLakeTable() ? "default" : "local";
        }
        return tableProperty.getStorageVolume();
    }

    public void setCompressionType(TCompressionType compressionType) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_COMPRESSION, compressionType.name());
        tableProperty.buildCompressionType();
    }

    public TCompressionType getCompressionType() {
        if (tableProperty == null) {
            return TCompressionType.LZ4_FRAME;
        }
        return tableProperty.getCompressionType();
    }

    public void setPartitionLiveNumber(int number) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER, String.valueOf(number));
        tableProperty.buildPartitionLiveNumber();
    }

    public Map<String, String> buildBinlogAvailableVersion() {
        Map<String, String> result = new HashMap<>();
        Collection<Partition> partitions = getPartitions();
        for (Partition partition : partitions) {
            result.put(TableProperty.BINLOG_PARTITION + partition.getId(),
                    String.valueOf(partition.getVisibleVersion()));
        }
        return result;
    }

    public void setBinlogAvailableVersion(Map<String, String> properties) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        tableProperty.modifyTableProperties(properties);
        tableProperty.buildBinlogAvailableVersion();
    }

    public Map<Long, Long> getBinlogAvailableVersion() {
        if (tableProperty == null) {
            return new HashMap<>();
        }
        return tableProperty.getBinlogAvailaberVersions();
    }

    public void clearBinlogAvailableVersion() {
        if (tableProperty == null) {
            return;
        }
        tableProperty.clearBinlogAvailableVersion();
    }

    @Override
    public List<UniqueConstraint> getUniqueConstraints() {
        if (tableProperty == null) {
            return null;
        }
        return tableProperty.getUniqueConstraints();
    }

    @Override
    public void setUniqueConstraints(List<UniqueConstraint> uniqueConstraints) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        Map<String, String> properties = Maps.newHashMap();
        String newProperty = uniqueConstraints.stream().map(UniqueConstraint::toString).collect(Collectors.joining(";"));
        properties.put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, newProperty);
        tableProperty.modifyTableProperties(properties);
        tableProperty.setUniqueConstraints(uniqueConstraints);
    }

    @Override
    public List<ForeignKeyConstraint> getForeignKeyConstraints() {
        if (tableProperty == null) {
            return null;
        }
        return tableProperty.getForeignKeyConstraints();
    }

    @Override
    public void setForeignKeyConstraints(List<ForeignKeyConstraint> foreignKeyConstraints) {
        if (tableProperty == null) {
            tableProperty = new TableProperty(new HashMap<>());
        }
        Map<String, String> properties = Maps.newHashMap();
        String newProperty = foreignKeyConstraints
                .stream().map(ForeignKeyConstraint::toString).collect(Collectors.joining(";"));
        properties.put(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, newProperty);
        tableProperty.modifyTableProperties(properties);
        tableProperty.setForeignKeyConstraints(foreignKeyConstraints);
    }

    @Override
    public void onCreate() {
        analyzePartitionInfo();
    }

    private void analyzePartitionInfo() {
        if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
            return;
        }
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        // currently, automatic partition only supports one expression
        Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs().get(0);
        // for Partition slot ref, the SlotDescriptor is not serialized, so should recover it here.
        // the SlotDescriptor is used by toThrift, which influences the execution process.
        List<SlotRef> slotRefs = Lists.newArrayList();
        partitionExpr.collect(SlotRef.class, slotRefs);
        Preconditions.checkState(slotRefs.size() == 1);
        if (slotRefs.get(0).getSlotDescriptorWithoutCheck() == null) {
            for (int i = 0; i < fullSchema.size(); i++) {
                Column column = fullSchema.get(i);
                if (column.getName().equalsIgnoreCase(slotRefs.get(0).getColumnName())) {
                    SlotDescriptor slotDescriptor =
                            new SlotDescriptor(new SlotId(i), column.getName(), column.getType(), column.isAllowNull());
                    slotRefs.get(0).setDesc(slotDescriptor);
                }
            }
        }
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        // drop all temp partitions of this table, so that there is no temp partitions in recycle bin,
        // which make things easier.
        dropAllTempPartitions();
        for (MvId mvId : getRelatedMaterializedViews()) {
            Table tmpTable = db.getTable(mvId.getId());
            if (tmpTable != null) {
                MaterializedView mv = (MaterializedView) tmpTable;
                mv.setActive(false);
                LOG.warn("Setting the materialized view {}({}) to invalid because " +
                        "the table {} was dropped.", mv.getName(), mv.getId(), getName());
            } else {
                LOG.warn("Ignore materialized view {} does not exists", mvId);
            }
        }
    }

    @Override
    public Runnable delete(boolean replay) {
        GlobalStateMgr.getCurrentState().getLocalMetastore().onEraseTable(this, replay);
        return replay ? null : new DeleteOlapTableTask(this);
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    public AlterJobV2Builder alterTable() {
        return new OlapTableAlterJobV2Builder(this);
    }

    private static class DeleteOlapTableTask implements Runnable {
        private final OlapTable table;

        public DeleteOlapTableTask(OlapTable table) {
            this.table = table;
        }

        @Override
        public void run() {
            HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();

            // drop all replicas
            for (Partition partition : table.getAllPartitions()) {
                List<MaterializedIndex> allIndices =
                        partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                for (MaterializedIndex materializedIndex : allIndices) {
                    long indexId = materializedIndex.getId();
                    int schemaHash = table.getSchemaHashByIndexId(indexId);
                    for (Tablet tablet : materializedIndex.getTablets()) {
                        long tabletId = tablet.getId();
                        List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
                        for (Replica replica : replicas) {
                            long backendId = replica.getBackendId();
                            DropReplicaTask dropTask = new DropReplicaTask(backendId, tabletId, schemaHash, true);
                            AgentBatchTask batchTask = batchTaskMap.get(backendId);
                            if (batchTask == null) {
                                batchTask = new AgentBatchTask();
                                batchTaskMap.put(backendId, batchTask);
                            }
                            batchTask.addTask(dropTask);
                        } // end for replicas
                    } // end for tablets
                } // end for indices
            } // end for partitions

            int numDropTaskPerBe = Config.max_agent_tasks_send_per_be;
            for (Map.Entry<Long, AgentBatchTask> entry : batchTaskMap.entrySet()) {
                AgentBatchTask originTasks = entry.getValue();
                if (originTasks.getTaskNum() > numDropTaskPerBe) {
                    AgentBatchTask partTask = new AgentBatchTask();
                    List<AgentTask> allTasks = originTasks.getAllTasks();
                    int curTask = 1;
                    for (AgentTask task : allTasks) {
                        partTask.addTask(task);
                        if (curTask++ > numDropTaskPerBe) {
                            AgentTaskExecutor.submit(partTask);
                            curTask = 1;
                            partTask = new AgentBatchTask();
                            ThreadUtil.sleepAtLeastIgnoreInterrupts(1000);
                        }
                    }
                    if (!partTask.getAllTasks().isEmpty()) {
                        AgentTaskExecutor.submit(partTask);
                    }
                } else {
                    AgentTaskExecutor.submit(originTasks);
                }
            }
        }
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> properties = Maps.newHashMap();

        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, getDefaultReplicationNum().toString());
        properties.put(PropertyAnalyzer.PROPERTIES_INMEMORY, isInMemory().toString());

        Map<String, String> tableProperty = getTableProperty().getProperties();
        if (tableProperty != null && tableProperty.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
            properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM,
                    tableProperty.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM));
        }
        return properties;
    }

    @Override
    public boolean supportsUpdate() {
        return getKeysType() == KeysType.PRIMARY_KEYS;
    }

    // ------ for lake table and lake materialized view start ------
    public String getStoragePath() {
        throw new SemanticException("getStoragePath is not supported");
    }

    public FilePathInfo getPartitionFilePathInfo() {
        throw new SemanticException("getPartitionFilePathInfo is not supported");
    }

    public FileCacheInfo getPartitionFileCacheInfo(long partitionId) {
        throw new SemanticException("getPartitionFileCacheInfo is not supported");
    }

    public void setStorageInfo(FilePathInfo pathInfo, boolean enableCache, long cacheTtlS, boolean asyncWriteBack) {
        throw new SemanticException("setStorageInfo is not supported");
    }
    // ------ for lake table and lake materialized view end ------
}
