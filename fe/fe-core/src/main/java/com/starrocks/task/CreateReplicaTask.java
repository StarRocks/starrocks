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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/CreateReplicaTask.java

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

package com.starrocks.task;

<<<<<<< HEAD
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.Status;
import com.starrocks.thrift.TBinlogConfig;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TCreateTabletReq;
import com.starrocks.thrift.TOlapTableIndex;
import com.starrocks.thrift.TPersistentIndexType;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

=======
import com.google.common.base.Preconditions;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.common.Status;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.thrift.TBinlogConfig;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TCreateTabletReq;
import com.starrocks.thrift.TPersistentIndexType;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
public class CreateReplicaTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(CreateReplicaTask.class);

    public enum RecoverySource {
        SCHEDULER,
        REPORT
    }

<<<<<<< HEAD
    private short shortKeyColumnCount;
    private int schemaHash;

    private long version;

    private KeysType keysType;
    private TStorageType storageType;
    private TCompressionType compressionType;
    private TStorageMedium storageMedium;

    private List<Column> columns;
    private List<Integer> sortKeyIdxes;

    // bloom filter columns
    private Set<String> bfColumns;
    private double bfFpp;

    // indexes
    private List<Index> indexes;

    private boolean isInMemory;

    private boolean enablePersistentIndex;

=======
    private final long version;
    private final TCompressionType compressionType;
    private final int compressionLevel;
    private final TStorageMedium storageMedium;
    private final boolean enablePersistentIndex;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    private TPersistentIndexType persistentIndexType;

    private BinlogConfig binlogConfig;

<<<<<<< HEAD
    private TTabletType tabletType;
=======
    private final TTabletType tabletType;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    // used for synchronous process
    private MarkedCountDownLatch<Long, Long> latch;

    private boolean inRestoreMode = false;

    // if base tablet id is set, BE will create the replica on same disk as this base tablet
    private long baseTabletId = -1;
    private int baseSchemaHash = -1;

    private RecoverySource recoverySource;

<<<<<<< HEAD
    private boolean createSchemaFile = true;

    public CreateReplicaTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                             short shortKeyColumnCount, int schemaHash, long version,
                             KeysType keysType, TStorageType storageType,
                             TStorageMedium storageMedium, List<Column> columns,
                             Set<String> bfColumns, double bfFpp, MarkedCountDownLatch<Long, Long> latch,
                             List<Index> indexes,
                             boolean isInMemory,
                             boolean enablePersistentIndex,
                             TTabletType tabletType, TCompressionType compressionType,
                             boolean createSchemaFile) {
        this(backendId, dbId, tableId, partitionId, indexId, tabletId, shortKeyColumnCount,
                schemaHash, version, keysType, storageType, storageMedium, columns, bfColumns,
                bfFpp, latch, indexes, isInMemory, enablePersistentIndex, tabletType, compressionType,
                null, createSchemaFile);
    }

    public CreateReplicaTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                             short shortKeyColumnCount, int schemaHash, long version,
                             KeysType keysType, TStorageType storageType,
                             TStorageMedium storageMedium, List<Column> columns,
                             Set<String> bfColumns, double bfFpp, MarkedCountDownLatch<Long, Long> latch,
                             List<Index> indexes,
                             boolean isInMemory,
                             boolean enablePersistentIndex,
                             BinlogConfig binlogConfig,
                             TTabletType tabletType, TCompressionType compressionType, List<Integer> sortKeyIdxes,
                             boolean createSchemaFile) {

        this(backendId, dbId, tableId, partitionId, indexId, tabletId, shortKeyColumnCount, schemaHash, version,
                keysType, storageType, storageMedium, columns, bfColumns, bfFpp, latch, indexes, isInMemory,
                enablePersistentIndex, tabletType, compressionType, sortKeyIdxes, createSchemaFile);
        this.binlogConfig = binlogConfig;
    }

    public CreateReplicaTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                             short shortKeyColumnCount, int schemaHash, long version,
                             KeysType keysType, TStorageType storageType,
                             TStorageMedium storageMedium, List<Column> columns,
                             Set<String> bfColumns, double bfFpp, MarkedCountDownLatch<Long, Long> latch,
                             List<Index> indexes,
                             boolean isInMemory,
                             boolean enablePersistentIndex,
                             TTabletType tabletType, TCompressionType compressionType, List<Integer> sortKeyIdxes,
                             boolean createSchemaFile) {
        super(null, backendId, TTaskType.CREATE, dbId, tableId, partitionId, indexId, tabletId);

        this.shortKeyColumnCount = shortKeyColumnCount;
        this.schemaHash = schemaHash;

        this.version = version;

        this.keysType = keysType;
        this.storageType = storageType;
        this.storageMedium = storageMedium;

        this.columns = columns;
        this.sortKeyIdxes = sortKeyIdxes;

        this.bfColumns = bfColumns;
        this.indexes = indexes;
        this.bfFpp = bfFpp;

        this.latch = latch;

        this.isInMemory = isInMemory;
        this.enablePersistentIndex = enablePersistentIndex;
        this.tabletType = tabletType;

        this.compressionType = compressionType;
        this.createSchemaFile = createSchemaFile;
    }

    public CreateReplicaTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                             short shortKeyColumnCount, int schemaHash, long version,
                             KeysType keysType, TStorageType storageType,
                             TStorageMedium storageMedium, List<Column> columns,
                             Set<String> bfColumns, double bfFpp, MarkedCountDownLatch<Long, Long> latch,
                             List<Index> indexes,
                             boolean isInMemory,
                             boolean enablePersistentIndex,
                             TPersistentIndexType persistentIndexType,
                             TTabletType tabletType, TCompressionType compressionType, List<Integer> sortKeyIdxes,
                             boolean createSchemaFile) {
        this(backendId, dbId, tableId, partitionId, indexId, tabletId, shortKeyColumnCount, schemaHash, version,
                keysType, storageType, storageMedium, columns, bfColumns, bfFpp, latch, indexes, isInMemory,
                enablePersistentIndex, tabletType, compressionType, sortKeyIdxes, createSchemaFile);
        this.persistentIndexType = persistentIndexType;
=======
    private int primaryIndexCacheExpireSec = 0;
    private boolean createSchemaFile = true;
    private boolean enableTabletCreationOptimization = false;
    private final TTabletSchema tabletSchema;
    private long timeoutMs = -1;

    private CreateReplicaTask(Builder builder) {
        super(null, builder.getNodeId(), TTaskType.CREATE, builder.getDbId(), builder.getTableId(),
                builder.getPartitionId(), builder.getIndexId(), builder.getTabletId());
        this.version = builder.getVersion();
        this.storageMedium = builder.getStorageMedium();
        this.latch = builder.getLatch();
        this.enablePersistentIndex = builder.isEnablePersistentIndex();
        this.primaryIndexCacheExpireSec = builder.getPrimaryIndexCacheExpireSec();
        this.persistentIndexType = builder.getPersistentIndexType();
        this.tabletType = builder.getTabletType();
        this.compressionType = builder.getCompressionType();
        this.compressionLevel = builder.getCompressionLevel();
        this.tabletSchema = builder.getTabletSchema();
        this.binlogConfig = builder.getBinlogConfig();
        this.createSchemaFile = builder.isCreateSchemaFile();
        this.enableTabletCreationOptimization = builder.isEnableTabletCreationOptimization();
        this.baseTabletId = builder.getBaseTabletId();
        this.recoverySource = builder.getRecoverySource();
        this.inRestoreMode = builder.isInRestoreMode();
    }

    public static Builder newBuilder() {
        return new Builder();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public void setRecoverySource(RecoverySource source) {
        this.recoverySource = source;
    }

    public RecoverySource getRecoverySource() {
        return this.recoverySource;
    }

    public void countDownLatch(long backendId, long tabletId) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, tabletId)) {
                LOG.debug("CreateReplicaTask current latch count: {}, backend: {}, tablet:{}",
                        latch.getCount(), backendId, tabletId);
            }
        }
    }

    // call this always means one of tasks is failed. count down to zero to finish entire task
    public void countDownToZero(String errMsg) {
        if (this.latch != null) {
            latch.countDownToZero(new Status(TStatusCode.CANCELLED, errMsg));
            LOG.debug("CreateReplicaTask download to zero. error msg: {}", errMsg);
        }
    }

    public void setLatch(MarkedCountDownLatch<Long, Long> latch) {
        this.latch = latch;
    }

<<<<<<< HEAD
    public void setInRestoreMode(boolean inRestoreMode) {
        this.inRestoreMode = inRestoreMode;
    }

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public void setBaseTablet(long baseTabletId, int baseSchemaHash) {
        this.baseTabletId = baseTabletId;
        this.baseSchemaHash = baseSchemaHash;
    }

<<<<<<< HEAD
=======
    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public TTabletType getTabletType() {
        return tabletType;
    }

    public TCreateTabletReq toThrift() {
        TCreateTabletReq createTabletReq = new TCreateTabletReq();
        createTabletReq.setTablet_id(tabletId);
<<<<<<< HEAD

        TTabletSchema tSchema = new TTabletSchema();
        tSchema.setShort_key_column_count(shortKeyColumnCount);
        tSchema.setSchema_hash(schemaHash);
        tSchema.setKeys_type(keysType.toThrift());
        tSchema.setStorage_type(storageType);
        tSchema.setId(indexId); // use index id as the schema id. assume schema change will assign a new index id.

        List<TColumn> tColumns = new ArrayList<TColumn>();
        for (Column column : columns) {
            TColumn tColumn = column.toThrift();
            // is bloom filter column
            if (bfColumns != null && bfColumns.contains(column.getName())) {
                tColumn.setIs_bloom_filter_column(true);
            }
            // when doing schema change, some modified column has a prefix in name.
            // this prefix is only used in FE, not visible to BE, so we should remove this prefix.
            if (column.getName().startsWith(SchemaChangeHandler.SHADOW_NAME_PRFIX)) {
                tColumn.setColumn_name(column.getName().substring(SchemaChangeHandler.SHADOW_NAME_PRFIX.length()));
            }
            if (column.getName().startsWith(SchemaChangeHandler.SHADOW_NAME_PRFIX_V1)) {
                tColumn.setColumn_name(column.getName().substring(SchemaChangeHandler.SHADOW_NAME_PRFIX_V1.length()));
            }
            tColumns.add(tColumn);
        }
        tSchema.setColumns(tColumns);
        tSchema.setSort_key_idxes(sortKeyIdxes);

        if (CollectionUtils.isNotEmpty(indexes)) {
            List<TOlapTableIndex> tIndexes = new ArrayList<>();
            for (Index index : indexes) {
                tIndexes.add(index.toThrift());
            }
            tSchema.setIndexes(tIndexes);
        }

        if (bfColumns != null) {
            tSchema.setBloom_filter_fpp(bfFpp);
        }
        tSchema.setIs_in_memory(isInMemory);
        createTabletReq.setTablet_schema(tSchema);

        createTabletReq.setVersion(version);

        createTabletReq.setStorage_medium(storageMedium);
        createTabletReq.setEnable_persistent_index(enablePersistentIndex);

        if (persistentIndexType != null) {
            createTabletReq.setPersistent_index_type(persistentIndexType);
        }

=======
        createTabletReq.setTablet_schema(tabletSchema);
        createTabletReq.setVersion(version);
        createTabletReq.setStorage_medium(storageMedium);
        createTabletReq.setEnable_persistent_index(enablePersistentIndex);
        if (persistentIndexType != null) {
            createTabletReq.setPersistent_index_type(persistentIndexType);
        }
        createTabletReq.setPrimary_index_cache_expire_sec(primaryIndexCacheExpireSec);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (binlogConfig != null) {
            TBinlogConfig tBinlogConfig = binlogConfig.toTBinlogConfig();
            createTabletReq.setBinlog_config(tBinlogConfig);
        }
<<<<<<< HEAD

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (inRestoreMode) {
            createTabletReq.setIn_restore_mode(true);
        }
        createTabletReq.setTable_id(tableId);
        createTabletReq.setPartition_id(partitionId);
<<<<<<< HEAD

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (baseTabletId != -1) {
            createTabletReq.setBase_tablet_id(baseTabletId);
            createTabletReq.setBase_schema_hash(baseSchemaHash);
        }
<<<<<<< HEAD

        createTabletReq.setCompression_type(compressionType);
        createTabletReq.setTablet_type(tabletType);
        createTabletReq.setCreate_schema_file(createSchemaFile);
        return createTabletReq;
    }
=======
        createTabletReq.setCompression_type(compressionType);
        createTabletReq.setCompression_level(compressionLevel);
        createTabletReq.setTablet_type(tabletType);
        createTabletReq.setCreate_schema_file(createSchemaFile);
        createTabletReq.setEnable_tablet_creation_optimization(enableTabletCreationOptimization);
        return createTabletReq;
    }

    public static class Builder {
        // TabletSchedCtx will use -1 to initialize many fields, so here we choose -2 as an invalid id.
        public static final long INVALID_ID = -2;
        private long nodeId = INVALID_ID;
        private long dbId = INVALID_ID;
        private long tableId = INVALID_ID;
        private long partitionId = INVALID_ID;
        private long indexId = INVALID_ID;
        private long tabletId = INVALID_ID;
        private long version = INVALID_ID;
        private TCompressionType compressionType;
        private int compressionLevel;
        private TStorageMedium storageMedium;
        private boolean enablePersistentIndex;
        private TPersistentIndexType persistentIndexType;
        private BinlogConfig binlogConfig;
        private TTabletType tabletType = TTabletType.TABLET_TYPE_DISK;
        private MarkedCountDownLatch<Long, Long> latch;
        private boolean inRestoreMode = false;
        private long baseTabletId = INVALID_ID;
        private RecoverySource recoverySource;
        private int primaryIndexCacheExpireSec = 0;
        private boolean createSchemaFile = true;
        private boolean enableTabletCreationOptimization = false;
        private TTabletSchema tabletSchema;

        private Builder() {
        }

        public long getNodeId() {
            return nodeId;
        }

        public Builder setNodeId(long nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public long getDbId() {
            return dbId;
        }

        public Builder setDbId(long dbId) {
            this.dbId = dbId;
            return this;
        }

        public long getTableId() {
            return tableId;
        }

        public Builder setTableId(long tableId) {
            this.tableId = tableId;
            return this;
        }

        public long getPartitionId() {
            return partitionId;
        }

        public Builder setPartitionId(long partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public long getIndexId() {
            return indexId;
        }

        public Builder setIndexId(long indexId) {
            this.indexId = indexId;
            return this;
        }

        public long getTabletId() {
            return tabletId;
        }

        public Builder setTabletId(long tabletId) {
            this.tabletId = tabletId;
            return this;
        }

        public long getVersion() {
            return version;
        }

        public Builder setVersion(long version) {
            this.version = version;
            return this;
        }

        public TCompressionType getCompressionType() {
            return compressionType;
        }

        public Builder setCompressionType(TCompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public int getCompressionLevel() {
            return compressionLevel;
        }

        public Builder setCompressionLevel(int compressionLevel) {
            this.compressionLevel = compressionLevel;
            return this;
        }        

        public TStorageMedium getStorageMedium() {
            return storageMedium;
        }

        public Builder setStorageMedium(TStorageMedium storageMedium) {
            this.storageMedium = storageMedium;
            return this;
        }

        public boolean isEnablePersistentIndex() {
            return enablePersistentIndex;
        }

        public Builder setEnablePersistentIndex(boolean enablePersistentIndex) {
            this.enablePersistentIndex = enablePersistentIndex;
            return this;
        }

        public TPersistentIndexType getPersistentIndexType() {
            return persistentIndexType;
        }

        public Builder setPersistentIndexType(TPersistentIndexType persistentIndexType) {
            this.persistentIndexType = persistentIndexType;
            return this;
        }

        public BinlogConfig getBinlogConfig() {
            return binlogConfig;
        }

        public Builder setBinlogConfig(BinlogConfig binlogConfig) {
            this.binlogConfig = binlogConfig;
            return this;
        }

        public TTabletType getTabletType() {
            return tabletType;
        }

        public Builder setTabletType(TTabletType tabletType) {
            this.tabletType = tabletType;
            return this;
        }

        public MarkedCountDownLatch<Long, Long> getLatch() {
            return latch;
        }

        public Builder setLatch(MarkedCountDownLatch<Long, Long> latch) {
            this.latch = latch;
            return this;
        }

        public boolean isInRestoreMode() {
            return inRestoreMode;
        }

        public Builder setInRestoreMode(boolean inRestoreMode) {
            this.inRestoreMode = inRestoreMode;
            return this;
        }

        public long getBaseTabletId() {
            return baseTabletId;
        }

        public Builder setBaseTabletId(long baseTabletId) {
            this.baseTabletId = baseTabletId;
            return this;
        }

        public RecoverySource getRecoverySource() {
            return recoverySource;
        }

        public Builder setRecoverySource(RecoverySource recoverySource) {
            this.recoverySource = recoverySource;
            return this;
        }

        public int getPrimaryIndexCacheExpireSec() {
            return primaryIndexCacheExpireSec;
        }

        public Builder setPrimaryIndexCacheExpireSec(int primaryIndexCacheExpireSec) {
            this.primaryIndexCacheExpireSec = primaryIndexCacheExpireSec;
            return this;
        }

        public boolean isCreateSchemaFile() {
            return createSchemaFile;
        }

        public Builder setCreateSchemaFile(boolean createSchemaFile) {
            this.createSchemaFile = createSchemaFile;
            return this;
        }

        public boolean isEnableTabletCreationOptimization() {
            return enableTabletCreationOptimization;
        }

        public Builder setEnableTabletCreationOptimization(boolean enableTabletCreationOptimization) {
            this.enableTabletCreationOptimization = enableTabletCreationOptimization;
            return this;
        }

        public TTabletSchema getTabletSchema() {
            return tabletSchema;
        }

        public Builder setTabletSchema(TTabletSchema tabletSchema) {
            this.tabletSchema = tabletSchema;
            return this;
        }

        public CreateReplicaTask build() {
            Preconditions.checkState(nodeId != INVALID_ID);
            Preconditions.checkState(dbId != INVALID_ID);
            Preconditions.checkState(tableId != INVALID_ID);
            Preconditions.checkState(partitionId != INVALID_ID);
            Preconditions.checkState(indexId != INVALID_ID);
            Preconditions.checkState(tabletId != INVALID_ID);
            Preconditions.checkState(version != INVALID_ID);
            Preconditions.checkState(tabletSchema != null);

            return new CreateReplicaTask(this);
        }
    }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
