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

public class CreateReplicaTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(CreateReplicaTask.class);

    public enum RecoverySource {
        SCHEDULER,
        REPORT
    }

    private final long version;
    private final TCompressionType compressionType;
    private final int compressionLevel;
    private final TStorageMedium storageMedium;
    private final boolean enablePersistentIndex;
    private TPersistentIndexType persistentIndexType;

    private BinlogConfig binlogConfig;

    private final TTabletType tabletType;

    // used for synchronous process
    private MarkedCountDownLatch<Long, Long> latch;

    private boolean inRestoreMode = false;

    // if base tablet id is set, BE will create the replica on same disk as this base tablet
    private long baseTabletId = -1;
    private int baseSchemaHash = -1;

    private RecoverySource recoverySource;

    private int primaryIndexCacheExpireSec = 0;
    private boolean createSchemaFile = true;
    private boolean enableTabletCreationOptimization = false;
    private final TTabletSchema tabletSchema;
    private long gtid = 0;
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
        this.gtid = builder.getGtid();
        this.timeoutMs = builder.getTimeoutMs();
    }

    public static Builder newBuilder() {
        return new Builder();
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
            LOG.debug("CreateReplicaTask count down to zero. error msg: {}", errMsg);
        }
    }

    public void setLatch(MarkedCountDownLatch<Long, Long> latch) {
        this.latch = latch;
    }

    public void setBaseTablet(long baseTabletId, int baseSchemaHash) {
        this.baseTabletId = baseTabletId;
        this.baseSchemaHash = baseSchemaHash;
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public TTabletType getTabletType() {
        return tabletType;
    }

    public TCreateTabletReq toThrift() {
        TCreateTabletReq createTabletReq = new TCreateTabletReq();
        createTabletReq.setTablet_id(tabletId);
        createTabletReq.setTablet_schema(tabletSchema);
        createTabletReq.setVersion(version);
        createTabletReq.setStorage_medium(storageMedium);
        createTabletReq.setEnable_persistent_index(enablePersistentIndex);
        if (persistentIndexType != null) {
            createTabletReq.setPersistent_index_type(persistentIndexType);
        }
        createTabletReq.setPrimary_index_cache_expire_sec(primaryIndexCacheExpireSec);
        if (binlogConfig != null) {
            TBinlogConfig tBinlogConfig = binlogConfig.toTBinlogConfig();
            createTabletReq.setBinlog_config(tBinlogConfig);
        }
        if (inRestoreMode) {
            createTabletReq.setIn_restore_mode(true);
        }
        createTabletReq.setTable_id(tableId);
        createTabletReq.setPartition_id(partitionId);
        if (baseTabletId != -1) {
            createTabletReq.setBase_tablet_id(baseTabletId);
            createTabletReq.setBase_schema_hash(baseSchemaHash);
        }
        createTabletReq.setCompression_type(compressionType);
        createTabletReq.setCompression_level(compressionLevel);
        createTabletReq.setTablet_type(tabletType);
        createTabletReq.setCreate_schema_file(createSchemaFile);
        createTabletReq.setEnable_tablet_creation_optimization(enableTabletCreationOptimization);
        createTabletReq.setGtid(gtid);
        createTabletReq.setTimeout_ms(timeoutMs);
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
        private long gtid = 0;
        private long timeoutMs = -1;

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

        public long getGtid() {
            return gtid;
        }

        public Builder setGtid(long gtid) {
            this.gtid = gtid;
            return this;
        }

        public long getTimeoutMs() {
            return timeoutMs;
        }

        public Builder setTimeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
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
}
