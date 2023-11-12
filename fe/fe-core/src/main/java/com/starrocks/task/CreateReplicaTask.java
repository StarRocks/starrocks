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

public class CreateReplicaTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(CreateReplicaTask.class);

    public enum RecoverySource {
        SCHEDULER,
        REPORT
    }

    private short shortKeyColumnCount;
    private int schemaHash;
    private int schemaVersion;

    private long version;

    private KeysType keysType;
    private TStorageType storageType;
    private TCompressionType compressionType;
    private TStorageMedium storageMedium;

    private List<Column> columns;
    private List<Integer> sortKeyIdxes;
    private List<Integer> sortKeyUniqueIds;

    // bloom filter columns
    private Set<String> bfColumns;
    private double bfFpp;

    // indexes
    private List<Index> indexes;

    private boolean isInMemory;

    private boolean enablePersistentIndex;

    private TPersistentIndexType persistentIndexType;

    private BinlogConfig binlogConfig;

    private TTabletType tabletType;

    // used for synchronous process
    private MarkedCountDownLatch<Long, Long> latch;

    private boolean inRestoreMode = false;

    // if base tablet id is set, BE will create the replica on same disk as this base tablet
    private long baseTabletId = -1;
    private int baseSchemaHash = -1;

    private RecoverySource recoverySource;

    // true if this task is created by recover request(See comment of Config.recover_with_empty_tablet)
    private boolean isRecoverTask = false;

    private boolean isFromScheduler = false;

    private int primaryIndexCacheExpireSec = 0;
    private boolean createSchemaFile = true;

    public CreateReplicaTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                             short shortKeyColumnCount, int schemaHash, long version,
                             KeysType keysType, TStorageType storageType,
                             TStorageMedium storageMedium, List<Column> columns,
                             Set<String> bfColumns, double bfFpp, MarkedCountDownLatch<Long, Long> latch,
                             List<Index> indexes,
                             boolean isInMemory,
                             boolean enablePersistentIndex,
                             int primaryIndexCacheExpireSec,
                             TTabletType tabletType, TCompressionType compressionType) {
        this(backendId, dbId, tableId, partitionId, indexId, tabletId, shortKeyColumnCount,
                schemaHash, 0, version, keysType, storageType, storageMedium, columns, bfColumns,
                bfFpp, latch, indexes, isInMemory, enablePersistentIndex, primaryIndexCacheExpireSec,
                tabletType, compressionType, null, null);
    }

    public CreateReplicaTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                             short shortKeyColumnCount, int schemaHash, long version,
                             KeysType keysType, TStorageType storageType,
                             TStorageMedium storageMedium, List<Column> columns,
                             Set<String> bfColumns, double bfFpp, MarkedCountDownLatch<Long, Long> latch,
                             List<Index> indexes,
                             boolean isInMemory,
                             boolean enablePersistentIndex,
                             int primaryIndexCacheExpireSec,
                             BinlogConfig binlogConfig,
                             TTabletType tabletType, TCompressionType compressionType, List<Integer> sortKeyIdxes,
                             List<Integer> sortKeyUniqueIds) {

        this(backendId, dbId, tableId, partitionId, indexId, tabletId, shortKeyColumnCount, schemaHash, 0, version,
                keysType, storageType, storageMedium, columns, bfColumns, bfFpp, latch, indexes, isInMemory,
                enablePersistentIndex, primaryIndexCacheExpireSec, tabletType, compressionType, sortKeyIdxes, sortKeyUniqueIds);
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
                             int primaryIndexCacheExpireSec,
                             TTabletType tabletType, TCompressionType compressionType, List<Integer> sortKeyIdxes,
                             List<Integer> sortKeyUniqueIds,
                             boolean createSchemaFile) {
        this(backendId, dbId, tableId, partitionId, indexId, tabletId, shortKeyColumnCount, schemaHash, 0, version,
                keysType, storageType, storageMedium, columns, bfColumns, bfFpp, latch, indexes, isInMemory,
                enablePersistentIndex, primaryIndexCacheExpireSec, tabletType, compressionType, sortKeyIdxes, sortKeyUniqueIds);
        this.createSchemaFile = createSchemaFile;
    }

    public CreateReplicaTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                             short shortKeyColumnCount, int schemaHash, int schemaVersion, long version,
                             KeysType keysType, TStorageType storageType,
                             TStorageMedium storageMedium, List<Column> columns,
                             Set<String> bfColumns, double bfFpp, MarkedCountDownLatch<Long, Long> latch,
                             List<Index> indexes,
                             boolean isInMemory,
                             boolean enablePersistentIndex,
                             int primaryIndexCacheExpireSec,
                             TTabletType tabletType, TCompressionType compressionType, List<Integer> sortKeyIdxes,
                             List<Integer> sortKeyUniqueIds) {
        super(null, backendId, TTaskType.CREATE, dbId, tableId, partitionId, indexId, tabletId);

        this.shortKeyColumnCount = shortKeyColumnCount;
        this.schemaHash = schemaHash;
        this.schemaVersion = schemaVersion;

        this.version = version;

        this.keysType = keysType;
        this.storageType = storageType;
        this.storageMedium = storageMedium;

        this.columns = columns;
        this.sortKeyIdxes = sortKeyIdxes;
        this.sortKeyUniqueIds = sortKeyUniqueIds;

        this.bfColumns = bfColumns;
        this.indexes = indexes;
        this.bfFpp = bfFpp;

        this.latch = latch;

        this.isInMemory = isInMemory;
        this.enablePersistentIndex = enablePersistentIndex;
        this.primaryIndexCacheExpireSec = primaryIndexCacheExpireSec;
        this.tabletType = tabletType;

        this.compressionType = compressionType;
    }

    public CreateReplicaTask(long backendId, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                             short shortKeyColumnCount, int schemaHash, long version,
                             KeysType keysType, TStorageType storageType,
                             TStorageMedium storageMedium, List<Column> columns,
                             Set<String> bfColumns, double bfFpp, MarkedCountDownLatch<Long, Long> latch,
                             List<Index> indexes,
                             boolean isInMemory,
                             boolean enablePersistentIndex,
                             int primaryIndexCacheExpireSec,
                             TPersistentIndexType persistentIndexType,
                             TTabletType tabletType, TCompressionType compressionType, List<Integer> sortKeyIdxes,
                             List<Integer> sortKeyUniqueIds,
                             boolean createSchemaFile) {
        this(backendId, dbId, tableId, partitionId, indexId, tabletId, shortKeyColumnCount, schemaHash, version,
                keysType, storageType, storageMedium, columns, bfColumns, bfFpp, latch, indexes, isInMemory,
                enablePersistentIndex, primaryIndexCacheExpireSec, tabletType, compressionType, sortKeyIdxes, sortKeyUniqueIds,
                createSchemaFile);
        this.persistentIndexType = persistentIndexType;
    }

    public void setRecoverySource(RecoverySource source) {
        this.recoverySource = source;
    }

    public RecoverySource getRecoverySource() {
        return this.recoverySource;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
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

    public void setInRestoreMode(boolean inRestoreMode) {
        this.inRestoreMode = inRestoreMode;
    }

    public void setBaseTablet(long baseTabletId, int baseSchemaHash) {
        this.baseTabletId = baseTabletId;
        this.baseSchemaHash = baseSchemaHash;
    }

    public TCreateTabletReq toThrift() {
        TCreateTabletReq createTabletReq = new TCreateTabletReq();
        createTabletReq.setTablet_id(tabletId);

        TTabletSchema tSchema = new TTabletSchema();
        tSchema.setShort_key_column_count(shortKeyColumnCount);
        tSchema.setSchema_hash(schemaHash);
        tSchema.setKeys_type(keysType.toThrift());
        tSchema.setStorage_type(storageType);
        tSchema.setId(indexId); // use index id as the schema id. assume schema change will assign a new index id.
        tSchema.setSchema_version(schemaVersion);

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
        tSchema.setSort_key_unique_ids(sortKeyUniqueIds);

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
        createTabletReq.setTablet_type(tabletType);
        createTabletReq.setCreate_schema_file(createSchemaFile);
        return createTabletReq;
    }
}
