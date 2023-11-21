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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/TabletMeta.java

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

import com.starrocks.thrift.TStorageMedium;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TabletMeta {
    private final long dbId;
    private final long tableId;
    private final long partitionId;
    private final long physicalPartitionId;
    private final long indexId;

    private final int oldSchemaHash;
    private final int newSchemaHash;

    private TStorageMedium storageMedium;

    private final boolean isLakeTablet;

    /**
     * If currentTimeMs is ahead of `toBeCleanedTimeMs`, the tablet meta will be cleaned from TabletInvertedIndex.
     */
    private Long toBeCleanedTimeMs = null;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public TabletMeta(long dbId, long tableId, long partitionId, long physicalPartitionId, long indexId, int schemaHash,
                      TStorageMedium storageMedium, boolean isLakeTablet) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.physicalPartitionId = physicalPartitionId;
        this.indexId = indexId;

        this.oldSchemaHash = schemaHash;
        this.newSchemaHash = -1;

        this.storageMedium = storageMedium;

        this.isLakeTablet = isLakeTablet;
    }

    // for single physical partition, the physicalPartitionId is same as partitionId
    public TabletMeta(long dbId, long tableId, long partitionId, long indexId, int schemaHash,
                      TStorageMedium storageMedium, boolean isLakeTablet) {
        this(dbId, tableId, partitionId, partitionId, indexId, schemaHash, storageMedium, isLakeTablet);
    }

    public TabletMeta(long dbId, long tableId, long partitionId, long indexId, int schemaHash,
                      TStorageMedium storageMedium) {
        this(dbId, tableId, partitionId, indexId, schemaHash, storageMedium, false);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getPhysicalPartitionId() {
        return physicalPartitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public void setStorageMedium(TStorageMedium storageMedium) {
        this.storageMedium = storageMedium;
    }

    public int getNewSchemaHash() {
        lock.readLock().lock();
        try {
            return this.newSchemaHash;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getOldSchemaHash() {
        lock.readLock().lock();
        try {
            return this.oldSchemaHash;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Long getToBeCleanedTime() {
        return toBeCleanedTimeMs;
    }

    public void setToBeCleanedTime(Long time) {
        toBeCleanedTimeMs = time;
    }

    public void resetToBeCleanedTime() {
        toBeCleanedTimeMs = null;
    }

    public boolean containsSchemaHash(int schemaHash) {
        lock.readLock().lock();
        try {
            return this.oldSchemaHash == schemaHash || this.newSchemaHash == schemaHash;
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isLakeTablet() {
        return isLakeTablet;
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("dbId=").append(dbId);
            sb.append(" tableId=").append(tableId);
            sb.append(" partitionId=").append(partitionId);
            sb.append(" physicalPartitionId=").append(physicalPartitionId);
            sb.append(" indexId=").append(indexId);
            sb.append(" oldSchemaHash=").append(oldSchemaHash);
            sb.append(" newSchemaHash=").append(newSchemaHash);

            return sb.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
}
