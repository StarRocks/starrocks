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

import com.starrocks.memory.estimate.ShallowMemory;
import com.starrocks.thrift.TStorageMedium;

@ShallowMemory
public class TabletMeta {
    private final long dbId;
    private final long tableId;
    private final long physicalPartitionId;
    private final long indexId;

    private TStorageMedium storageMedium;

    private final boolean isLakeTablet;

    /**
     * If currentTimeMs is ahead of `toBeCleanedTimeMs`, the tablet meta will be cleaned from TabletInvertedIndex.
     */
    private Long toBeCleanedTimeMs = null;

    public TabletMeta(long dbId, long tableId, long physicalPartitionId, long indexId,
                      TStorageMedium storageMedium,
                      boolean isLakeTablet) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.physicalPartitionId = physicalPartitionId;
        this.indexId = indexId;
        this.storageMedium = storageMedium;
        this.isLakeTablet = isLakeTablet;
    }

    public TabletMeta(long dbId, long tableId, long physicalPartitionId, long indexId,
                      TStorageMedium storageMedium) {
        this(dbId, tableId, physicalPartitionId, indexId, storageMedium, false);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
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

    public Long getToBeCleanedTime() {
        return toBeCleanedTimeMs;
    }

    public void setToBeCleanedTime(Long time) {
        toBeCleanedTimeMs = time;
    }

    public void resetToBeCleanedTime() {
        toBeCleanedTimeMs = null;
    }

    public boolean isLakeTablet() {
        return isLakeTablet;
    }

    @Override
    public String toString() {
        return "dbId=" + dbId +
                " tableId=" + tableId +
                " physicalPartitionId=" + physicalPartitionId +
                " indexId=" + indexId;
    }
}
