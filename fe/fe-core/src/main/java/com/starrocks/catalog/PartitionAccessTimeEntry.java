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

package com.starrocks.catalog;

/**
 * One partition's durable access-time record, flushed from each FE's own in-memory map into the internal
 * {@code _statistics_.partition_access_time} table (MAX-aggregated across FEs). An in-process data carrier
 * between {@link PartitionAccessTimeMgr}, {@link PartitionAccessTimeStore} and {@link PartitionAccessTimePersister}.
 */
public class PartitionAccessTimeEntry {
    private final long dbId;
    private final long tableId;
    private final long partitionId;
    private final long accessTimeMs;

    public PartitionAccessTimeEntry(long dbId, long tableId, long partitionId, long accessTimeMs) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.accessTimeMs = accessTimeMs;
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

    public long getAccessTimeMs() {
        return accessTimeMs;
    }
}
