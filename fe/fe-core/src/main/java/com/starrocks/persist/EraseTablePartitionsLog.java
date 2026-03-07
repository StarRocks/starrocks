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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.util.List;

/**
 * WAL log for batch erasing partitions that belong to a table being deleted from the recycle bin.
 * This serves as an intermediate checkpoint during Lake table deletion, so that after FE restart,
 * only the remaining (un-logged) partitions need to be retried.
 */
public class EraseTablePartitionsLog implements Writable {

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "tableId")
    private long tableId;

    @SerializedName(value = "partitionIds")
    private List<Long> partitionIds;

    public EraseTablePartitionsLog(long dbId, long tableId, List<Long> partitionIds) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionIds = partitionIds;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }
}
