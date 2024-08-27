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

import com.google.common.collect.Range;
import com.starrocks.common.DdlException;
import com.starrocks.lake.DataCacheInfo;

// only for RangePartition
public class RecyclePartitionInfoV1 extends RecyclePartitionInfo {
    protected Range<PartitionKey> range;

    public RecyclePartitionInfoV1() {
        super();
    }

    public RecyclePartitionInfoV1(long dbId, long tableId, Partition partition, Range<PartitionKey> range,
                                  DataProperty dataProperty, short replicationNum, boolean isInMemory) {
        super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory);
        this.range = range;
    }

    @Override
    public Range<PartitionKey> getRange() {
        return range;
    }

    @Override
    public DataCacheInfo getDataCacheInfo() {
        return null;
    }

    @Override
    void recover(OlapTable table) throws DdlException {
        RecyclePartitionInfo.recoverRangePartition(table, this);
    }
}
