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
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecyclePartitionInfoV2 extends RecyclePartitionInfo {
    @SerializedName(value = "storageCacheInfo")
    private DataCacheInfo dataCacheInfo;

    public RecyclePartitionInfoV2(long dbId, long tableId, Partition partition,
                                  DataProperty dataProperty, short replicationNum, boolean isInMemory,
                                  DataCacheInfo dataCacheInfo) {
        super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory);
        this.dataCacheInfo = dataCacheInfo;
    }

    @Override
    public Range<PartitionKey> getRange() {
        return null;
    }

    @Override
    public DataCacheInfo getDataCacheInfo() {
        return dataCacheInfo;
    }

    @Override
    void recover(OlapTable table) throws DdlException {
        RecyclePartitionInfo.recoverRangePartition(table, this);
    }

    public static RecyclePartitionInfoV2 read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, RecyclePartitionInfoV2.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(-1L);
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
