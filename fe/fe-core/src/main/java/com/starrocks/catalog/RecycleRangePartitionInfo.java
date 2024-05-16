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
import com.starrocks.common.util.RangeUtils;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class RecycleRangePartitionInfo extends RecyclePartitionInfoV2 implements GsonPreProcessable,
        GsonPostProcessable {

    private Range<PartitionKey> range;
    // because Range<PartitionKey> and PartitionKey can not be serialized by gson
    // ATTN: call preSerialize before serialization and postDeserialized after deserialization
    @SerializedName(value = "serializedRange")
    private byte[] serializedRange;

    public RecycleRangePartitionInfo(long dbId, long tableId, Partition partition, Range<PartitionKey> range,
                                     DataProperty dataProperty, short replicationNum, boolean isInMemory,
                                     DataCacheInfo dataCacheInfo) {
        super(dbId, tableId, partition, dataProperty, replicationNum,
                isInMemory, dataCacheInfo);
        this.range = range;
    }

    @Override
    public Range<PartitionKey> getRange() {
        return range;
    }

    @Override
    void recover(OlapTable table) throws DdlException {
        RecyclePartitionInfo.recoverRangePartition(table, this);
    }

    private byte[] serializeRange(Range<PartitionKey> range) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(stream);
        RangeUtils.writeRange(dos, range);
        return stream.toByteArray();
    }

    private Range<PartitionKey> deserializeRange(byte[] serializedRange) throws IOException {
        InputStream inputStream = new ByteArrayInputStream(serializedRange);
        DataInput dataInput = new DataInputStream(inputStream);
        return RangeUtils.readRange(dataInput);
    }

    @Override
    public void gsonPreProcess() throws IOException {
        serializedRange = serializeRange(range);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        range = deserializeRange(serializedRange);
    }
}
