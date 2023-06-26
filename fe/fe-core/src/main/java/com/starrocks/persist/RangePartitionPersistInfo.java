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

import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
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

public class RangePartitionPersistInfo extends PartitionPersistInfoV2
        implements GsonPreProcessable, GsonPostProcessable {

    private Range<PartitionKey> range;

    @SerializedName(value = "serializedRange")
    private byte[] serializedRange;

    public RangePartitionPersistInfo(Long dbId, Long tableId, Partition partition,
                                     DataProperty dataProperty, short replicationNum,
                                     boolean isInMemory, boolean isTempPartition,
                                     Range<PartitionKey> range,
                                     DataCacheInfo dataCacheInfo) {
        super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory, isTempPartition, dataCacheInfo);
        this.range = range;
    }

    public Range<PartitionKey> getRange() {
        return range;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(serializedRange);
        DataInput dataInput = new DataInputStream(inputStream);
        this.range = RangeUtils.readRange(dataInput);
    }

    @Override
    public void gsonPreProcess() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(stream);
        RangeUtils.writeRange(dos, range);
        this.serializedRange = stream.toByteArray();
    }

}