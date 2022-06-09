// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.google.common.collect.Range;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RangePartitionPersistInfo extends PartitionPersistInfoV2 {

    private Range<PartitionKey> range;

    public RangePartitionPersistInfo() {
    }

    public RangePartitionPersistInfo(Long dbId, Long tableId, Partition partition,
                                     DataProperty dataProperty, short replicationNum,
                                     boolean isInMemory, boolean isTempPartition,
                                     Range<PartitionKey> range) {
        super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory, isTempPartition);
        this.range = range;
    }

    public Range<PartitionKey> getRange() {
        return range;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        RangeUtils.writeRange(out, range);
    }

    @Override
    public PartitionPersistInfoV2 readFieldIn(DataInput in) throws IOException {
        String json = Text.readString(in);
        RangePartitionPersistInfo info = GsonUtils.GSON.fromJson(json, RangePartitionPersistInfo.class);
        info.range = RangeUtils.readRange(in);
        return info;
    }
}
