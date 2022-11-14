// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.util.RangeUtils;
import com.starrocks.lake.StorageCacheInfo;
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
                                     StorageCacheInfo storageCacheInfo) {
        super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory, isTempPartition, storageCacheInfo);
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