// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;

public class UnPartitionPersistInfo extends PartitionPersistInfoV2 {

    public UnPartitionPersistInfo() {
    }

    public UnPartitionPersistInfo(Long dbId, Long tableId, Partition partition,
                                  DataProperty dataProperty, short replicationNum,
                                  boolean isInMemory, boolean isTempPartition) {
        super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory, isTempPartition);
    }

    @Override
    public PartitionPersistInfoV2 readFieldIn(DataInput in) throws IOException {
        String json = Text.readString(in);
        UnPartitionPersistInfo info = GsonUtils.GSON.fromJson(json, UnPartitionPersistInfo.class);
        return info;
    }
}
