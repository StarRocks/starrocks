// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Partition;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ListPartitionPersistInfo extends PartitionPersistInfoV2 {

    @SerializedName("values")
    private List<String> values;
    @SerializedName("multiValues")
    private List<List<String>> multiValues;

    public ListPartitionPersistInfo() {
        this.values = new ArrayList<>();
        this.multiValues = new ArrayList<>();
    }

    @Override
    public PartitionPersistInfoV2 readFieldIn(DataInput in) throws IOException {
        String json = Text.readString(in);
        ListPartitionPersistInfo info = GsonUtils.GSON.fromJson(json, ListPartitionPersistInfo.class);
        return info;
    }

    public ListPartitionPersistInfo(Long dbId, Long tableId, Partition partition,
                                    DataProperty dataProperty, short replicationNum,
                                    boolean isInMemory, boolean isTempPartition,
                                    List<String> values, List<List<String>> multiValues) {
        super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory, isTempPartition);
        this.multiValues = multiValues;
        this.values = values;
    }

    public List<String> getValues() {
        return values;
    }

    public List<List<String>> getMultiValues() {
        return multiValues;
    }

}
