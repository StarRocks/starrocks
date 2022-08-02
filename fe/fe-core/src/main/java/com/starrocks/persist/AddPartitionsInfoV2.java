// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.clearspring.analytics.util.Lists;
import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class AddPartitionsInfoV2 implements Writable {

    private final List<PartitionPersistInfoV2> infos;

    public AddPartitionsInfoV2(List<PartitionPersistInfoV2> infos) {
        this.infos = infos;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (PartitionPersistInfoV2 info : infos) {
            info.write(out);
        }
    }

    public static AddPartitionsInfoV2 read(DataInput in) throws IOException {
        int size = in.readInt();
        List<PartitionPersistInfoV2> infos = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            infos.add(PartitionPersistInfoV2.read(in));
        }
        return new AddPartitionsInfoV2(infos);
    }

    public List<PartitionPersistInfoV2> getAddPartitionInfos() {
        return infos;
    }
}
