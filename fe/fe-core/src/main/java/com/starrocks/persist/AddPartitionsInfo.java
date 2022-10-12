// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.persist;

import com.clearspring.analytics.util.Lists;
import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * used for batch add multi partitions in one atomic operation
 */

public class AddPartitionsInfo implements Writable {

    private final List<PartitionPersistInfo> infos;

    public AddPartitionsInfo(List<PartitionPersistInfo> infos) {
        this.infos = infos;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(infos.size());
        for (PartitionPersistInfo info : infos) {
            info.write(out);
        }
    }

    public static AddPartitionsInfo read(DataInput in) throws IOException {
        int size = in.readInt();
        List<PartitionPersistInfo> infos = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            infos.add(PartitionPersistInfo.read(in));
        }
        return new AddPartitionsInfo(infos);
    }

    public List<PartitionPersistInfo> getAddPartitionInfos() {
        return infos;
    }

}
