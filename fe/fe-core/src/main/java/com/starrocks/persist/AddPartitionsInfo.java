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

import com.google.common.collect.Lists;
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
