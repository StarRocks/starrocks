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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/TempPartitions.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

// This class saved all temp partitions of a table.
// temp partition is used to implement the overwrite load.
// user can load data into some of the temp partitions,
// and then replace the formal partitions with these temp partitions
// to make an overwrite load.
public class TempPartitions implements Writable, GsonPostProcessable {
    @SerializedName(value = "idToPartition")
    private Map<Long, Partition> idToPartition = Maps.newHashMap();
    private Map<String, Partition> nameToPartition = Maps.newHashMap();
    @Deprecated
    // the range info of temp partitions has been moved to "partitionInfo" in OlapTable.
    // this field is deprecated.
    private RangePartitionInfo partitionInfo = null;

    public TempPartitions() {
    }

    public void addPartition(Partition partition) {
        idToPartition.put(partition.getId(), partition);
        nameToPartition.put(partition.getName(), partition);
    }

    /*
     * Drop temp partitions.
     * If needDropTablet is true, also drop the tablet from tablet inverted index.
     */
    public void dropPartition(String partitionName, boolean needDropTablet) {
        Partition partition = nameToPartition.get(partitionName);
        if (partition != null) {
            idToPartition.remove(partition.getId());
            nameToPartition.remove(partitionName);
            if (!GlobalStateMgr.isCheckpointThread() && needDropTablet) {
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    for (Tablet tablet : index.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }
        }
    }

    public Partition getPartition(long partitionId) {
        return idToPartition.get(partitionId);
    }

    public Partition getPartition(String partitionName) {
        return nameToPartition.get(partitionName);
    }

    public List<Partition> getAllPartitions() {
        return Lists.newArrayList(idToPartition.values());
    }

    public boolean hasPartition(String partName) {
        return nameToPartition.containsKey(partName);
    }

    public boolean isEmpty() {
        return idToPartition.isEmpty();
    }

    @Deprecated
    public RangePartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    @Deprecated
    public void unsetPartitionInfo() {
        partitionInfo = null;
    }

    // drop all temp partitions
    public void dropAll() {
        Set<String> partNames = Sets.newHashSet(nameToPartition.keySet());
        for (String partName : partNames) {
            dropPartition(partName, true);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static TempPartitions read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TempPartitions.class);
    }

    @Override
    public void gsonPostProcess() {
        for (Partition partition : idToPartition.values()) {
            nameToPartition.put(partition.getName(), partition);
        }
    }
}
