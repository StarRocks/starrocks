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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/KafkaProgress.java

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

package com.starrocks.load.routineload;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.KafkaUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this is description of kafka routine load progress
 * the data before offset was already loaded in StarRocks
 */
// {"partitionIdToOffset": {}}
public class KafkaProgress extends RoutineLoadProgress {
    private static final Logger LOG = LogManager.getLogger(KafkaProgress.class);

    public static final String OFFSET_BEGINNING = "OFFSET_BEGINNING"; // -2
    public static final String OFFSET_END = "OFFSET_END"; // -1
    // OFFSET_ZERO is just for show info, if user specified offset is 0
    public static final String OFFSET_ZERO = "OFFSET_ZERO";

    public static final long OFFSET_BEGINNING_VAL = -2;
    public static final long OFFSET_END_VAL = -1;

    // (partition id, begin offset)
    @SerializedName("po")
    private Map<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();

    public KafkaProgress() {
        super(LoadDataSourceType.KAFKA);
    }

    public KafkaProgress(Map<Integer, Long> partitionOffsets) {
        super(LoadDataSourceType.KAFKA);
        if (partitionOffsets != null) {
            this.partitionIdToOffset = partitionOffsets;
        }
    }

    public Map<Integer, Long> getPartitionIdToOffset(List<Integer> partitionIds) {
        Map<Integer, Long> result = Maps.newHashMap();
        for (Map.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            for (Integer partitionId : partitionIds) {
                if (entry.getKey().equals(partitionId)) {
                    result.put(partitionId, entry.getValue());
                }
            }
        }
        return result;
    }

    public ImmutableMap<Integer, Long> getPartitionIdToOffset() {
        return ImmutableMap.copyOf(partitionIdToOffset);
    }

    public void addPartitionOffset(Pair<Integer, Long> partitionOffset) {
        partitionIdToOffset.put(partitionOffset.first, partitionOffset.second);
    }

    public Long getOffsetByPartition(int kafkaPartition) {
        return partitionIdToOffset.get(kafkaPartition);
    }

    public boolean containsPartition(Integer kafkaPartition) {
        return partitionIdToOffset.containsKey(kafkaPartition);
    }

    public boolean hasPartition() {
        return !partitionIdToOffset.isEmpty();
    }

    // (partition id, end offset)
    // OFFSET_ZERO: user set offset == 0, no committed msg
    // OFFSET_END: user set offset = OFFSET_END, no committed msg
    // OFFSET_BEGINNING: user set offset = OFFSET_BEGINNING, no committed msg
    // other: current committed msg's offset
    private void getReadableProgress(Map<Integer, String> showPartitionIdToOffset) {
        for (Map.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            if (entry.getValue() == 0) {
                showPartitionIdToOffset.put(entry.getKey(), OFFSET_ZERO);
            } else if (entry.getValue() == -1) {
                showPartitionIdToOffset.put(entry.getKey(), OFFSET_END);
            } else if (entry.getValue() == -2) {
                showPartitionIdToOffset.put(entry.getKey(), OFFSET_BEGINNING);
            } else {
                // The offset saved in partitionIdToOffset is the next offset to be consumed.
                // So here we minus 1 to return the "already consumed" offset.
                showPartitionIdToOffset.put(entry.getKey(), "" + (entry.getValue() - 1));
            }
        }
    }

    // modify the partition offset of this progress.
    // all partitions are validated by the caller
    public void modifyOffset(List<Pair<Integer, Long>> kafkaPartitionOffsets) throws DdlException {
        for (Pair<Integer, Long> pair : kafkaPartitionOffsets) {
            partitionIdToOffset.put(pair.first, pair.second);
        }
        // update kafkaPartitionOffsets as well, so that the current partitonIdToOffset can be completely persisted
        for (Integer partitionId : partitionIdToOffset.keySet()) {
            Pair<Integer, Long> pair = new Pair<>(partitionId, partitionIdToOffset.get(partitionId));
            if (!kafkaPartitionOffsets.contains(pair)) {
                LOG.info("add {} to kafkaPartitionOffsets {}", pair, kafkaPartitionOffsets);
                kafkaPartitionOffsets.add(pair);
            }
        }
    }

    // convert offset of OFFSET_END and OFFSET_BEGINNING to current offset number
    public void convertOffset(String brokerList, String topic, Map<String, String> properties) throws UserException {
        List<Integer> beginningPartitions = Lists.newArrayList();
        List<Integer> endPartitions = Lists.newArrayList();
        for (Map.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            Integer part = entry.getKey();
            Long offset = entry.getValue();
            if (offset == -2L) {
                beginningPartitions.add(part);
            }
            if (offset == -1L) {
                endPartitions.add(part);
            }
        }

        if (beginningPartitions.size() > 0) {
            Map<Integer, Long> partOffsets = KafkaUtil
                    .getBeginningOffsets(brokerList, topic, ImmutableMap.copyOf(properties), beginningPartitions);
            partitionIdToOffset.putAll(partOffsets);
        }
        if (endPartitions.size() > 0) {
            Map<Integer, Long> partOffsets =
                    KafkaUtil.getLatestOffsets(brokerList, topic, ImmutableMap.copyOf(properties), endPartitions);
            partitionIdToOffset.putAll(partOffsets);
        }
    }

    @Override
    public String toString() {
        Map<Integer, String> showPartitionIdToOffset = Maps.newHashMap();
        getReadableProgress(showPartitionIdToOffset);
        return "KafkaProgress [partitionIdToOffset="
                + Joiner.on("|").withKeyValueSeparator("_").join(showPartitionIdToOffset) + "]";
    }

    @Override
    public String toJsonString() {
        Map<Integer, String> showPartitionIdToOffset = Maps.newHashMap();
        getReadableProgress(showPartitionIdToOffset);
        Gson gson = new Gson();
        return gson.toJson(showPartitionIdToOffset);
    }

    @Override
    public void update(RoutineLoadProgress progress) {
        KafkaProgress newProgress = (KafkaProgress) progress;
        // + 1 to point to the next msg offset to be consumed
        newProgress.partitionIdToOffset.entrySet().stream()
                .forEach(entity -> this.partitionIdToOffset.put(entity.getKey(), entity.getValue() + 1));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(partitionIdToOffset.size());
        for (Map.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            out.writeInt((Integer) entry.getKey());
            out.writeLong((Long) entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        partitionIdToOffset = new HashMap<>();
        for (int i = 0; i < size; i++) {
            partitionIdToOffset.put(in.readInt(), in.readLong());
        }
    }

}
