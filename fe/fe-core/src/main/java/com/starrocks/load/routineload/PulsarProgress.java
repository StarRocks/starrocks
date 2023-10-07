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

package com.starrocks.load.routineload;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksFEMetaVersion;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PulsarUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TPulsarMessageId;
import com.starrocks.thrift.TPulsarRLTaskProgress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this is description of pulsar routine load progress
 * the data before position was already loaded in StarRocks
 */
// {"partitionToBacklogNum": {}}
public class PulsarProgress extends RoutineLoadProgress {
    private static final Logger LOG = LogManager.getLogger(PulsarProgress.class);

    // https://github.com/apache/pulsar/blob/master/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/MessageId.java#L86
    public static TPulsarMessageId messageIdEarliest = new TPulsarMessageId(-1, -1L, -1, -1);
    public static TPulsarMessageId messageIdLatest = new TPulsarMessageId(9223372036854775807L, 9223372036854775807L, -1, -1);
    public static final String POSITION_EARLIEST = "POSITION_EARLIEST";
    public static final String POSITION_LATEST = "POSITION_LATEST";

    // (partition id -> begin position)
    private Map<String, TPulsarMessageId> partitionToPosition = Maps.newConcurrentMap();

    public PulsarProgress() {
        super(LoadDataSourceType.PULSAR);
    }

    public PulsarProgress(TPulsarRLTaskProgress tPulsarRLTaskProgress) {
        super(LoadDataSourceType.PULSAR);
        this.partitionToPosition = tPulsarRLTaskProgress.getPartitionCmtPosition();
    }

    public Map<String, TPulsarMessageId> getPartitionToPosition(List<String> partitions) {
        Map<String, TPulsarMessageId> result = Maps.newHashMap();
        for (Map.Entry<String, TPulsarMessageId> entry : partitionToPosition.entrySet()) {
            for (String partition : partitions) {
                if (entry.getKey().equals(partition)) {
                    result.put(partition, entry.getValue());
                }
            }
        }
        return result;
    }

    public TPulsarMessageId getPositionByPartition(String partition) {
        if (partitionToPosition.containsKey(partition)) {
            return partitionToPosition.get(partition);
        } else {
            return messageIdLatest;
        }
    }

    public void addPartitionToPosition(Pair<String, TPulsarMessageId> partitionToPosition) {
        this.partitionToPosition.put(partitionToPosition.first, partitionToPosition.second);
    }

    public void modifyPosition(List<Pair<String, TPulsarMessageId>> partitionToPositions) {
        for (Pair<String, TPulsarMessageId> pair : partitionToPositions) {
            this.partitionToPosition.put(pair.first, pair.second);
        }
    }

    // convert position of POSITION_LATEST to latest message id
    public void convertPosition(String serviceUrl, String topic, String subscription,
                                Map<String, String> properties) throws UserException {
        List<String> latestPartitions = Lists.newArrayList();
        for (Map.Entry<String, TPulsarMessageId> entry : partitionToPosition.entrySet()) {
            String partition = entry.getKey();
            TPulsarMessageId position = entry.getValue();
            if (PulsarUtil.messageIdEq(position, messageIdLatest)) {
                latestPartitions.add(partition);
            }
        }

        if (!latestPartitions.isEmpty()) {
            Map<String, TPulsarMessageId> partitionPositions = PulsarUtil.getLatestMessageIds(
                    serviceUrl, topic, subscription, ImmutableMap.copyOf(properties), latestPartitions);
            partitionToPosition.putAll(partitionPositions);
        }
    }

    public boolean containsPartition(String pulsarPartition) {
        return partitionToPosition.containsKey(pulsarPartition);
    }

    private void getReadableProgress(Map<String, String> showPartitionToPosition) {
        for (Map.Entry<String, TPulsarMessageId> entry : partitionToPosition.entrySet()) {
            TPulsarMessageId messageId = entry.getValue();
            if (entry.getValue() == messageIdEarliest) {
                showPartitionToPosition.put(entry.getKey(), POSITION_EARLIEST);
            } else if (entry.getValue() == messageIdLatest) {
                showPartitionToPosition.put(entry.getKey(), POSITION_LATEST);
            } else {
                showPartitionToPosition.put(entry.getKey(), PulsarUtil.formatMessageId(messageId));
            }
        }
    }

    @Override
    public String toString() {
        Map<String, String> showPartitionPosition = Maps.newHashMap();
        getReadableProgress(showPartitionPosition);
        return "PulsarProgress [partitionToPosition="
                + Joiner.on("|").withKeyValueSeparator("_").join(showPartitionPosition) + "]";
    }

    @Override
    public String toJsonString() {
        Map<String, String> showPartitionPosition = Maps.newHashMap();
        getReadableProgress(showPartitionPosition);
        Gson gson = new Gson();
        return gson.toJson(showPartitionPosition);
    }

    @Override
    public void update(RLTaskTxnCommitAttachment attachment) {
        PulsarProgress newProgress = (PulsarProgress) attachment.getProgress();
        for (Map.Entry<String, TPulsarMessageId> entry : newProgress.partitionToPosition.entrySet()) {
            String partition = entry.getKey();
            TPulsarMessageId messageId = entry.getValue();
            // Update progress
            partitionToPosition.put(partition, messageId);
        }
        LOG.debug("update pulsar progress: {}, task: {}, job: {}",
                newProgress.toJsonString(), DebugUtil.printId(attachment.getTaskId()), attachment.getJobId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(partitionToPosition.size());
        for (Map.Entry<String, TPulsarMessageId> entry : partitionToPosition.entrySet()) {
            Text.writeString(out, entry.getKey());
            TPulsarMessageId messageId = entry.getValue();
            out.writeInt(messageId.getPartition());
            out.writeLong(messageId.getLedgerId());
            out.writeLong(messageId.getEntryId());
            out.writeInt(messageId.getBatchIndex());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        partitionToPosition = new HashMap<String, TPulsarMessageId>();
        for (int i = 0; i < size; i++) {
            if (GlobalStateMgr.getCurrentStateStarRocksMetaVersion() <= StarRocksFEMetaVersion.VERSION_5) {
                String partition = Text.readString(in);
                in.readLong();
                TPulsarMessageId messageId = messageIdLatest;
                partitionToPosition.put(partition, messageId);
            } else {
                String topicPartition = Text.readString(in);
                TPulsarMessageId messageId = new TPulsarMessageId();
                messageId.partition = in.readInt();
                messageId.ledgerId = in.readLong();
                messageId.entryId = in.readLong();
                messageId.batchIndex = in.readInt();
                partitionToPosition.put(topicPartition, messageId);
            }
        }
    }
}
