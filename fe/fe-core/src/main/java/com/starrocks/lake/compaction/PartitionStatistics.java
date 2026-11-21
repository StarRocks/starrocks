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


package com.starrocks.lake.compaction;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.image.ProtoEnums;
import com.starrocks.persist.proto.PartitionStatisticsPB;

import javax.annotation.Nullable;

public class PartitionStatistics {
    @SerializedName(value = "partition")
    private final PartitionIdentifier partition;
    @SerializedName(value = "compactionVersion")
    private PartitionVersion compactionVersion;
    @SerializedName(value = "currentVersion")
    private PartitionVersion currentVersion;
    @SerializedName(value = "nextCompactionTime")
    private long nextCompactionTime;
    // The scheduler deferred this partition because its table was under an alter. This is leader-local.
    private transient volatile long alterCompactionRetryTime;
    @SerializedName(value = "compactionScore")
    private Quantiles compactionScore;
    // default priority is 0, manual compaction will have priority value 1
    @SerializedName(value = "priority")
    private volatile CompactionPriority priority = CompactionPriority.DEFAULT;
    // not persist on purpose, used to control the interval of continuous partial success compaction
    private int punishFactor = 1;

    public enum CompactionPriority {
        DEFAULT(0),
        MANUAL_COMPACT(1),
        UNSHARE(2);

        private final int value;

        CompactionPriority(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public PartitionStatistics(PartitionIdentifier partition) {
        this.partition = partition;
        this.compactionVersion = new PartitionVersion(0 /* version */, System.currentTimeMillis() /* createTime */);
        this.nextCompactionTime = 0;
        this.punishFactor = 1;
    }

    public PartitionIdentifier getPartition() {
        return partition;
    }

    public PartitionVersion getCurrentVersion() {
        return currentVersion;
    }

    public void setCurrentVersion(PartitionVersion currentVersion) {
        this.currentVersion = currentVersion;
    }

    public PartitionVersion getCompactionVersion() {
        return compactionVersion;
    }

    public void setCompactionVersion(PartitionVersion value) {
        compactionVersion = value;
    }

    public long getLastCompactionTime() {
        return getCompactionVersion().getCreateTime();
    }

    public void setNextCompactionTime(long nextCompactionTime) {
        this.nextCompactionTime = nextCompactionTime;
    }

    public long getNextCompactionTime() {
        return Math.max(nextCompactionTime, alterCompactionRetryTime);
    }

    void setAlterCompactionRetryTime(long retryTime) {
        this.alterCompactionRetryTime = retryTime;
    }

    void clearCompactionRetryTimes() {
        setNextCompactionTime(0);
        setAlterCompactionRetryTime(0);
    }

    public long getDeltaVersions() {
        return getCurrentVersion().getVersion() - getCompactionVersion().getVersion();
    }

    public int getPunishFactor() {
        return punishFactor;
    }

    private void adjustPunishFactor(Quantiles newCompactionScore, boolean isPartialSuccess) {
        if (compactionScore != null && newCompactionScore != null) {
            if (compactionScore.getMax() == newCompactionScore.getMax() && isPartialSuccess) {
                // this means partial compaction succeeds, need increase punish factor,
                // so that other partitions' compaction can proceed.
                // max interval will be Config.lake_compaction_interval_ms_on_success * punishFactor
                punishFactor = Math.min(punishFactor * 2, 360);
            } else {
                punishFactor = 1;
            }
        } else {
            punishFactor = 1;
        }
    }

    public void setCompactionScore(@Nullable Quantiles compactionScore) {
        this.compactionScore = compactionScore;
    }

    // should only called by compaction
    public void setCompactionScoreAndAdjustPunishFactor(@Nullable Quantiles compactionScore, boolean isPartialSuccess) {
        adjustPunishFactor(compactionScore, isPartialSuccess);
        setCompactionScore(compactionScore);
    }

    @Nullable
    public Quantiles getCompactionScore() {
        return compactionScore;
    }

    public CompactionPriority getPriority() {
        // For backward compatibility
        // prevent null value when deserializing JSON that doesn't include the priority field
        return priority == null ? CompactionPriority.DEFAULT : priority;
    }

    public void setPriority(CompactionPriority priority) {
        this.priority = priority;
    }

    public void resetPriority() {
        this.setPriority(CompactionPriority.DEFAULT);
    }

    public PartitionStatisticsSnapshot getSnapshot() {
        return new PartitionStatisticsSnapshot(this);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public PartitionStatisticsPB toProto() {
        PartitionStatisticsPB.Builder builder = PartitionStatisticsPB.newBuilder();
        if (partition != null) {
            builder.setPartition(partition.toProto());
        }
        if (compactionVersion != null) {
            builder.setCompactionVersion(compactionVersion.toProto());
        }
        if (currentVersion != null) {
            builder.setCurrentVersion(currentVersion.toProto());
        }
        builder.setNextCompactionTime(nextCompactionTime);
        if (compactionScore != null) {
            builder.setCompactionScore(compactionScore.toProto());
        }
        if (priority != null) {
            builder.setPriority(ProtoEnums.toProto(priority, PartitionStatisticsPB.Priority.class));
        }
        return builder.build();
    }

    public static PartitionStatistics fromProto(PartitionStatisticsPB pb) {
        PartitionStatistics result = new PartitionStatistics(
                pb.hasPartition() ? PartitionIdentifier.fromProto(pb.getPartition()) : null);
        // V2 Gson restoration bypasses the constructor and leaves this runtime field at zero.
        result.punishFactor = 0;
        result.priority = null;
        result.compactionVersion = null;
        if (pb.hasCompactionVersion()) {
            result.compactionVersion = PartitionVersion.fromProto(pb.getCompactionVersion());
        }
        if (pb.hasCurrentVersion()) {
            result.currentVersion = PartitionVersion.fromProto(pb.getCurrentVersion());
        }
        result.nextCompactionTime = pb.getNextCompactionTime();
        if (pb.hasCompactionScore()) {
            result.compactionScore = Quantiles.fromProto(pb.getCompactionScore());
        }
        if (pb.hasPriority()) {
            result.priority = ProtoEnums.fromProto(pb.getPriority(), CompactionPriority.class);
        }
        return result;
    }
}
