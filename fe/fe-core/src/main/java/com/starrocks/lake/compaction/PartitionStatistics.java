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
    @SerializedName(value = "compactionScore")
    private Quantiles compactionScore;
    // default priority is 0, manual compaction will have priority value 1
    @SerializedName(value = "priority")
    private volatile CompactionPriority priority = CompactionPriority.DEFAULT;
    // not persist on purpose, used to control the interval of continuous partial success compaction
    private int punishFactor = 1;

    public enum CompactionPriority {
        DEFAULT(0),
        MANUAL_COMPACT(1);

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
        return nextCompactionTime;
    }

    public long getDeltaVersions() {
        return getCurrentVersion().getVersion() - getCompactionVersion().getVersion();
    }

    public int getPunishFactor() {
        return punishFactor;
    }

    private void adjustPunishFactor(Quantiles newCompactionScore) {
        if (compactionScore != null && newCompactionScore != null) {
            if (compactionScore.getMax() == newCompactionScore.getMax()) {
                // this means partial compaction succeeds, need increase punish factor,
                // so that other partitions' compaction can proceed.
                // max interval will be CompactionScheduler.MIN_COMPACTION_INTERVAL_MS_ON_SUCCESS * punishFactor
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
    public void setCompactionScoreAndAdjustPunishFactor(@Nullable Quantiles compactionScore) {
        adjustPunishFactor(compactionScore);
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
}

