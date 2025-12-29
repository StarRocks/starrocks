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

package com.starrocks.lake.restore;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

/**
 * Static definition of a physical partition restore task.
 * Contains the full set of index and tablet mappings that belong to the same physical partition.
 */
public class RestoreTask {
    public enum PartitionTaskState {
        PENDING,
        RUNNING,
        SUCCESS,
        FAILED
    }

    @SerializedName(value = "targetPhysicalPartitionId")
    private final long targetPhysicalPartitionId;
    @SerializedName(value = "sourceVisibleVersion")
    private final long sourceVisibleVersion;
    @SerializedName(value = "tabletEntries")
    private final List<TabletRestoreEntry> tabletEntries;
    @SerializedName(value = "taskState")
    private PartitionTaskState taskState;

    public RestoreTask(long targetPhysicalPartitionId,
                       long sourceVisibleVersion,
                       List<TabletRestoreEntry> tabletEntries) {
        this.targetPhysicalPartitionId = targetPhysicalPartitionId;
        this.sourceVisibleVersion = sourceVisibleVersion;
        this.tabletEntries = tabletEntries == null ? new ArrayList<>() : tabletEntries;
        this.taskState = PartitionTaskState.PENDING;
    }

    public long getTargetPhysicalPartitionId() {
        return targetPhysicalPartitionId;
    }

    public long getSourceVisibleVersion() {
        return sourceVisibleVersion;
    }

    public List<TabletRestoreEntry> getTabletEntries() {
        return tabletEntries;
    }

    public TabletRestoreEntry getRepresentativeEntry() {
        return tabletEntries.isEmpty() ? null : tabletEntries.get(0);
    }

    public PartitionTaskState getTaskState() {
        return taskState;
    }

    public void setTaskState(PartitionTaskState taskState) {
        this.taskState = taskState;
    }

    public static class TabletRestoreEntry {
        @SerializedName(value = "targetSchemaId")
        private final long targetSchemaId;
        @SerializedName(value = "sourceTabletId")
        private final long sourceTabletId;
        @SerializedName(value = "targetTabletId")
        private final long targetTabletId;

        public TabletRestoreEntry(long targetSchemaId, long sourceTabletId, long targetTabletId) {
            this.targetSchemaId = targetSchemaId;
            this.sourceTabletId = sourceTabletId;
            this.targetTabletId = targetTabletId;
        }

        public long getTargetSchemaId() {
            return targetSchemaId;
        }

        public long getSourceTabletId() {
            return sourceTabletId;
        }

        public long getTargetTabletId() {
            return targetTabletId;
        }
    }
}
