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

package com.starrocks.lake.snapshot;

import com.google.gson.annotations.SerializedName;

public class ManualClusterSnapshot extends ClusterSnapshot {
    @SerializedName(value = "clusterSnapshotInfo")
    private ClusterSnapshotInfo clusterSnapshotInfo;

    public ManualClusterSnapshot() {
    }

    public ManualClusterSnapshot(long id, String snapshotName, ClusterSnapshotType type, String storageVolumeName,
                                 long createdTimeMs, long finishedTimeMs, long feJournalId, long starMgrJournalId) {
        super(id, snapshotName, type, storageVolumeName, createdTimeMs, finishedTimeMs, feJournalId, starMgrJournalId);
    }

    public ClusterSnapshotInfo getClusterSnapshotInfo() {
        return this.clusterSnapshotInfo;
    }

    @Override
    public void setClusterSnapshotInfo(ClusterSnapshotInfo clusterSnapshotInfo) {
        this.clusterSnapshotInfo = clusterSnapshotInfo;
    }
}