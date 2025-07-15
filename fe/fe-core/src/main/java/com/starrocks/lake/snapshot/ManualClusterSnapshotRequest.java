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
import com.starrocks.server.GlobalStateMgr;

/*
 * This class is used to save the user request into the queue and
 * transform into a actual manual cluster snapshot job when it is executed.
*/
public class ManualClusterSnapshotRequest {
    @SerializedName(value = "snapshotName")
    private String snapshotName;
    @SerializedName(value = "storageVolumeName")
    private String storageVolumeName;

    public ManualClusterSnapshotRequest(String snapshotName, String storageVolumeName) {
        this.snapshotName = snapshotName;
        this.storageVolumeName = storageVolumeName;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getStorageVolumeName() {
        return storageVolumeName;
    }

    public ManualClusterSnapshotJob toManualClusterSnapshotJob() {
        long id = GlobalStateMgr.getCurrentState().getNextId();
        long createdTimeMs = System.currentTimeMillis();
        ManualClusterSnapshotJob job = new ManualClusterSnapshotJob(id, snapshotName, storageVolumeName, createdTimeMs);
        return job;
    }
}
