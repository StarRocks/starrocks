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
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.thrift.TClusterSnapshotsItem;

public class ClusterSnapshot {
    public enum ClusterSnapshotType {
        AUTOMATED, MANUAL, INCREMENTAL
    }

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "snapshotName")
    private String snapshotName;
    @SerializedName(value = "type")
    private ClusterSnapshotType type;
    @SerializedName(value = "storageVolumeName")
    private String storageVolumeName;
    @SerializedName(value = "createdTimeMs")
    private long createdTimeMs;
    @SerializedName(value = "finishedTimeMs")
    private long finishedTimeMs;
    @SerializedName(value = "feJournalId")
    private long feJournalId;
    @SerializedName(value = "starMgrJournal")
    private long starMgrJournalId;

    public ClusterSnapshot() {
    }

    public ClusterSnapshot(long id, String snapshotName, String storageVolumeName, long createdTimeMs,
            long finishedTimeMs, long feJournalId, long starMgrJournalId) {
        this.id = id;
        this.snapshotName = snapshotName;
        this.type = ClusterSnapshotType.AUTOMATED;
        this.storageVolumeName = storageVolumeName;
        this.createdTimeMs = createdTimeMs;
        this.finishedTimeMs = finishedTimeMs;
        this.feJournalId = feJournalId;
        this.starMgrJournalId = starMgrJournalId;
    }

    public void setJournalIds(long feJournalId, long starMgrJournalId) {
        this.feJournalId = feJournalId;
        this.starMgrJournalId = starMgrJournalId;
    }

    public void setFinishedTimeMs(long finishedTimeMs) {
        this.finishedTimeMs = finishedTimeMs;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getStorageVolumeName() {
        return storageVolumeName;
    }

    public long getCreatedTimeMs() {
        return createdTimeMs;
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }

    public long getFeJournalId() {
        return feJournalId;
    }

    public long getStarMgrJournalId() {
        return starMgrJournalId;
    }

    public long getId() {
        return id;
    }

    public TClusterSnapshotsItem getInfo() {
        TClusterSnapshotsItem item = new TClusterSnapshotsItem();
        item.setSnapshot_name(snapshotName);
        item.setSnapshot_type(type.name());
        item.setCreated_time(createdTimeMs / 1000);
        item.setFe_jouranl_id(feJournalId);
        item.setStarmgr_jouranl_id(starMgrJournalId);
        item.setProperties("");
        item.setStorage_volume(storageVolumeName);

        StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        try {
            StorageVolume sv = storageVolumeMgr.getStorageVolumeByName(storageVolumeName);
            if (sv == null) {
                throw new Exception("Unknown storage volume: " + storageVolumeName);
            }
            item.setStorage_path(ClusterSnapshotUtils.getSnapshotImagePath(sv, snapshotName));
        } catch (Exception e) {
            item.setStorage_path("");
        }
        return item;
    }
}
