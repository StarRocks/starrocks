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
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.thrift.TClusterSnapshotsItem;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ClusterSnapshot implements Writable {
    public enum ClusterSnapshotType { AUTOMATED }

    @SerializedName(value = "snapshotId")
    private long snapshotId;
    @SerializedName(value = "snapshotName")
    private String snapshotName;
    @SerializedName(value = "type")
    private ClusterSnapshotType type;
    @SerializedName(value = "storageVolumeName")
    private String storageVolumeName;
    @SerializedName(value = "createTime")
    private long createTime;
    @SerializedName(value = "successTime")
    private long successTime;
    @SerializedName(value = "feJournalId")
    private long feJournalId;
    @SerializedName(value = "starMgrJournal")
    private long starMgrJournalId;

    public ClusterSnapshot() {}

    public ClusterSnapshot(long snapshotId, String snapshotName, String storageVolumeName, long createTime,
                           long successTime, long feJournalId, long starMgrJournalId) {
        this.snapshotId = snapshotId;
        this.snapshotName = snapshotName;
        this.type = ClusterSnapshotType.AUTOMATED;
        this.storageVolumeName = storageVolumeName;
        this.createTime = createTime;
        this.successTime = successTime;
        this.feJournalId = feJournalId;
        this.starMgrJournalId = starMgrJournalId;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public TClusterSnapshotsItem getInfo() {
        TClusterSnapshotsItem item = new TClusterSnapshotsItem();
        item.setSnapshot_name(snapshotName);
        item.setSnapshot_type(type.name());
        item.setCreated_time(createTime);
        item.setFinished_time(successTime);
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
            item.setStorage_path(sv.getLocations().get(0));
        } catch (Exception e) {
            item.setStorage_path("");
        }
        return item;
    }

    public static ClusterSnapshot read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ClusterSnapshot.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
