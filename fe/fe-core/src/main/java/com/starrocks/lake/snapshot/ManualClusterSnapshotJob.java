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

import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.epack.persist.ManualClusterSnapshotLog;
import com.starrocks.server.GlobalStateMgr;

public class ManualClusterSnapshotJob extends ClusterSnapshotJob {
    public ManualClusterSnapshotJob(long id, String snapshotName, String storageVolumeName, long createdTimeMs) {
        super(id, snapshotName, storageVolumeName, createdTimeMs);
    }

    public ManualClusterSnapshotJob(ClusterSnapshotJob other) {
        super(other);
    }

    @Override
    protected ClusterSnapshot createClusterSnapshot(long id, String snapshotName, String storageVolumeName, long createdTimeMs) {
        return new ManualClusterSnapshot(id, snapshotName, ClusterSnapshot.ClusterSnapshotType.MANUAL,
                    storageVolumeName, createdTimeMs, -1, 0, 0);
    }

    @Override
    public boolean needClusterSnapshotInfo() {
        return true;
    }

    @Override
    public ClusterSnapshotJob copyForPersist() {
        return new ManualClusterSnapshotJob(this);
    }

    @Override
    public void persistStateChange(ClusterSnapshotJobState newState) {
        ManualClusterSnapshotJob persistJob = (ManualClusterSnapshotJob) this.copyForPersist();
        persistJob.setState(newState);
        ManualClusterSnapshotLog log = new ManualClusterSnapshotLog();
        log.setSnapshotJob(persistJob);
        EditLogEPack editLogEPack = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLogEPack.logManualClusterSnapshotLog(log, wal -> {
            this.setState(newState);
        });
    }
}