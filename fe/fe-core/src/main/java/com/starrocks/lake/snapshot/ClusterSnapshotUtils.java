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

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.storagevolume.StorageVolume;

public class ClusterSnapshotUtils {
    public static String getSnapshotImagePath(StorageVolume sv, String snapshotName) {
        return String.join("/", sv.getLocations().get(0),
                GlobalStateMgr.getCurrentState().getStarOSAgent().getRawServiceId(), "meta/image", snapshotName);
    }

    public static void uploadAutomatedSnapshotToRemote(String snapshotName) throws StarRocksException {
        if (snapshotName == null || snapshotName.isEmpty()) {
            return;
        }

        StorageVolume sv = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAutomatedSnapshotStorageVolume();
        String snapshotImagePath = getSnapshotImagePath(sv, snapshotName);
        String localImagePath = GlobalStateMgr.getServingState().getImageDir();

        HdfsUtil.copyFromLocal(localImagePath, snapshotImagePath, sv.getProperties());
    }

    public static void clearAutomatedSnapshotFromRemote(String snapshotName) throws StarRocksException {
        if (snapshotName == null || snapshotName.isEmpty()) {
            return;
        }

        StorageVolume sv = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAutomatedSnapshotStorageVolume();
        BrokerDesc brokerDesc = new BrokerDesc(sv.getProperties());
        String snapshotImagePath = getSnapshotImagePath(sv, snapshotName);

        HdfsUtil.deletePath(snapshotImagePath, brokerDesc);
    }
}
