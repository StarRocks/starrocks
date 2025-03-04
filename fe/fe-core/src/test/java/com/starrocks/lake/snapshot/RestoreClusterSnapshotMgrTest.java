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

import com.starrocks.common.StarRocksException;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.persist.Storage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class RestoreClusterSnapshotMgrTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Test
    public void testDownloadSnapshotFailed() throws Exception {
        Assert.assertThrows(StarRocksException.class, () -> {
            RestoreClusterSnapshotMgr.init("src/test/resources/conf/cluster_snapshot.yaml",
                    new String[] { "-cluster_snapshot" });
        });

        Assert.assertFalse(RestoreClusterSnapshotMgr.isRestoring());
        RestoreClusterSnapshotMgr.finishRestoring();
        Assert.assertFalse(RestoreClusterSnapshotMgr.isRestoring());
    }

    @Test
    public void testNormal() throws Exception {
        new MockUp<Storage>() {
            @Mock
            public long getImageJournalId() {
                return 10L;
            }
        };

        new MockUp<HdfsFsManager>() {
            @Mock
            public void copyToLocal(String srcPath, String destPath, Map<String, String> properties) {
                return;
            } // IOException
        };

        RestoreClusterSnapshotMgr.init("src/test/resources/conf/cluster_snapshot.yaml",
                new String[] { "-cluster_snapshot" });
        Assert.assertTrue(RestoreClusterSnapshotMgr.getRestoredSnapshotInfo().getSnapshotName()
                .equals("automated_cluster_snapshot_1704038400000"));
        Assert.assertTrue(RestoreClusterSnapshotMgr.getRestoredSnapshotInfo().getFeJournalId() == 10L);
        Assert.assertTrue(RestoreClusterSnapshotMgr.getRestoredSnapshotInfo().getStarMgrJournalId() == 10L);
        Assert.assertTrue(RestoreClusterSnapshotMgr.isRestoring());

        for (ClusterSnapshotConfig.StorageVolume sv : RestoreClusterSnapshotMgr.getConfig().getStorageVolumes()) {
            GlobalStateMgr.getCurrentState().getStorageVolumeMgr().createStorageVolume(sv.getName(), sv.getType(),
                    Collections.singletonList(sv.getLocation()), sv.getProperties(), Optional.of(true),
                    sv.getComment());
        }

        RestoreClusterSnapshotMgr.finishRestoring();
        Assert.assertFalse(RestoreClusterSnapshotMgr.isRestoring());
    }
}
