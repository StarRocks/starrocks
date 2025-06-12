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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class ClusterSnapshotInfoTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testClusterSnapshotInfoBasic() {
        // 1. test get version info function
        long testDbId = 0;
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db != null && !db.isSystemDatabase()) {
                testDbId = dbId;
                break;
            }
        }
        Database sourceDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        final Database dbTest = new Database(sourceDb.getId(), sourceDb.getFullName());
        for (Table tbl : sourceDb.getTables()) {
            if (tbl.isOlapTable()) {
                dbTest.registerTableUnlocked(tbl);
            }
        }

        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
        };

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(long dbId) {
                if (dbId == dbTest.getId()) {
                    return dbTest;
                } else {
                    return null;
                }
            }
        };

        ClusterSnapshotInfo clusterSnapshotInfo = null;
        final LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        {
            clusterSnapshotInfo = new ClusterSnapshotInfo();
            Assert.assertTrue(clusterSnapshotInfo.isEmpty());
            new MockUp<GlobalStateMgr>() {
                @Mock
                public static boolean isCheckpointThread() {
                    return true;
                }
                @Mock
                public LocalMetastore getLocalMetastore() {
                    return localMetastore;
                }
            };
            clusterSnapshotInfo = new ClusterSnapshotInfo();
            Assert.assertTrue(!clusterSnapshotInfo.isEmpty());
        }
        for (Table tbl : dbTest.getTables()) {
            OlapTable olapTable = (OlapTable) tbl;
            for (PhysicalPartition part : olapTable.getPhysicalPartitions()) {
                long value = clusterSnapshotInfo.getVersion(dbTest.getId(), olapTable.getId(), part.getParentId(), part.getId());
                Assert.assertTrue(value != 0 && value == part.getVisibleVersion());

                Assert.assertTrue(clusterSnapshotInfo.isDbExisted(dbTest.getId()));
                Assert.assertTrue(clusterSnapshotInfo.isTableExisted(dbTest.getId(), olapTable.getId()));
                Assert.assertTrue(clusterSnapshotInfo.isPartitionExisted(dbTest.getId(), olapTable.getId(),
                                                                           part.getParentId()));

                for (MaterializedIndex index : part.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    Assert.assertTrue(clusterSnapshotInfo.isMaterializedIndexExisted(dbTest.getId(), olapTable.getId(),
                                                                                     part.getParentId(), part.getId(),
                                                                                     index.getId()));
                    Assert.assertTrue(!clusterSnapshotInfo.isMaterializedIndexExisted(dbTest.getId(), olapTable.getId(),
                                                                                      part.getParentId(), part.getId(),
                                                                                      index.getId() + 1L));
                }
            }
        }
        Assert.assertTrue(!clusterSnapshotInfo.isDbExisted(0L));
        Assert.assertTrue(!clusterSnapshotInfo.isTableExisted(0L, 1L));
        Assert.assertTrue(!clusterSnapshotInfo.isPartitionExisted(0L, 1L, 2L));
        Assert.assertTrue(!clusterSnapshotInfo.isMaterializedIndexExisted(0L, 1L, 2L, 3L, 4L));
        clusterSnapshotInfo = null;
    }
}
