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

import com.google.common.collect.Lists;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterSnapshotInfoTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testCommittedVersionsAfterVisible() {
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
            public List<Database> getAllDbs() {
                return Lists.newArrayList(dbTest);
            }
        };

        ClusterSnapshotInfo clusterSnapshotInfo =
                SnapshotInfoHelper.buildClusterSnapshotInfo(
                        GlobalStateMgr.getCurrentState().getLocalMetastore().getAllDbs());
        Assertions.assertTrue(!clusterSnapshotInfo.isEmpty());

        for (Table tbl : dbTest.getTables()) {
            OlapTable olapTable = (OlapTable) tbl;
            for (PhysicalPartition part : olapTable.getPhysicalPartitions()) {
                long dbId = dbTest.getId();
                long tableId = olapTable.getId();
                long parentPartId = part.getParentId();
                long physicalPartId = part.getId();

                List<Long> committedAfterVisible = clusterSnapshotInfo.getCommittedVersionsAfterVisible(
                        dbId, tableId, parentPartId, physicalPartId);

                long visibleVersion = part.getVisibleVersion();
                long committedVersion = part.getCommittedVersion();
                long expectedSize = Math.max(0L, committedVersion - visibleVersion);

                Assertions.assertEquals(expectedSize, committedAfterVisible.size());
                if (expectedSize > 0) {
                    Assertions.assertEquals(visibleVersion + 1, committedAfterVisible.get(0).longValue());
                    Assertions.assertEquals(committedVersion,
                            committedAfterVisible.get(committedAfterVisible.size() - 1).longValue());
                }
            }
        }
    }

    @Test
    public void testCommittedVersionsAfterVisibleWithInvalidIds() {
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
            public List<Database> getAllDbs() {
                return Lists.newArrayList(dbTest);
            }
        };

        ClusterSnapshotInfo clusterSnapshotInfo =
                SnapshotInfoHelper.buildClusterSnapshotInfo(
                        GlobalStateMgr.getCurrentState().getLocalMetastore().getAllDbs());
        Assertions.assertTrue(!clusterSnapshotInfo.isEmpty());

        // Non-existent identifiers should return an empty list
        List<Long> res = clusterSnapshotInfo.getCommittedVersionsAfterVisible(0L, 0L, 0L, 0L);
        Assertions.assertTrue(res.isEmpty());
    }

    @Test
    public void testCommittedVersionsAfterVisibleWithManualBump() {
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
            public List<Database> getAllDbs() {
                return Lists.newArrayList(dbTest);
            }
        };

        // Pick the first available physical partition and manually bump nextVersion
        PhysicalPartition targetPart = null;
        OlapTable targetTable = null;
        outer:
        for (Table tbl : dbTest.getTables()) {
            OlapTable olapTable = (OlapTable) tbl;
            for (PhysicalPartition part : olapTable.getPhysicalPartitions()) {
                targetPart = part;
                targetTable = olapTable;
                break outer;
            }
        }
        Assertions.assertNotNull(targetPart);
        Assertions.assertNotNull(targetTable);

        long originalNext = targetPart.getNextVersion();
        long visible = targetPart.getVisibleVersion();
        long bump = 3L; // expect versions (visible+1 .. visible+3)
        try {
            targetPart.setNextVersion(visible + bump + 1);

            ClusterSnapshotInfo clusterSnapshotInfo =
                    SnapshotInfoHelper.buildClusterSnapshotInfo(
                            GlobalStateMgr.getCurrentState().getLocalMetastore().getAllDbs());

            List<Long> committedAfterVisible = clusterSnapshotInfo.getCommittedVersionsAfterVisible(
                    dbTest.getId(), targetTable.getId(), targetPart.getParentId(), targetPart.getId());

            Assertions.assertEquals(bump, committedAfterVisible.size());
            Assertions.assertEquals(visible + 1, committedAfterVisible.get(0).longValue());
            Assertions.assertEquals(visible + bump, committedAfterVisible.get((int) bump - 1).longValue());
        } finally {
            // restore state
            targetPart.setNextVersion(originalNext);
        }
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
            public List<Database> getAllDbs() {
                return Lists.newArrayList(dbTest);
            }
        };

        ClusterSnapshotInfo clusterSnapshotInfo = null;
        Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
        {
            clusterSnapshotInfo = new ClusterSnapshotInfo(dbInfos);
            Assertions.assertTrue(clusterSnapshotInfo.isEmpty());
            clusterSnapshotInfo =
                SnapshotInfoHelper.buildClusterSnapshotInfo(GlobalStateMgr.getCurrentState().getLocalMetastore().getAllDbs());
            Assertions.assertTrue(!clusterSnapshotInfo.isEmpty());
        }
        for (Table tbl : dbTest.getTables()) {
            OlapTable olapTable = (OlapTable) tbl;
            for (PhysicalPartition part : olapTable.getPhysicalPartitions()) {
                long value = clusterSnapshotInfo.getVersion(dbTest.getId(), olapTable.getId(), part.getParentId(), part.getId());
                Assertions.assertTrue(value != 0 && value == part.getVisibleVersion());

                Assertions.assertTrue(clusterSnapshotInfo.containsDb(dbTest.getId()));
                Assertions.assertTrue(clusterSnapshotInfo.containsTable(dbTest.getId(), olapTable.getId()));
                Assertions.assertTrue(clusterSnapshotInfo.containsPartition(dbTest.getId(), olapTable.getId(),
                                                                        part.getParentId()));

                for (MaterializedIndex index : part.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    Assertions.assertTrue(clusterSnapshotInfo.containsMaterializedIndex(dbTest.getId(), olapTable.getId(),
                                                                                    part.getParentId(), part.getId(),
                                                                                    index.getId()));
                    Assertions.assertTrue(!clusterSnapshotInfo.containsMaterializedIndex(dbTest.getId(), olapTable.getId(),
                                                                                     part.getParentId(), part.getId(),
                                                                                     index.getId() + 1L));

                    Assertions.assertTrue(clusterSnapshotInfo.containsShardGroupId(dbTest.getId(), olapTable.getId(),
                                                                                   part.getParentId(), index.getShardGroupId()));
                    Assertions.assertTrue(clusterSnapshotInfo.containsShardGroupId(dbTest.getId(), olapTable.getId(),
                                                                                   part.getParentId(), part.getId(),
                                                                                   index.getShardGroupId()));
                    Assertions.assertTrue(!clusterSnapshotInfo.containsShardGroupId(dbTest.getId(), olapTable.getId(),
                                                                                    part.getParentId() + 666L,
                                                                                    index.getShardGroupId()));
                    Assertions.assertTrue(!clusterSnapshotInfo.containsShardGroupId(dbTest.getId(), olapTable.getId(),
                                                                                    part.getParentId(), part.getId() + 666L,
                                                                                    index.getShardGroupId()));
                    Assertions.assertTrue(!clusterSnapshotInfo.containsShardGroupId(dbTest.getId(), olapTable.getId(),
                                                                                    part.getParentId(), part.getId(),
                                                                                    index.getShardGroupId() + 666L));
                    Assertions.assertTrue(!clusterSnapshotInfo.containsShardGroupId(dbTest.getId(), olapTable.getId(),
                                                                                    part.getParentId(),
                                                                                    index.getShardGroupId() + 666L));
                }
            }
        }
        Assertions.assertTrue(!clusterSnapshotInfo.containsDb(0L));
        Assertions.assertTrue(!clusterSnapshotInfo.containsTable(0L, 1L));
        Assertions.assertTrue(!clusterSnapshotInfo.containsPartition(0L, 1L, 2L));
        Assertions.assertTrue(!clusterSnapshotInfo.containsMaterializedIndex(0L, 1L, 2L, 3L, 4L));
        clusterSnapshotInfo = null;
    }
}
