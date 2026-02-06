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

import com.starrocks.persist.ClusterSnapshotRestoredVersionLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ClusterSnapshotRestoredVersionMgrTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private EditLog editLog;

    private ClusterSnapshotRestoredVersionMgr mgr;

    @BeforeEach
    public void setUp() {
        mgr = new ClusterSnapshotRestoredVersionMgr();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };

        new Expectations() {
            {
                globalStateMgr.getEditLog();
                result = editLog;
                minTimes = 0;
            }
        };
    }

    @Test
    public void testGetRestoredCommittedVersions() {
        Map<Long, Map<Long, Map<Long, Long>>> versions = mgr.getRestoredCommittedVersions();
        Assertions.assertNotNull(versions);
        Assertions.assertTrue(versions.isEmpty());
    }

    @Test
    public void testUpdateRestoredCommittedVersions() {
        Map<Long, Map<Long, Map<Long, Long>>> newVersions = new HashMap<>();
        Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
        Map<Long, Long> partMap = new HashMap<>();
        partMap.put(300L, 10L);
        tableMap.put(200L, partMap);
        newVersions.put(100L, tableMap);

        mgr.updateRestoredCommittedVersions(newVersions);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(10L, result.get(100L).get(200L).get(300L).longValue());
    }

    @Test
    public void testUpdateRestoredCommittedVersionsWithNull() {
        mgr.updateRestoredCommittedVersions(null);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testIsRestoreVersion() {
        Map<Long, Map<Long, Map<Long, Long>>> versions = new HashMap<>();
        Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
        Map<Long, Long> partMap = new HashMap<>();
        partMap.put(300L, 10L);
        tableMap.put(200L, partMap);
        versions.put(100L, tableMap);

        mgr.updateRestoredCommittedVersions(versions);

        // Test matching version
        Assertions.assertTrue(mgr.isRestoreVersion(100L, 200L, 300L, 10L));
        Assertions.assertTrue(mgr.isRestoreVersion(100L, 200L, 300L, 9L));

        // Test non-matching version
        Assertions.assertFalse(mgr.isRestoreVersion(100L, 200L, 300L, 11L));

        // Test non-existent entries
        Assertions.assertFalse(mgr.isRestoreVersion(999L, 200L, 300L, 10L));
        Assertions.assertFalse(mgr.isRestoreVersion(100L, 999L, 300L, 10L));
        Assertions.assertFalse(mgr.isRestoreVersion(100L, 200L, 999L, 10L));
    }

    @Test
    public void testIsRestoreVersionWithEmptyMap() {
        Assertions.assertFalse(mgr.isRestoreVersion(100L, 200L, 300L, 10L));
    }

    @Test
    public void testWriteLogWithEmptyMap() {
        new MockUp<EditLog>() {
            @Mock
            public void logClusterSnapshotRestoredVersion(ClusterSnapshotRestoredVersionLog log, WALApplier walApplier) {
                if (walApplier != null) {
                    walApplier.apply(log);
                }
            }
        };

        // This is the key test for the fix: empty map should still be persisted
        mgr.writeLog(Collections.emptyMap());
    }

    @Test
    public void testWriteLogWithNonEmptyMap() {
        Map<Long, Map<Long, Map<Long, Long>>> versions = new HashMap<>();
        Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
        Map<Long, Long> partMap = new HashMap<>();
        partMap.put(300L, 10L);
        tableMap.put(200L, partMap);
        versions.put(100L, tableMap);

        new MockUp<EditLog>() {
            @Mock
            public void logClusterSnapshotRestoredVersion(ClusterSnapshotRestoredVersionLog log, WALApplier walApplier) {
                if (walApplier != null) {
                    walApplier.apply(log);
                }
            }
        };
        
        mgr.writeLog(versions);
    }

    @Test
    public void testReplayLogWithNull() {
        mgr.replayLog(null);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testReplayLogWithEmptyMap() {
        ClusterSnapshotRestoredVersionLog log = new ClusterSnapshotRestoredVersionLog(Collections.emptyMap());
        mgr.replayLog(log);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testReplayLogWithData() {
        Map<Long, Map<Long, Map<Long, Long>>> versions = new HashMap<>();
        Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
        Map<Long, Long> partMap = new HashMap<>();
        partMap.put(300L, 10L);
        tableMap.put(200L, partMap);
        versions.put(100L, tableMap);

        ClusterSnapshotRestoredVersionLog log = new ClusterSnapshotRestoredVersionLog(versions);
        mgr.replayLog(log);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(10L, result.get(100L).get(200L).get(300L).longValue());
    }

    @Test
    public void testInitWithNull() {
        new Expectations() {
            {
                editLog.logClusterSnapshotRestoredVersion((ClusterSnapshotRestoredVersionLog) any, (WALApplier) any);
                times = 0;
            }
        };

        mgr.init(null);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testInitWithEmptyDbInfos() {
        ClusterSnapshotInfo snapshotInfo = new ClusterSnapshotInfo(Collections.emptyMap());

        new Expectations() {
            {
                editLog.logClusterSnapshotRestoredVersion((ClusterSnapshotRestoredVersionLog) any, (WALApplier) any);
                times = 0; // init returns early when dbInfos is empty, so no log is written
            }
        };

        mgr.init(snapshotInfo);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testInitWithCommittedVersionGreaterThanVisibleVersion() {
        long dbId = 100L;
        long tableId = 200L;
        long partitionId = 1L;
        long physicalPartitionId = 300L;
        long visibleVersion = 5L;
        long committedVersion = 10L;

        Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos = new HashMap<>();
        physicalPartInfos.put(physicalPartitionId,
                new PhysicalPartitionSnapshotInfo(physicalPartitionId, visibleVersion, committedVersion, 0L, null));

        Map<Long, PartitionSnapshotInfo> partInfos = new HashMap<>();
        partInfos.put(partitionId, new PartitionSnapshotInfo(partitionId, physicalPartInfos));

        Map<Long, TableSnapshotInfo> tableInfos = new HashMap<>();
        tableInfos.put(tableId, new TableSnapshotInfo(tableId, false, partInfos));

        Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
        dbInfos.put(dbId, new DatabaseSnapshotInfo(dbId, tableInfos));

        ClusterSnapshotInfo snapshotInfo = new ClusterSnapshotInfo(dbInfos);

        new MockUp<EditLog>() {
            @Mock
            public void logClusterSnapshotRestoredVersion(ClusterSnapshotRestoredVersionLog log, WALApplier walApplier) {
                if (walApplier != null) {
                    walApplier.apply(log);
                }
            }
        };

        mgr.init(snapshotInfo);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(committedVersion, result.get(dbId).get(tableId).get(physicalPartitionId).longValue());
    }

    @Test
    public void testInitWithCommittedVersionEqualToVisibleVersion() {
        long dbId = 100L;
        long tableId = 200L;
        long partitionId = 1L;
        long physicalPartitionId = 300L;
        long visibleVersion = 10L;
        long committedVersion = 10L; // Equal, should not be added

        Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos = new HashMap<>();
        physicalPartInfos.put(physicalPartitionId,
                new PhysicalPartitionSnapshotInfo(physicalPartitionId, visibleVersion, committedVersion, 0L, null));

        Map<Long, PartitionSnapshotInfo> partInfos = new HashMap<>();
        partInfos.put(partitionId, new PartitionSnapshotInfo(partitionId, physicalPartInfos));

        Map<Long, TableSnapshotInfo> tableInfos = new HashMap<>();
        tableInfos.put(tableId, new TableSnapshotInfo(tableId, false, partInfos));

        Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
        dbInfos.put(dbId, new DatabaseSnapshotInfo(dbId, tableInfos));

        ClusterSnapshotInfo snapshotInfo = new ClusterSnapshotInfo(dbInfos);

        new MockUp<EditLog>() {
            @Mock
            public void logClusterSnapshotRestoredVersion(ClusterSnapshotRestoredVersionLog log, WALApplier walApplier) {
                if (walApplier != null) {
                    walApplier.apply(log);
                }
            }
        };

        mgr.init(snapshotInfo);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testInitWithCommittedVersionLessThanVisibleVersion() {
        long dbId = 100L;
        long tableId = 200L;
        long partitionId = 1L;
        long physicalPartitionId = 300L;
        long visibleVersion = 10L;
        long committedVersion = 5L; // Less than visible, should not be added

        Map<Long, PhysicalPartitionSnapshotInfo> physicalPartInfos = new HashMap<>();
        physicalPartInfos.put(physicalPartitionId,
                new PhysicalPartitionSnapshotInfo(physicalPartitionId, visibleVersion, committedVersion, 0L, null));

        Map<Long, PartitionSnapshotInfo> partInfos = new HashMap<>();
        partInfos.put(partitionId, new PartitionSnapshotInfo(partitionId, physicalPartInfos));

        Map<Long, TableSnapshotInfo> tableInfos = new HashMap<>();
        tableInfos.put(tableId, new TableSnapshotInfo(tableId, false, partInfos));

        Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
        dbInfos.put(dbId, new DatabaseSnapshotInfo(dbId, tableInfos));

        ClusterSnapshotInfo snapshotInfo = new ClusterSnapshotInfo(dbInfos);

        new MockUp<EditLog>() {
            @Mock
            public void logClusterSnapshotRestoredVersion(ClusterSnapshotRestoredVersionLog log, WALApplier walApplier) {
                if (walApplier != null) {
                    walApplier.apply(log);
                }
            }
        };

        mgr.init(snapshotInfo);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testInitWithNullTableInfos() {
        long dbId = 100L;

        Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
        dbInfos.put(dbId, new DatabaseSnapshotInfo(dbId, null));
        ClusterSnapshotInfo snapshotInfo = new ClusterSnapshotInfo(dbInfos);

        new MockUp<EditLog>() {
            @Mock
            public void logClusterSnapshotRestoredVersion(ClusterSnapshotRestoredVersionLog log, WALApplier walApplier) {
                if (walApplier != null) {
                    walApplier.apply(log);
                }
            }
        };

        mgr.init(snapshotInfo);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testInitWithNullPartInfos() {
        long dbId = 100L;
        long tableId = 200L;

        Map<Long, TableSnapshotInfo> tableInfos = new HashMap<>();
        tableInfos.put(tableId, new TableSnapshotInfo(tableId, false, null));
        Map<Long, DatabaseSnapshotInfo> dbInfos = new HashMap<>();
        dbInfos.put(dbId, new DatabaseSnapshotInfo(dbId, tableInfos));
        ClusterSnapshotInfo snapshotInfo = new ClusterSnapshotInfo(dbInfos);

        new MockUp<EditLog>() {
            @Mock
            public void logClusterSnapshotRestoredVersion(ClusterSnapshotRestoredVersionLog log, WALApplier walApplier) {
                if (walApplier != null) {
                    walApplier.apply(log);
                }
            }
        };

        mgr.init(snapshotInfo);
        Map<Long, Map<Long, Map<Long, Long>>> result = mgr.getRestoredCommittedVersions();
        Assertions.assertTrue(result.isEmpty());
    }
}