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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ClusterSnapshotRestoredVersionCleanerTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private LocalMetastore localMetastore;

    @Mocked
    private ClusterSnapshotRestoredVersionMgr restoreMgr;

    private ClusterSnapshotRestoredVersionCleaner cleaner;

    @BeforeEach
    public void setUp() {
        cleaner = new ClusterSnapshotRestoredVersionCleaner(1000L);
    }

    @Test
    public void testEmptyRestoredVersions() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                times = 1;

                globalStateMgr.isLeader();
                result = true;
                times = 1;

                globalStateMgr.getClusterSnapshotRestoredVersionMgr();
                result = restoreMgr;
                times = 1;

                restoreMgr.getRestoredCommittedVersions();
                result = Collections.emptyMap();
                times = 1;
            }
        };

        // Should return early when map is empty
        Deencapsulation.invoke(cleaner, "cleanObsoleteEntriesInternal");
    }


    @Test
    public void testDatabaseDropped() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        long dbId = 100L;
        long tableId = 200L;
        long physicalPartitionId = 300L;
        long recordedVersion = 10L;

        Map<Long, Long> partMap = new HashMap<>();
        partMap.put(physicalPartitionId, recordedVersion);

        Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
        tableMap.put(tableId, partMap);

        Map<Long, Map<Long, Map<Long, Long>>> restoredVersions = new HashMap<>();
        restoredVersions.put(dbId, tableMap);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 1;

                globalStateMgr.isLeader();
                result = true;
                times = 1;

                globalStateMgr.getClusterSnapshotRestoredVersionMgr();
                result = restoreMgr;
                minTimes = 1;

                restoreMgr.getRestoredCommittedVersions();
                result = restoredVersions;
                times = 1;

                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                times = 1;

                localMetastore.getDb(dbId);
                result = null; // Database dropped
                times = 1;

                restoreMgr.writeLog((Map) any);
                times = 1;
            }
        };

        Deencapsulation.invoke(cleaner, "cleanObsoleteEntriesInternal");
    }

    @Test
    public void testTableDropped() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        long dbId = 100L;
        long tableId = 200L;
        long physicalPartitionId = 300L;
        long recordedVersion = 10L;

        Map<Long, Long> partMap = new HashMap<>();
        partMap.put(physicalPartitionId, recordedVersion);

        Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
        tableMap.put(tableId, partMap);

        Map<Long, Map<Long, Map<Long, Long>>> restoredVersions = new HashMap<>();
        restoredVersions.put(dbId, tableMap);

        Database db = new Database(dbId, "test_db");

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 1;

                globalStateMgr.isLeader();
                result = true;
                times = 1;

                globalStateMgr.getClusterSnapshotRestoredVersionMgr();
                result = restoreMgr;
                minTimes = 1;

                restoreMgr.getRestoredCommittedVersions();
                result = restoredVersions;
                times = 1;

                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                times = 2;

                localMetastore.getDb(dbId);
                result = db;
                times = 1;

                localMetastore.getTable(dbId, tableId);
                result = null; // Table dropped
                times = 1;

                restoreMgr.writeLog((Map) any);
                times = 1;
            }
        };

        Deencapsulation.invoke(cleaner, "cleanObsoleteEntriesInternal");
    }

    @Test
    public void testPhysicalPartitionDropped() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        long dbId = 100L;
        long tableId = 200L;
        long physicalPartitionId = 300L;
        long recordedVersion = 10L;

        Map<Long, Long> partMap = new HashMap<>();
        partMap.put(physicalPartitionId, recordedVersion);

        Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
        tableMap.put(tableId, partMap);

        Map<Long, Map<Long, Map<Long, Long>>> restoredVersions = new HashMap<>();
        restoredVersions.put(dbId, tableMap);

        Database db = new Database(dbId, "test_db");
        OlapTable table = new OlapTable();

        new Expectations(table) {
            {
                table.getPhysicalPartition(physicalPartitionId);
                result = null; // Physical partition dropped
                times = 1;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 1;

                globalStateMgr.isLeader();
                result = true;
                times = 1;

                globalStateMgr.getClusterSnapshotRestoredVersionMgr();
                result = restoreMgr;
                minTimes = 1;

                restoreMgr.getRestoredCommittedVersions();
                result = restoredVersions;
                times = 1;

                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                times = 2;

                localMetastore.getDb(dbId);
                result = db;
                times = 1;

                localMetastore.getTable(dbId, tableId);
                result = table;
                times = 1;

                restoreMgr.writeLog((Map) any);
                times = 1;
            }
        };

        Deencapsulation.invoke(cleaner, "cleanObsoleteEntriesInternal");
    }

    @Test
    public void testPartitionVersionCaughtUp() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        long dbId = 100L;
        long tableId = 200L;
        long physicalPartitionId = 300L;
        long recordedVersion = 10L;
        long visibleVersion = 15L; // Visible version > recorded version

        Map<Long, Long> partMap = new HashMap<>();
        partMap.put(physicalPartitionId, recordedVersion);

        Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
        tableMap.put(tableId, partMap);

        Map<Long, Map<Long, Map<Long, Long>>> restoredVersions = new HashMap<>();
        restoredVersions.put(dbId, tableMap);

        Database db = new Database(dbId, "test_db");
        OlapTable table = new OlapTable();
        PhysicalPartition physicalPartition = Deencapsulation.newInstance(PhysicalPartition.class);
        Deencapsulation.setField(physicalPartition, "visibleVersion", visibleVersion);

        new Expectations(table) {
            {
                table.getPhysicalPartition(physicalPartitionId);
                result = physicalPartition;
                times = 1;
            }
        };

        new Expectations(physicalPartition) {
            {
                physicalPartition.getVisibleVersion();
                result = visibleVersion;
                times = 1;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 1;

                globalStateMgr.isLeader();
                result = true;
                times = 1;

                globalStateMgr.getClusterSnapshotRestoredVersionMgr();
                result = restoreMgr;
                minTimes = 1;

                restoreMgr.getRestoredCommittedVersions();
                result = restoredVersions;
                times = 1;

                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                times = 2;

                localMetastore.getDb(dbId);
                result = db;
                times = 1;

                localMetastore.getTable(dbId, tableId);
                result = table;
                times = 1;

                restoreMgr.writeLog((Map) any);
                times = 1;
            }
        };

        Deencapsulation.invoke(cleaner, "cleanObsoleteEntriesInternal");
    }

    @Test
    public void testPartitionVersionNotCaughtUp() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        long dbId = 100L;
        long tableId = 200L;
        long physicalPartitionId = 300L;
        long recordedVersion = 15L;
        long visibleVersion = 10L; // Visible version <= recorded version, should keep

        Map<Long, Long> partMap = new HashMap<>();
        partMap.put(physicalPartitionId, recordedVersion);

        Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
        tableMap.put(tableId, partMap);

        Map<Long, Map<Long, Map<Long, Long>>> restoredVersions = new HashMap<>();
        restoredVersions.put(dbId, tableMap);

        Database db = new Database(dbId, "test_db");
        OlapTable table = new OlapTable();
        PhysicalPartition physicalPartition = Deencapsulation.newInstance(PhysicalPartition.class);
        Deencapsulation.setField(physicalPartition, "visibleVersion", visibleVersion);

        new Expectations(table) {
            {
                table.getPhysicalPartition(physicalPartitionId);
                result = physicalPartition;
                times = 1;
            }
        };

        new Expectations(physicalPartition) {
            {
                physicalPartition.getVisibleVersion();
                result = visibleVersion;
                times = 1;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 1;

                globalStateMgr.isLeader();
                result = true;
                times = 1;

                globalStateMgr.getClusterSnapshotRestoredVersionMgr();
                result = restoreMgr;
                minTimes = 1;

                restoreMgr.getRestoredCommittedVersions();
                result = restoredVersions;
                times = 1;

                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                times = 2;

                localMetastore.getDb(dbId);
                result = db;
                times = 1;

                localMetastore.getTable(dbId, tableId);
                result = table;
                times = 1;

                restoreMgr.writeLog((Map) any);
                times = 0;
            }
        };

        Deencapsulation.invoke(cleaner, "cleanObsoleteEntriesInternal");
    }

    @Test
    public void testMultiplePartitionsMixed() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        long dbId = 100L;
        long tableId = 200L;
        long physicalPartitionId1 = 300L;
        long physicalPartitionId2 = 301L;
        long recordedVersion1 = 10L;
        long recordedVersion2 = 15L;

        Map<Long, Long> partMap = new HashMap<>();
        partMap.put(physicalPartitionId1, recordedVersion1);
        partMap.put(physicalPartitionId2, recordedVersion2);

        Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
        tableMap.put(tableId, partMap);

        Map<Long, Map<Long, Map<Long, Long>>> restoredVersions = new HashMap<>();
        restoredVersions.put(dbId, tableMap);

        Database db = new Database(dbId, "test_db");
        OlapTable table = new OlapTable();

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 1;

                globalStateMgr.isLeader();
                result = true;
                times = 1;

                globalStateMgr.getClusterSnapshotRestoredVersionMgr();
                result = restoreMgr;
                minTimes = 1;

                restoreMgr.getRestoredCommittedVersions();
                result = restoredVersions;
                times = 1;

                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                minTimes = 1;

                localMetastore.getDb(dbId);
                result = db;
                times = 1;

                localMetastore.getTable(dbId, tableId);
                result = table;
                times = 1;

                restoreMgr.writeLog((Map) any);
                minTimes = 1;
            }
        };

        Deencapsulation.invoke(cleaner, "cleanObsoleteEntriesInternal");
    }

    @Test
    public void testEmptyTableMap() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        long dbId = 100L;

        Map<Long, Map<Long, Map<Long, Long>>> restoredVersions = new HashMap<>();
        restoredVersions.put(dbId, new HashMap<>()); // Empty table map

        Database db = new Database(dbId, "test_db");

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 1;

                globalStateMgr.isLeader();
                result = true;
                times = 1;

                globalStateMgr.getClusterSnapshotRestoredVersionMgr();
                result = restoreMgr;
                minTimes = 1;

                restoreMgr.getRestoredCommittedVersions();
                result = restoredVersions;
                times = 1;

                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                times = 1;

                localMetastore.getDb(dbId);
                result = db;
                times = 1;

                restoreMgr.writeLog((Map) any);
                times = 0;
            }
        };

        Deencapsulation.invoke(cleaner, "cleanObsoleteEntriesInternal");
    }

    @Test
    public void testEmptyPartitionMap() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        long dbId = 100L;
        long tableId = 200L;

        Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
        tableMap.put(tableId, new HashMap<>()); // Empty partition map

        Map<Long, Map<Long, Map<Long, Long>>> restoredVersions = new HashMap<>();
        restoredVersions.put(dbId, tableMap);

        Database db = new Database(dbId, "test_db");
        OlapTable table = new OlapTable();

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 1;

                globalStateMgr.isLeader();
                result = true;
                times = 1;

                globalStateMgr.getClusterSnapshotRestoredVersionMgr();
                result = restoreMgr;
                minTimes = 1;

                restoreMgr.getRestoredCommittedVersions();
                result = restoredVersions;
                times = 1;

                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                minTimes = 1; // Called at least twice: getDb() and getTable()

                localMetastore.getDb(dbId);
                result = db;
                times = 1;

                localMetastore.getTable(dbId, tableId);
                result = (OlapTable) table;
                times = 1;

                restoreMgr.writeLog((Map) any);
                times = 0;
            }
        };

        Deencapsulation.invoke(cleaner, "cleanObsoleteEntriesInternal");
    }
}
