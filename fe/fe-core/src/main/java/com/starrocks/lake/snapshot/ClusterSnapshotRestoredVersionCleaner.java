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
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Periodically clean obsolete entries in {@link ClusterSnapshotRestoredVersionMgr}.
 *
 * This daemon:
 *  - runs only when FE is ready
 *  - is only effective in shared-data mode and on leader FE
 *  - every interval, scans the restoredCommittedVersions map and
 *    removes entries whose partitions have caught up or been dropped
 *  - persists the updated map
 */
public class ClusterSnapshotRestoredVersionCleaner extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(ClusterSnapshotRestoredVersionCleaner.class);

    public ClusterSnapshotRestoredVersionCleaner(long intervalMs) {
        super("cluster-snapshot-restored-version-cleaner", intervalMs);
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            cleanObsoleteEntriesInternal();
        } catch (Throwable t) {
            LOG.warn("Unexpected exception when cleaning cluster snapshot restored versions", t);
        }
    }

    private void cleanObsoleteEntriesInternal() {
        // Only meaningful in shared-data mode and on leader FE
        if (!RunMode.isSharedDataMode()) {
            return;
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        if (globalStateMgr == null || !globalStateMgr.isLeader()) {
            return;
        }

        Map<Long, Map<Long, Map<Long, Long>>> restoredCommittedVersions =
                globalStateMgr.getClusterSnapshotRestoredVersionMgr().getRestoredCommittedVersions();
        if (restoredCommittedVersions == null || restoredCommittedVersions.isEmpty()) {
            return;
        }

        Map<Long, Map<Long, Map<Long, Long>>> oldMap = restoredCommittedVersions;
        Map<Long, Map<Long, Map<Long, Long>>> newMap = new HashMap<>();
        boolean changed = false;

        for (Map.Entry<Long, Map<Long, Map<Long, Long>>> dbEntry : oldMap.entrySet()) {
            long dbId = dbEntry.getKey();
            Database db = globalStateMgr.getLocalMetastore().getDb(dbId);
            if (db == null) {
                // db dropped, remove whole db entry
                changed = true;
                continue;
            }

            Map<Long, Map<Long, Long>> tableMap = dbEntry.getValue();
            if (tableMap == null || tableMap.isEmpty()) {
                continue;
            }

            Map<Long, Map<Long, Long>> newTableMap = new HashMap<>();

            for (Map.Entry<Long, Map<Long, Long>> tableEntry : tableMap.entrySet()) {
                long tableId = tableEntry.getKey();
                OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getTable(dbId, tableId);
                if (table == null) {
                    // table dropped, remove whole table entry
                    changed = true;
                    continue;
                }

                Map<Long, Long> partMap = tableEntry.getValue();
                if (partMap == null || partMap.isEmpty()) {
                    continue;
                }

                Map<Long, Long> newPartMap = new HashMap<>();
                for (Map.Entry<Long, Long> partEntry : partMap.entrySet()) {
                    long physicalPartitionId = partEntry.getKey();
                    long recordedVersion = partEntry.getValue();
                    PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                    if (physicalPartition == null) {
                        // partition dropped
                        changed = true;
                        continue;
                    }

                    long visibleVersion = physicalPartition.getVisibleVersion();
                    // If the partition's visible version has caught up (or surpassed) the recorded
                    // committed version, we can drop this entry.
                    if (visibleVersion >= recordedVersion) {
                        changed = true;
                        continue;
                    }

                    newPartMap.put(physicalPartitionId, recordedVersion);
                }

                if (!newPartMap.isEmpty()) {
                    newTableMap.put(tableId, newPartMap);
                }
            }

            if (!newTableMap.isEmpty()) {
                newMap.put(dbId, newTableMap);
            }
        }

        if (changed) {
            ClusterSnapshotRestoredVersionMgr restoreMgr =
                    GlobalStateMgr.getCurrentState().getClusterSnapshotRestoredVersionMgr();
            if (restoreMgr != null) {
                // Write log first, then update memory in WALApplier after log is successfully written
                restoreMgr.writeLog(newMap);
            }
            LOG.info("Cleaned obsolete cluster snapshot restored versions, db count: {}, table count: {}",
                    newMap.size(),
                    newMap.values().stream().mapToInt(m -> m.size()).sum());
        }
    }
}


