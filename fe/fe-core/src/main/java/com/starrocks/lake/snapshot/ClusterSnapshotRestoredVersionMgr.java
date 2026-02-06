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
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ClusterSnapshotRestoredVersionMgr {
    private static final Logger LOG = LogManager.getLogger(ClusterSnapshotRestoredVersionMgr.class);

    // dbId -> tableId -> physicalPartitionId -> committedVersion (only when
    // committedVersion > visibleVersion)
    // This map is always replaced as a whole with a freshly built instance (never
    // mutated concurrently),
    // so marking it volatile is enough to guarantee visibility across threads.
    private volatile Map<Long, Map<Long, Map<Long, Long>>> restoredCommittedVersions = Collections.emptyMap();

    public Map<Long, Map<Long, Map<Long, Long>>> getRestoredCommittedVersions() {
        return restoredCommittedVersions;
    }

    public void updateRestoredCommittedVersions(
            Map<Long, Map<Long, Map<Long, Long>>> newRestoredCommittedVersions) {
        if (newRestoredCommittedVersions == null) {
            restoredCommittedVersions = Collections.emptyMap();
        } else {
            restoredCommittedVersions = newRestoredCommittedVersions;
        }
    }

    public boolean isRestoreVersion(long dbId, long tableId, long physicalPartitionId, long version) {
        if (restoredCommittedVersions == null || restoredCommittedVersions.isEmpty()) {
            return false;
        }

        Map<Long, Map<Long, Long>> tableMap = restoredCommittedVersions.get(dbId);
        if (tableMap == null || tableMap.isEmpty()) {
            return false;
        }

        Map<Long, Long> physicalPartMap = tableMap.get(tableId);
        if (physicalPartMap == null || physicalPartMap.isEmpty()) {
            return false;
        }

        Long committedVersion = physicalPartMap.get(physicalPartitionId);
        return committedVersion != null && committedVersion >= version;
    }

    /**
     * Initialize restoredCommittedVersions when:
     * 1. FE starts with cluster_snapshot and
     * 2. The snapshot is an external cluster snapshot.
     *
     * This method constructs the versionMap but does NOT persist it.
     * Call persist() after all edit logs are loaded to persist the versionMap.
     *
     * This method is idempotent and can be safely called during FE start.
     */
    public void init(ClusterSnapshotInfo clusterSnapshotInfo) {

        if (clusterSnapshotInfo == null) {
            LOG.warn("ClusterSnapshotInfo is null");
            return;
        }

        Map<Long, DatabaseSnapshotInfo> dbInfos = clusterSnapshotInfo.getDbInfos();
        if (dbInfos == null || dbInfos.isEmpty()) {
            LOG.info("ClusterSnapshotInfo has empty dbInfos");
            return;
        }

        Map<Long, Map<Long, Map<Long, Long>>> result = new HashMap<>();

        for (Map.Entry<Long, DatabaseSnapshotInfo> dbEntry : dbInfos.entrySet()) {
            Long dbId = dbEntry.getKey();
            DatabaseSnapshotInfo dbInfo = dbEntry.getValue();
            if (dbInfo == null || dbInfo.tableInfos == null || dbInfo.tableInfos.isEmpty()) {
                continue;
            }

            Map<Long, Map<Long, Long>> tableMap = new HashMap<>();
            for (Map.Entry<Long, TableSnapshotInfo> tableEntry : dbInfo.tableInfos.entrySet()) {
                Long tableId = tableEntry.getKey();
                TableSnapshotInfo tableInfo = tableEntry.getValue();
                if (tableInfo == null || tableInfo.partInfos == null || tableInfo.partInfos.isEmpty()) {
                    continue;
                }

                Map<Long, Long> physicalPartMap = new HashMap<>();
                for (Map.Entry<Long, PartitionSnapshotInfo> partEntry : tableInfo.partInfos.entrySet()) {
                    PartitionSnapshotInfo partInfo = partEntry.getValue();
                    if (partInfo == null || partInfo.physicalPartInfos == null
                            || partInfo.physicalPartInfos.isEmpty()) {
                        continue;
                    }

                    for (Map.Entry<Long, PhysicalPartitionSnapshotInfo> physicalPartEntry : partInfo.physicalPartInfos
                            .entrySet()) {
                        Long physicalPartId = physicalPartEntry.getKey();
                        PhysicalPartitionSnapshotInfo physicalPartInfo = physicalPartEntry.getValue();
                        if (physicalPartInfo == null) {
                            continue;
                        }
                        long visibleVersion = physicalPartInfo.visibleVersion;
                        long committedVersion = physicalPartInfo.committedVersion;
                        if (committedVersion > visibleVersion) {
                            physicalPartMap.put(physicalPartId, committedVersion);
                        }
                        LOG.debug("dbId: {}, tableId: {}, partId: {}, ver: {}, cVers: {}", dbId, tableId,
                                physicalPartId,
                                visibleVersion, committedVersion);
                    }
                }

                if (!physicalPartMap.isEmpty()) {
                    tableMap.put(tableId, physicalPartMap);
                }
            }

            if (!tableMap.isEmpty()) {
                result.put(dbId, tableMap);
            }
        }

        LOG.info("Initialized restored committed versions, db count: {}, table count: {}",
                result.size(),
                result.values().stream().mapToInt(m -> m.size()).sum());
        // EditLog is already initialized, so we can write log directly
        // Write log first, then update memory in WALApplier after log is successfully written
        writeLogInternal(result);
    }

    /**
     * Write log with new restored committed versions.
     * The memory will be updated in WALApplier after the log is successfully written.
     */
    public void writeLog(Map<Long, Map<Long, Map<Long, Long>>> newRestoredCommittedVersions) {
        writeLogInternal(newRestoredCommittedVersions);
    }

    private void writeLogInternal(Map<Long, Map<Long, Map<Long, Long>>> newRestoredCommittedVersions) {
        ClusterSnapshotRestoredVersionLog log = new ClusterSnapshotRestoredVersionLog(newRestoredCommittedVersions);
        // Update memory in WALApplier after editLog is successfully written.
        // This ensures that memory is only updated after the log is persisted.
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotRestoredVersion(log, wal -> {
            if (newRestoredCommittedVersions == null) {
                restoredCommittedVersions = Collections.emptyMap();
            } else {
                restoredCommittedVersions = newRestoredCommittedVersions;
            }
        });
        // Use newRestoredCommittedVersions for logging since it's the value that was actually written to log
        if (newRestoredCommittedVersions == null || newRestoredCommittedVersions.isEmpty()) {
            LOG.info("Persisted empty restored committed versions to edit log (cleared)");
        } else {
            LOG.info("Persisted restored committed versions to edit log, db count: {}, table count: {}",
                    newRestoredCommittedVersions.size(),
                    newRestoredCommittedVersions.values().stream().mapToInt(m -> m.size()).sum());
        }
    }

    /**
     * Replay the restored committed versions log from edit log.
     * This method is called during edit log replay to restore the version map.
     */
    public void replayLog(ClusterSnapshotRestoredVersionLog log) {
        if (log == null || log.getRestoredCommittedVersions() == null) {
            LOG.warn("ClusterSnapshotRestoredVersionLog is null or empty, skip replay");
            restoredCommittedVersions = Collections.emptyMap();
            return;
        }

        restoredCommittedVersions = new HashMap<>(log.getRestoredCommittedVersions());
        LOG.info("Replayed restored committed versions from edit log, db count: {}, table count: {}",
                restoredCommittedVersions.size(),
                restoredCommittedVersions.values().stream().mapToInt(m -> m.size()).sum());
    }

}
