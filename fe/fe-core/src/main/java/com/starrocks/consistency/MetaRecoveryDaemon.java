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

package com.starrocks.consistency;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.PartitionVersionRecoveryInfo.PartitionVersion;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MetaRecoveryDaemon extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(MetaRecoveryDaemon.class);

    private final Set<UnRecoveredPartition> unRecoveredPartitions = new HashSet<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public MetaRecoveryDaemon() {
        super("meta_recovery");
    }

    @Override
    protected void runAfterCatalogReady() {
        recover();
    }

    public void recover() {
        GlobalStateMgr stateMgr = GlobalStateMgr.getCurrentState();
        if (!checkTabletReportCacheUp(stateMgr.getDominationStartTimeMs())) {
            LOG.warn("check tablet report time failed, wait for next round");
            return;
        }

        List<PartitionVersion> partitionsToRecover = new ArrayList<>();
        List<Long> dbIds = stateMgr.getLocalMetastore().getDbIds();
        for (long dbId : dbIds) {
            Database database = stateMgr.getLocalMetastore().getDb(dbId);
            if (database == null || database.isSystemDatabase()) {
                continue;
            }

            Locker locker = new Locker();
            locker.lockDatabase(database.getId(), LockType.READ);
            try {
                for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(database.getId())) {
                    if (!table.isOlapTableOrMaterializedView()) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;

                    for (Partition partition : olapTable.getAllPartitions()) {
                        short replicaNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());

                        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                            long recoverVersion = -1L;
                            // commonVersion is used to record all replica versions on this partition.
                            // key is the version, value is a flag means whether this version is existing on all tablets.
                            Map<Long, Boolean> commonVersion = new HashMap<>();
                            StringBuilder info = new StringBuilder();
                            boolean isFirstTablet = true;
                            boolean foundCommonVersion = true;
                            for (MaterializedIndex idx : physicalPartition.getMaterializedIndices(
                                    MaterializedIndex.IndexExtState.VISIBLE)) {
                                for (Tablet tablet : idx.getTablets()) {
                                    LocalTablet localTablet = (LocalTablet) tablet;
                                    long quorumVersion = localTablet.getQuorumVersion(replicaNum / 2 + 1);
                                    if (quorumVersion == -1L) {
                                        foundCommonVersion = false;
                                        info.append(String.format("cannot find quorum version for tablet: %d; ",
                                                tablet.getId()));
                                        LOG.warn("cannot find quorum version for tablet: {}, " +
                                                "ignore partition: {}-{} table: {}-{} db: {}-{}",
                                                tablet.getId(), physicalPartition.getId(), partition.getName(),
                                                table.getId(), table.getName(), database.getId(), database.getFullName());
                                    }
                                    if (recoverVersion == -1L) {
                                        recoverVersion = quorumVersion;
                                    } else if (recoverVersion != quorumVersion) {
                                        foundCommonVersion = false;
                                        info.append(String.format("found different quorum versions: %d %d; ",
                                                recoverVersion, quorumVersion));
                                        LOG.warn("found different quorum versions: {} {}, " +
                                                        "ignore partition: {}-{} table: {}-{} db: {}-{}",
                                                recoverVersion, quorumVersion, physicalPartition.getId(), partition.getName(),
                                                table.getId(), table.getName(), database.getId(), database.getFullName());
                                    }

                                    Set<Long> replicaVersions = localTablet.getAllReplicaVersions();
                                    if (isFirstTablet) {
                                        for (Long version : replicaVersions) {
                                            commonVersion.put(version, true);
                                        }
                                        isFirstTablet = false;
                                    } else {
                                        for (Map.Entry<Long, Boolean> entry : commonVersion.entrySet()) {
                                            if (!replicaVersions.contains(entry.getKey())) {
                                                entry.setValue(false);
                                            }
                                        }
                                    }
                                }
                            }
                            if (foundCommonVersion && recoverVersion != physicalPartition.getVisibleVersion()) {
                                if (GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                                        .hasCommittedTxnOnPartition(dbId, table.getId(), physicalPartition.getId())) {
                                    LOG.warn("There are committed txns waiting to publish, " +
                                                    "ignore partition: {}-{} table: {}-{} db: {}-{}",
                                            physicalPartition.getId(), partition.getName(), table.getId(),
                                            table.getName(), database.getId(), database.getFullName());
                                    UnRecoveredPartition recoveryInfo = new UnRecoveredPartition(database.getFullName(),
                                            table.getName(), partition.getName(), physicalPartition.getId(),
                                            "There are committed txns waiting to publish, ignore this partition");
                                    addToUnRecoveredPartitions(recoveryInfo);
                                } else {
                                    partitionsToRecover.add(new PartitionVersion(dbId, table.getId(),
                                            physicalPartition.getId(), recoverVersion));
                                }
                            } else if (!foundCommonVersion) {
                                long maxCommonVersion = -1L;
                                for (Map.Entry<Long, Boolean> entry : commonVersion.entrySet()) {
                                    if (entry.getValue() && entry.getKey() > maxCommonVersion) {
                                        maxCommonVersion = entry.getKey();
                                    }
                                }
                                UnRecoveredPartition recoveryInfo = new UnRecoveredPartition(database.getFullName(),
                                        table.getName(), partition.getName(), physicalPartition.getId(), info.toString());
                                if (maxCommonVersion != -1L) {
                                    recoveryInfo.setMaxCommonVersion(maxCommonVersion);
                                }
                                addToUnRecoveredPartitions(recoveryInfo);
                            }
                        }
                    }
                }
            } finally {
                locker.unLockDatabase(database.getId(), LockType.READ);
            }
        }

        PartitionVersionRecoveryInfo recoveryInfo =
                new PartitionVersionRecoveryInfo(partitionsToRecover, System.currentTimeMillis());

        recoverPartitionVersion(recoveryInfo);

        GlobalStateMgr.getCurrentState().getEditLog()
                .logRecoverPartitionVersion(new PartitionVersionRecoveryInfo(partitionsToRecover, System.currentTimeMillis()));
    }

    public void recoverPartitionVersion(PartitionVersionRecoveryInfo recoveryInfo) {
        for (PartitionVersion version : recoveryInfo.getPartitionVersions()) {
            Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(version.getDbId());
            if (database == null) {
                LOG.warn("recover partition version failed, db is null, versionInfo: {}", version);
                continue;
            }
            Locker locker = new Locker();
            locker.lockDatabase(database.getId(), LockType.WRITE);
            try {
                Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(database.getId(), version.getTableId());
                if (table == null) {
                    LOG.warn("recover partition version failed, table is null, versionInfo: {}", version);
                    continue;
                }
                if (!table.isOlapTableOrMaterializedView()) {
                    LOG.warn("recover partition version failed, table is not OLAP table, versionInfo: {}", version);
                    continue;
                }
                PhysicalPartition physicalPartition = table.getPhysicalPartition(version.getPartitionId());
                if (physicalPartition == null) {
                    LOG.warn("recover partition version failed, partition is null, versionInfo: {}", version);
                    continue;
                }

                Partition partition = table.getPartition(physicalPartition.getParentId());
                if (partition == null) {
                    LOG.warn("recover partition version failed, partition is null, versionInfo: {}", version);
                    continue;
                }

                long originVisibleVersion = physicalPartition.getVisibleVersion();
                long originNextVersion = physicalPartition.getNextVersion();
                physicalPartition.setVisibleVersion(version.getVersion(), recoveryInfo.getRecoverTime());
                physicalPartition.setNextVersion(version.getVersion() + 1);
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        if (!(tablet instanceof LocalTablet)) {
                            continue;
                        }

                        LocalTablet localTablet = (LocalTablet) tablet;
                        for (Replica replica : localTablet.getAllReplicas()) {
                            if (replica.getVersion() > version.getVersion() && localTablet.getAllReplicas().size() > 1) {
                                replica.setBad(true);
                                LOG.warn("set tablet: {} on backend: {} to bad, " +
                                        "because its version: {} is higher than recovery version: {}",
                                        tablet.getId(), replica.getBackendId(), replica.getVersion(), version.getVersion());
                            }
                        }
                    }
                }
                LOG.info("set partition visible version from {} to {}, " +
                        "set partition next version from {} to {}, versionInfo: {}, db name: {}, table name: {}",
                        originVisibleVersion, physicalPartition.getVisibleVersion(), originNextVersion,
                        physicalPartition.getNextVersion(), version, database.getFullName(), table.getName());
                removeUnRecoveredPartitions(new UnRecoveredPartition(database.getFullName(),
                        table.getName(), partition.getName(), physicalPartition.getId(), null));
            } finally {
                locker.unLockDatabase(database.getId(), LockType.WRITE);
            }
        }
    }

    private void addToUnRecoveredPartitions(UnRecoveredPartition info) {
        this.lock.writeLock().lock();
        try {
            this.unRecoveredPartitions.add(info);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    private void removeUnRecoveredPartitions(UnRecoveredPartition info) {
        this.lock.writeLock().lock();
        try {
            this.unRecoveredPartitions.remove(info);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void fetchProcNodeResult(BaseProcResult result) {
        this.lock.readLock().lock();
        try {
            for (UnRecoveredPartition info : unRecoveredPartitions) {
                String advise;
                if (info.getMaxCommonVersion() != -1L) {
                    advise = String.format("You can set the partition version to %d, using command: " +
                                    "admin set table %s partition (%d) version to %d",
                            info.getMaxCommonVersion(), info.getDbName() + "." + info.getTableName(),
                            info.getPartitionId(), info.getMaxCommonVersion());
                } else {
                    advise = "no";
                }
                result.addRow(Lists.newArrayList(info.getDbName(), info.getTableName(),
                        info.getPartitionName(), Long.toString(info.getPartitionId()), info.getInfo(), advise));
            }
        } finally {
            this.lock.readLock().unlock();
        }
    }

    protected boolean checkTabletReportCacheUp(long timeMs) {
        for (Backend backend : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends()) {
            if (TimeUtils.timeStringToLong(backend.getBackendStatus().lastSuccessReportTabletsTime)
                    < timeMs) {
                LOG.warn("last tablet report time of backend {}:{} is {}, should wait it to report tablets",
                        backend.getHost(), backend.getHeartbeatPort(), backend.getBackendStatus().lastSuccessReportTabletsTime);
                return false;
            }
        }
        return true;
    }

    public static class UnRecoveredPartition {
        private final String dbName;
        private final String tableName;
        private final String partitionName;
        private final long partitionId;
        private long maxCommonVersion = -1L;
        private final String info;

        public UnRecoveredPartition(String dbName, String tableName, String partitionName, long partitionId, String info) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.partitionName = partitionName;
            this.partitionId = partitionId;
            this.info = info;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTableName() {
            return tableName;
        }

        public String getPartitionName() {
            return partitionName;
        }

        public long getPartitionId() {
            return partitionId;
        }

        public void setMaxCommonVersion(long maxCommonVersion) {
            this.maxCommonVersion = maxCommonVersion;
        }

        public long getMaxCommonVersion() {
            return this.maxCommonVersion;
        }

        public String getInfo() {
            return info;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, partitionName, partitionId);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof UnRecoveredPartition)) {
                return false;
            }

            UnRecoveredPartition other = (UnRecoveredPartition) obj;
            return this.dbName.equals(other.dbName)
                    && this.tableName.equals(other.tableName)
                    && this.partitionName.equals(other.partitionName)
                    && this.partitionId == other.partitionId;
        }
    }
}
