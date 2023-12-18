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

package com.starrocks.binlog;

import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.QueryableReentrantReadWriteLock;
import com.starrocks.meta.lock.LockType;
import com.starrocks.meta.lock.Locker;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TTabletMetaType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class BinlogManager {

    private static final Logger LOG = LogManager.getLogger(BinlogManager.class);

    // tablet statistics through report handler
    // tableId -> tabletId -> beId
    private Map<Long, Map<Long, Set<Long>>> tabletStatistics;

    // tableId -> number of reported tablet from different BE
    private Map<Long, Long> tableIdToReportedNum;

    // tableId -> binlog version
    private Map<Long, Long> tableIdToBinlogVersion;

    // tableId -> ReplicaCount
    private Map<Long, Long> tableIdToReplicaCount;

    // tableId -> PartitionId
    private Map<Long, Set<Long>> tableIdToPartitions;

    public static final long INVALID = -1;

    QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock(true);

    public BinlogManager() {
        tableIdToReportedNum = new HashMap<>();
        tableIdToBinlogVersion = new HashMap<>();
        tableIdToReplicaCount = new HashMap<>();
        tabletStatistics = new HashMap<>();
        tableIdToPartitions = new HashMap<>();
    }

    private boolean checkIsPartitionChanged(Set<Long> prePartitions, Collection<PhysicalPartition> curPartitions) {
        if (prePartitions.size() != curPartitions.size()) {
            return true;
        }
        for (PhysicalPartition partition : curPartitions) {
            if (!prePartitions.contains(partition.getId())) {
                return true;
            }
        }
        return false;
    }

    // the caller should not hold the write lock of Database
    public void checkAndSetBinlogAvailableVersion(Database db, OlapTable table, long tabletId, long beId) {
        lock.writeLock().lock();
        try {
            Locker locker = new Locker();
            try {
                // check if partitions has changed
                locker.lockDatabase(db, LockType.READ);
                Set<Long> partitions = tableIdToPartitions.computeIfAbsent(table.getId(), key -> new HashSet<>());
                boolean isPartitionChanged = false;
                // if partitions is empty indicates that the tablet is
                // the first tablet of the table to be reported,
                // no need to check whether the partitions have been changed
                if (!partitions.isEmpty()) {
                    isPartitionChanged = checkIsPartitionChanged(partitions, table.getAllPhysicalPartitions());
                }

                if (isPartitionChanged) {
                    // for the sake of simplicity, if the partition have been changed, re-statistics
                    partitions.clear();
                    partitions.addAll(table.getAllPhysicalPartitions().stream().
                            map(partition -> partition.getId()).
                            collect(Collectors.toSet()));
                    tableIdToReportedNum.clear();
                    tableIdToBinlogVersion.clear();
                    tableIdToReplicaCount.clear();
                    tabletStatistics.clear();
                }

                Map<Long, Set<Long>> allTabletIds = tabletStatistics.computeIfAbsent(table.getId(), key -> new HashMap<>());

                // the previous version binlog config was not completed,
                // and a new version was generated,
                // the tablets counted need to be cleared
                if (tableIdToBinlogVersion.containsKey(table.getId()) &&
                        tableIdToBinlogVersion.get(table.getId()) != table.getBinlogVersion()) {
                    allTabletIds.clear();
                    tableIdToReportedNum.remove(table.getId());
                }
                tableIdToBinlogVersion.put(table.getId(), table.getBinlogVersion());
                if (!tableIdToReplicaCount.containsKey(table.getId())) {
                    long totalReplicaCount = table.getAllPhysicalPartitions().stream().
                            map(partition -> partition.getBaseIndex().getReplicaCount()).
                            reduce(0L, (acc, n) -> acc + n);
                    tableIdToReplicaCount.put(table.getId(), totalReplicaCount);
                }
                long num = tableIdToReportedNum.computeIfAbsent(table.getId(), key -> 0L);

                Set<Long> allBeIds = allTabletIds.computeIfAbsent(tabletId, key -> new HashSet<>());
                if (!allBeIds.contains(beId)) {
                    num++;
                    tableIdToReportedNum.put(table.getId(), num);
                    allBeIds.add(beId);
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }

            if (tableIdToReportedNum.get(table.getId()).equals(tableIdToReplicaCount.get(table.getId()))) {
                locker.lockDatabase(db, LockType.WRITE);
                try {
                    // check again if all replicas have been reported
                    long totalReplicaCount = table.getAllPhysicalPartitions().stream().
                            map(partition -> partition.getBaseIndex().getReplicaCount()).
                            reduce(0L, (acc, n) -> acc + n);

                    if (totalReplicaCount != tableIdToReplicaCount.get(table.getId())) {
                        tableIdToBinlogVersion.put(table.getId(), totalReplicaCount);
                        return;
                    }

                    long binlogTxnId = table.getBinlogTxnId();
                    if (binlogTxnId == INVALID) {
                        // the binlog config takes effect in all tablets of the table in BE for now
                        Long nextTxnId = GlobalStateMgr.getCurrentGlobalTransactionMgr().
                                getTransactionIDGenerator().getNextTransactionId();
                        table.setBinlogTxnId(binlogTxnId);
                        binlogTxnId = nextTxnId;
                    }
                    // check whether the concurrent imports when binlog is enabled have completed
                    List<Long> tableList = new ArrayList<>();
                    tableList.add(table.getId());
                    boolean isFinished = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().
                            getDatabaseTransactionMgr(db.getId()).isPreviousTransactionsFinished(
                                    binlogTxnId, tableList);

                    if (isFinished) {
                        Map<String, String> properties = table.buildBinlogAvailableVersion();
                        ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(db.getId(),
                                table.getId(), properties);
                        GlobalStateMgr.getCurrentState().getEditLog().logModifyBinlogAvailableVersion(log);
                        table.setBinlogAvailableVersion(properties);
                        LOG.info("set binlog available version tableName : {}, partitions : {}",
                                table.getName(), properties.toString());
                        tabletStatistics.remove(table.getId());
                        tableIdToReportedNum.remove(table.getId());
                        tableIdToReplicaCount.remove(table.getId());
                        tableIdToPartitions.remove(table.getId());
                    }
                } catch (AnalysisException e) {
                    LOG.warn(e);
                } finally {
                    locker.unLockDatabase(db, LockType.WRITE);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // the caller don't need to hold the db lock
    // for updateBinlogConfigMeta will get the db writelock
    // return true means that the modification of FEMeta is successful
    public boolean tryEnableBinlog(Database db, long tableId, long binlogTtL, long binlogMaxSize) {
        // pass properties not binlogConfig is for
        // unify the logic of alter table manaul and alter table of mv trigger
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, "true");
        if (!(binlogTtL == INVALID)) {
            properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_TTL, String.valueOf(binlogTtL));
        }
        if (!(binlogMaxSize == INVALID)) {
            properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE, String.valueOf(binlogMaxSize));
        }

        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        return schemaChangeHandler.updateBinlogConfigMeta(db, tableId, properties, TTabletMetaType.BINLOG_CONFIG);
    }

    // the caller don't need to hold the db lock
    // for updateBinlogConfigMeta will get the db writelock
    // return true means that the modification of FEMeta is successful,
    public boolean tryDisableBinlog(Database db, long tableId) {
        // modify FE meta, return true
        // try best effort to distribute BE tasks
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, "false");
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        return schemaChangeHandler.updateBinlogConfigMeta(db, tableId, properties, TTabletMetaType.DISABLE_BINLOG);
    }


    public boolean isBinlogAvailable(long dbId, long tableId) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                OlapTable olapTable = (OlapTable) db.getTable(tableId);
                if (olapTable != null) {
                    return olapTable.getBinlogAvailableVersion().size() != 0;
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }
        return false;
    }

    // isBinlogavailable should be called first to make sure that
    // the binlog is available
    // result : partitionId -> binlogAvailableVersion, null indicates the db or table is dropped
    public Map<Long, Long> getBinlogAvailableVersion(long dbId, long tableId) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                OlapTable olapTable = (OlapTable) db.getTable(tableId);
                if (olapTable != null) {
                    return olapTable.getBinlogAvailableVersion();
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }
        return null;
    }

    // return all tables with binlog enabled and current binlogConfig with the latest version
    // result : <tableId, BinlogConfig>
    public HashMap<Long, BinlogConfig> showAllBinlog() {
        HashMap<Long, BinlogConfig> allTablesWithBinlogConfigMap = new HashMap<>();
        List<Long> allDbIds = GlobalStateMgr.getCurrentState().getDbIds();
        for (Long dbId : allDbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db != null) {
                Locker locker = new Locker();
                locker.lockDatabase(db, LockType.READ);
                try {
                    List<Table> tables = db.getTables();
                    for (Table table : tables) {
                        if (table.isOlapTable() && ((OlapTable) table).isBinlogEnabled()) {
                            allTablesWithBinlogConfigMap.put(table.getId(), ((OlapTable) table).getCurBinlogConfig());
                        }
                    }
                } finally {
                    locker.unLockDatabase(db, LockType.READ);
                }
            }
        }
        return allTablesWithBinlogConfigMap;
    }
}