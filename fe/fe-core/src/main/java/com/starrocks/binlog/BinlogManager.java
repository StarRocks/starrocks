// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.binlog;

import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.UpdateBinlogAvailableVersionInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TTabletMetaType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BinlogManager {

    private static final Logger LOG = LogManager.getLogger(BinlogManager.class);

    private Map<Long, Long> binlogTxnIdToTableId;

    // all tableIds to be set binlogAvailableVersions
    // persistence is required, because if the mater is switched when the binlog has been enabled
    // but the binlogAvailableVersion has not been set through publish verison,
    // the information loss will cause the binlogAvailabeVersion to be unable to be set all the time

    //tableId -> dbId
    private Map<Long, Long> allTableIds;

    // When enable binlog, there may be concurrent imports
    // record the map between all txnIds of concurrently imports
    // and binlogTxnId
    private Map<Long, Set<Long>> transcationidToBinlogTxnId;

    private ReentrantReadWriteLock binlogLock = new ReentrantReadWriteLock(true);

    public static final long INVALID = -1;

    public BinlogManager() {
        binlogTxnIdToTableId = new HashMap<>();
        transcationidToBinlogTxnId = new HashMap<>();
        allTableIds = new HashMap<>();
    }

    public Map<Long, Long> getAllTableIds() {
        return allTableIds;
    }

    public void setAllTableIds(Map<Long, Long> allTableIds) {
        this.allTableIds = allTableIds;
    }

    public void recordBinlogTxnId(long binlogTxnId, long dbId, long tableId, Set<Long> transactionIds) {
        if (binlogTxnIdToTableId.containsKey(binlogTxnId)) {
            // should not happen, for the binlogTxnId can not be duplicate
            LOG.warn("the binlogTxnId has added to the binlog manager");
            return;
        }
        binlogLock.writeLock().lock();
        try {
            binlogTxnIdToTableId.put(binlogTxnId, tableId);
            allTableIds.put(tableId, dbId);

            // null when replay editlog
            if (transactionIds != null) {
                for (Long transactionId : transactionIds) {
                    transcationidToBinlogTxnId.putIfAbsent(transactionId, new HashSet<Long>());
                    transcationidToBinlogTxnId.get(transactionId).add(binlogTxnId);
                }
            }
        } finally {
            binlogLock.writeLock().unlock();
        }

    }

    // if the mater is switched when the binlog has been enabled
    // but the binlogAvailableVersion has not been set through publish verison,
    // then new FE master need to check and set the binlogAvaiableVesion of the table to visibleVersion
    public void setLeftTableBinlogAvailableVersion() {
        for (Map.Entry<Long, Long> entry : allTableIds.entrySet()) {
            Database db = GlobalStateMgr.getCurrentState().getDb(entry.getValue());
            if (db != null) {
                OlapTable table = (OlapTable) db.getTable(entry.getKey());
                if (table != null && table.enableBinlog()) {
                    Optional<Partition> fisrtPartition = table.getAllPartitions().stream().findFirst();
                    if (fisrtPartition.isPresent() && fisrtPartition.get().getBinlogAvailableVersion() == INVALID) {
                        HashMap<Long, Long> allPartitionToAvailableVesion = new HashMap<>();
                        table.getAllPartitions().forEach(partition -> {
                            partition.setBinlogAvailableVersion(partition.getVisibleVersion() + 1);
                            allPartitionToAvailableVesion.put(partition.getId(), partition.getBinlogAvailableVersion());
                        });

                        UpdateBinlogAvailableVersionInfo info = new UpdateBinlogAvailableVersionInfo(entry.getValue(),
                                entry.getKey(), true, allPartitionToAvailableVesion);
                    }

                }
            }
        }
        allTableIds.clear();
    }
    public boolean isBinlogWaitingTransactionId(long transactionId) {
        binlogLock.readLock().lock();
        try {
            if (transcationidToBinlogTxnId.containsKey(transactionId)) {
                return true;
            }
        } finally {
            binlogLock.readLock().unlock();
        }
        return false;
    }

    // the caller hold the write lock of Database
    public void checkAndSetBinlogAvailableVersion(long toPublishTxnId, Set<Long> runningTransactionId,
                                                  Database db) {
        binlogLock.readLock().lock();
        Set<Long> binlogTxnIds = transcationidToBinlogTxnId.get(toPublishTxnId);
        if (binlogTxnIds == null) {
            return;
        }
        for (Long binlogTxnId : transcationidToBinlogTxnId.get(toPublishTxnId)) {
            boolean isLast = true;
            for (Long transactionId : runningTransactionId) {
                if (transactionId != toPublishTxnId && transactionId < binlogTxnId) {
                    isLast = false;
                }
            }
            if (isLast) {
                Long tableId = binlogTxnIdToTableId.get(binlogTxnId);
                OlapTable olapTable = (OlapTable) db.getTable(tableId);
                Map<Long, Long> partitionIdToAvailableVersion = olapTable.setBinlogAvailableVersin();
                UpdateBinlogAvailableVersionInfo updateBinlogAvailableVersionInfo = new UpdateBinlogAvailableVersionInfo(
                        db.getId(), olapTable.getId(), true, partitionIdToAvailableVersion);
                GlobalStateMgr.getCurrentState().getEditLog().
                        logModifyBinlogAvailableVersion(updateBinlogAvailableVersionInfo);

                allTableIds.remove(tableId);
                binlogTxnIdToTableId.remove(binlogTxnId);
            }
        }
        transcationidToBinlogTxnId.remove(toPublishTxnId);
        binlogLock.readLock().lock();
    }

    // the caller don't need to hold the db lock
    public boolean tryEnableBinlog(Database db, long tableId, long binlogTtL, long binlogMaxSize) {
        // after the FE metadata is successfully modified,
        // flag of context will be set to true
        BinlogContext context = new BinlogContext();
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

        try {
            SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
            schemaChangeHandler.updateBinlogConfigMeta(db, tableId, properties, context, TTabletMetaType.BINLOG_CONFIG);
        } catch (DdlException exception) {
            return context.isModifyFeMetaSuccess();
        }
        return true;

    }

    // the caller don't need to hold the db lock
    public boolean tryCloseBinlog(Database db, long tableId) {
        try {
            // modify FE meta, return true
            // try to distrucite BE task best effort
            HashMap<String, String> properties = new HashMap<>();
            properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, "false");
            SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
            schemaChangeHandler.updateBinlogConfigMeta(db, tableId, properties, null, TTabletMetaType.DISABLE_BINLOG);
        } catch (DdlException e) {
            return false;
        }
        return true;
    }

    // Check whether the binlogAvailableVersion of all partitions of the table is available,
    // return true if available
    // return false if anyone is unavailable,
    public boolean isBinlogAvailable(long tableId, long dbId) {
        OlapTable olaptable = (OlapTable) GlobalStateMgr.getCurrentState().getDb(dbId).getTable(tableId);
        long failedNum = olaptable.getPartitions().stream().filter(partition -> {
            return partition.getBinlogAvailableVersion() == -1;
        }).count();

        if (failedNum != 0) {
            return false;
        }
        return true;
    }

    // isBinlogavailable should be called first to make sure that
    // the binlog is available
    public HashMap<Partition, Long> getBinlogAvailableVersion(long tableId, long dbId) {
        HashMap<Partition, Long> partionToAvilableVersionMap = new HashMap<>();
        OlapTable olaptable = (OlapTable) GlobalStateMgr.getCurrentState().getDb(dbId).getTable(tableId);
        olaptable.getPartitions().stream().forEach(
                partiton -> partionToAvilableVersionMap.put(partiton, partiton.getBinlogAvailableVersion()));
        return partionToAvilableVersionMap;
    }

    // return all tables with binlog enabled and the latest version itls current binlogConfig
    // result : <tableId,BinlogConfig>
    public HashMap<Long, BinlogConfig> showAllBinlog() {
        HashMap<Long, BinlogConfig> allTablesWithBinlogConfigMap = new HashMap<>();
        List<Long> allDbIds = GlobalStateMgr.getCurrentState().getDbIds();
        for (Long dbId : allDbIds) {
            List<Table> tables = GlobalStateMgr.getCurrentState().getDb(dbId).getTables();
            for (Table table : tables) {
                if (table.isOlapTable() && ((OlapTable) table).enableBinlog()) {
                    allTablesWithBinlogConfigMap.put(table.getId(), ((OlapTable) table).getCurBinlogConfig());
                }
            }
        }
        return allTablesWithBinlogConfigMap;
    }

    public static class BinlogContext {
        boolean isModifyFeMetaSuccess = false;

        public void setModifyFeMetaSuccess(boolean modifyFeMetaSuccess) {
            isModifyFeMetaSuccess = modifyFeMetaSuccess;
        }

        public boolean isModifyFeMetaSuccess() {
            return isModifyFeMetaSuccess;
        }
    }
}