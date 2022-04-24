package com.starrocks.server;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.StarRocksFE;
import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.AddRollupClause;
import com.starrocks.analysis.AdminCheckTabletsStmt;
import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterDatabaseQuotaStmt;
import com.starrocks.analysis.AlterDatabaseRename;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.CancelAlterSystemStmt;
import com.starrocks.analysis.CancelAlterTableStmt;
import com.starrocks.analysis.ColumnRenameClause;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateFunctionStmt;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateTableLikeStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropFunctionStmt;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropPartitionClause;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.MultiRangePartitionDesc;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.analysis.PartitionRenameClause;
import com.starrocks.analysis.RangePartitionDesc;
import com.starrocks.analysis.RecoverDbStmt;
import com.starrocks.analysis.RecoverPartitionStmt;
import com.starrocks.analysis.RecoverTableStmt;
import com.starrocks.analysis.RefreshExternalTableStmt;
import com.starrocks.analysis.ReplacePartitionClause;
import com.starrocks.analysis.RollupRenameClause;
import com.starrocks.analysis.ShowAlterStmt;
import com.starrocks.analysis.SingleRangePartitionDesc;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.analysis.TruncateTableStmt;
import com.starrocks.catalog.BrokerTable;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.StarOSTablet;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.cluster.BaseParam;
import com.starrocks.cluster.Cluster;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.QueryableReentrantLock;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.meta.MetaContext;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.AddPartitionsInfo;
import com.starrocks.persist.BackendIdsUpdateInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropDbInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.DropLinkDbAndUpdateDbInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.MultiEraseTableInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfo;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.spi.Metadata;
import com.starrocks.spi.WithPersistence;
import com.starrocks.system.Backend;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.task.DropReplicaTask;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRefreshTableRequest;
import com.starrocks.thrift.TRefreshTableResponse;
import com.starrocks.thrift.TSetConfigRequest;
import com.starrocks.thrift.TSetConfigResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageFormat;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.common.SystemIdGenerator.getNextId;
import static com.starrocks.server.GlobalStateMgr.NEXT_ID_INIT_VALUE;
import static com.starrocks.server.GlobalStateMgr.getCurrentCatalogJournalVersion;
import static com.starrocks.server.GlobalStateMgr.getCurrentSystemInfo;
import static com.starrocks.server.GlobalStateMgr.isCheckpointThread;

public class LocalMetastore implements Metadata, WithPersistence {
    private static final Logger LOG = LogManager.getLogger(LocalMetastore.class);

    private ConcurrentHashMap<Long, Database> idToDb;
    private ConcurrentHashMap<String, Database> fullNameToDb;

    private ConcurrentHashMap<Long, Cluster> idToCluster;
    private ConcurrentHashMap<String, Cluster> nameToCluster;

    private EditLog editLog;


    public LocalMetastore() {
        this.idToDb = new ConcurrentHashMap<>();
        this.fullNameToDb = new ConcurrentHashMap<>();
        this.idToCluster = new ConcurrentHashMap<>();
        this.nameToCluster = new ConcurrentHashMap<>();
    }

    @Override
    public Table getTable(String dbName, String tblName) throws AnalysisException {
        Database database = getDb(dbName);
        if (database == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        return database.getTable(tblName);
    }

    public void renameRollup(Database db, OlapTable table, RollupRenameClause renameClause) throws DdlException {
        if (table.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + table.getName() + "] is under " + table.getState());
        }

        String rollupName = renameClause.getRollupName();
        // check if it is base table name
        if (rollupName.equals(table.getName())) {
            throw new DdlException("Using ALTER TABLE RENAME to change table name");
        }

        String newRollupName = renameClause.getNewRollupName();
        if (rollupName.equals(newRollupName)) {
            throw new DdlException("Same rollup name");
        }

        Map<String, Long> indexNameToIdMap = table.getIndexNameToId();
        if (indexNameToIdMap.get(rollupName) == null) {
            throw new DdlException("Rollup index[" + rollupName + "] does not exists");
        }

        // check if name is already used
        if (indexNameToIdMap.get(newRollupName) != null) {
            throw new DdlException("Rollup name[" + newRollupName + "] is already used");
        }

        long indexId = indexNameToIdMap.remove(rollupName);
        indexNameToIdMap.put(newRollupName, indexId);

        // log
        TableInfo tableInfo = TableInfo.createForRollupRename(db.getId(), table.getId(), indexId, newRollupName);
        editLog.logRollupRename(tableInfo);
        LOG.info("rename rollup[{}] to {}", rollupName, newRollupName);
    }


    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
    }

    public List<String> getDbNames() {
        return Lists.newArrayList(fullNameToDb.keySet());
    }

    private Long getNextId() {
        return GlobalStateMgr.getCurrentState().getNextId();
    }

    public ConcurrentHashMap<String, Database> getFullNameToDb() {
        return fullNameToDb;
    }

    // The interface which DdlExecutor needs.
    @Override
    public void createDb(CreateDbStmt stmt) throws DdlException {
        final String clusterName = stmt.getClusterName();
        String fullDbName = stmt.getFullDbName();
        long id = 0L;
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (!nameToCluster.containsKey(clusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_SELECT_CLUSTER, clusterName);
            }
            if (fullNameToDb.containsKey(fullDbName)) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create database[{}] which already exists", fullDbName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, fullDbName);
                }
            } else {
                id = getNextId();
                Database db = new Database(id, fullDbName);
                db.setClusterName(clusterName);
                unprotectCreateDb(db);
                editLog.logCreateDb(db);
            }
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
        LOG.info("createDb dbName = " + fullDbName + ", id = " + id);
    }

    // special
    // For replay edit log, needn't lock metadata
    public void unprotectCreateDb(Database db) {
        idToDb.put(db.getId(), db);
        fullNameToDb.put(db.getFullName(), db);
        final Cluster cluster = nameToCluster.get(db.getClusterName());
        cluster.addDb(db.getFullName(), db.getId());
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());
    }

    // special
    @Override
    public long loadDb(DataInputStream dis, long checksum) throws IOException, DdlException {
        int dbCount = dis.readInt();
        long newChecksum = checksum ^ dbCount;
        for (long i = 0; i < dbCount; ++i) {
            Database db = new Database();
            db.readFields(dis);
            newChecksum ^= db.getId();
            idToDb.put(db.getId(), db);
            fullNameToDb.put(db.getFullName(), db);
            if (db.getDbState() == Database.DbState.LINK) {
                fullNameToDb.put(db.getAttachDb(), db);
            }
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());
        }
        LOG.info("finished replay databases from image");
        return newChecksum;
    }

    public void recreateTabletInvertIndex() {
        if (isCheckpointThread()) {
            return;
        }

        // create inverted index
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Database db : this.fullNameToDb.values()) {
            long dbId = db.getId();
            for (Table table : db.getTables()) {
                if (table.getType() != Table.TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                long tableId = olapTable.getId();
                Collection<Partition> allPartitions = olapTable.getAllPartitions();
                for (Partition partition : allPartitions) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    boolean useStarOS = partition.isUseStarOS();
                    for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        long indexId = index.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium);
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            if (!useStarOS) {
                                for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                                    invertedIndex.addReplica(tabletId, replica);
                                    if (MetaContext.get().getMetaVersion() < FeMetaVersion.VERSION_48) {
                                        // set replica's schema hash
                                        replica.setSchemaHash(schemaHash);
                                    }
                                }
                            }
                        }
                    } // end for indices
                } // end for partitions
            } // end for tables
        } // end for dbs
    }

    // special
    @Override
    public long saveDb(DataOutputStream dos, long checksum) throws IOException {
        int dbCount = idToDb.size() - nameToCluster.keySet().size();
        checksum ^= dbCount;
        dos.writeInt(dbCount);
        for (Map.Entry<Long, Database> entry : idToDb.entrySet()) {
            Database db = entry.getValue();
            String dbName = db.getFullName();
            // Don't write information_schema db meta
            if (!InfoSchemaDb.isInfoSchemaDb(dbName)) {
                checksum ^= entry.getKey();
                db.readLock();
                try {
                    db.write(dos);
                } finally {
                    db.readUnlock();
                }
            }
        }
        return checksum;
    }

    // for test
    public void addCluster(Cluster cluster) {
        nameToCluster.put(cluster.getName(), cluster);
        idToCluster.put(cluster.getId(), cluster);
    }

    // special
    @Override
    public void replayCreateDb(Database db) {
        GlobalStateMgr.getCurrentState().tryLock(true);
        try {
            unprotectCreateDb(db);
            LOG.info("finish replay create db, name: {}, id: {}", db.getFullName(), db.getId());
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    // OVERRIDE
    @Override
    public void dropDb(DropDbStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();

        // 1. check if database exists
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (!fullNameToDb.containsKey(dbName)) {
                if (stmt.isSetIfExists()) {
                    LOG.info("drop database[{}] which does not exist", dbName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
                }
            }

            // 2. drop tables in db
            Database db = this.fullNameToDb.get(dbName);
            db.writeLock();
            try {
                if (!stmt.isForceDrop()) {
                    if (GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                            .existCommittedTxns(db.getId(), null, null)) {
                        throw new DdlException(
                                "There are still some transactions in the COMMITTED state waiting to be completed. " +
                                        "The database [" + dbName +
                                        "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                        " please use \"DROP database FORCE\".");
                    }
                }
                if (db.getDbState() == Database.DbState.LINK && dbName.equals(db.getAttachDb())) {
                    // We try to drop a hard link.
                    final DropLinkDbAndUpdateDbInfo info = new DropLinkDbAndUpdateDbInfo();
                    fullNameToDb.remove(db.getAttachDb());
                    db.setDbState(Database.DbState.NORMAL);
                    info.setUpdateDbState(Database.DbState.NORMAL);
                    final Cluster cluster = nameToCluster
                            .get(ClusterNamespace.getClusterNameFromFullName(db.getAttachDb()));
                    final BaseParam param = new BaseParam();
                    param.addStringParam(db.getAttachDb());
                    param.addLongParam(db.getId());
                    cluster.removeLinkDb(param);
                    info.setDropDbCluster(cluster.getName());
                    info.setDropDbId(db.getId());
                    info.setDropDbName(db.getAttachDb());
                    editLog.logDropLinkDb(info);
                    return;
                }

                if (db.getDbState() == Database.DbState.LINK && dbName.equals(db.getFullName())) {
                    // We try to drop a db which other dbs attach to it,
                    // which is not allowed.
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DB_STATE_LINK_OR_MIGRATE,
                            ClusterNamespace.getNameFromFullName(dbName));
                    return;
                }

                if (dbName.equals(db.getAttachDb()) && db.getDbState() == Database.DbState.MOVE) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DB_STATE_LINK_OR_MIGRATE,
                            ClusterNamespace.getNameFromFullName(dbName));
                    return;
                }

                // save table names for recycling
                Set<String> tableNames = db.getTableNamesWithLock();
                unprotectDropDb(db, stmt.isForceDrop(), false);
                if (!stmt.isForceDrop()) {
                    GlobalStateMgr.getCurrentRecycleBin().recycleDatabase(db, tableNames);
                } else {
                    onEraseDatabase(db.getId());
                }
            } finally {
                db.writeUnlock();
            }

            // 3. remove db from catalog
            idToDb.remove(db.getId());
            fullNameToDb.remove(db.getFullName());
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.removeDb(dbName, db.getId());
            DropDbInfo info = new DropDbInfo(dbName, stmt.isForceDrop());
            editLog.logDropDb(info);

            LOG.info("finish drop database[{}], id: {}, is force : {}", dbName, db.getId(), stmt.isForceDrop());
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    // private
    public void unprotectDropDb(Database db, boolean isForeDrop, boolean isReplay) {
        for (Table table : db.getTables()) {
            unprotectDropTable(db, table.getId(), isForeDrop, isReplay);
        }
    }

    // special
    @Override
    public void replayDropLinkDb(DropLinkDbAndUpdateDbInfo info) {
        GlobalStateMgr.getCurrentState().tryLock(true);
        try {
            final Database db = this.fullNameToDb.remove(info.getDropDbName());
            db.setDbState(info.getUpdateDbState());
            final Cluster cluster = nameToCluster
                    .get(info.getDropDbCluster());
            final BaseParam param = new BaseParam();
            param.addStringParam(db.getAttachDb());
            param.addLongParam(db.getId());
            cluster.removeLinkDb(param);
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    // special
    @Override
    public void replayDropDb(String dbName, boolean isForceDrop) throws DdlException {
        GlobalStateMgr.getCurrentState().tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            db.writeLock();
            try {
                Set<String> tableNames = db.getTableNamesWithLock();
                unprotectDropDb(db, isForceDrop, true);
                if (!isForceDrop) {
                    GlobalStateMgr.getCurrentRecycleBin().recycleDatabase(db, tableNames);
                } else {
                    onEraseDatabase(db.getId());
                }
            } finally {
                db.writeUnlock();
            }

            fullNameToDb.remove(dbName);
            idToDb.remove(db.getId());
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.removeDb(dbName, db.getId());

            LOG.info("finish replay drop db, name: {}, id: {}", dbName, db.getId());
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    // OVERRIDE
    @Override
    public Database getDb(long dbId) {
        return idToDb.get(dbId);
    }

    // OVERRIDE
    @Override
    public Database getDb(String name) {
        if (fullNameToDb.containsKey(name)) {
            return fullNameToDb.get(name);
        } else {
            // This maybe a information_schema db request, and information_schema db name is case insensitive.
            // So, we first extract db name to check if it is information_schema.
            // Then we reassemble the origin cluster name with lower case db name,
            // and finally get information_schema db from the name map.
            String dbName = ClusterNamespace.getNameFromFullName(name);
            if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
                String clusterName = ClusterNamespace.getClusterNameFromFullName(name);
                return fullNameToDb.get(ClusterNamespace.getFullName(clusterName, dbName.toLowerCase()));
            }
        }
        return null;
    }


    @Override
    public void replayRenameRollup(TableInfo tableInfo) throws DdlException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long indexId = tableInfo.getIndexId();
        String newRollupName = tableInfo.getNewRollupName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            String rollupName = table.getIndexNameById(indexId);
            Map<String, Long> indexNameToIdMap = table.getIndexNameToId();
            indexNameToIdMap.remove(rollupName);
            indexNameToIdMap.put(newRollupName, indexId);

            LOG.info("replay rename rollup[{}] to {}", rollupName, newRollupName);
        } finally {
            db.writeUnlock();
        }
    }

    // special for recovder
    public void recoverDatabase(RecoverDbStmt recoverStmt) throws DdlException {
        // check is new db with same name already exist
        if (getDb(recoverStmt.getDbName()) != null) {
            throw new DdlException("Database[" + recoverStmt.getDbName() + "] already exist.");
        }

        Database db = GlobalStateMgr.getCurrentRecycleBin().recoverDatabase(recoverStmt.getDbName());

        // add db to catalog
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (fullNameToDb.containsKey(db.getFullName())) {
                throw new DdlException("Database[" + db.getFullName() + "] already exist.");
                // it's ok that we do not put db back to CatalogRecycleBin
                // cause this db cannot recover any more
            }

            fullNameToDb.put(db.getFullName(), db);
            idToDb.put(db.getId(), db);
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.addDb(db.getFullName(), db.getId());

            // log
            RecoverInfo recoverInfo = new RecoverInfo(db.getId(), -1L, -1L);
            editLog.logRecoverDb(recoverInfo);
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }

        LOG.info("finish recover database, name: {}, id: {}", recoverStmt.getDbName(), db.getId());
    }

    // OVERRIDE or special
    public void recoverTable(RecoverTableStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();

        Database db = null;
        if ((db = getDb(dbName)) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = recoverStmt.getTableName();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table != null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }

            if (!GlobalStateMgr.getCurrentRecycleBin().recoverTable(db, tableName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }
        } finally {
            db.writeUnlock();
        }
    }

    // OVERRIDE or special
    public void recoverPartition(RecoverPartitionStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();

        Database db = null;
        if ((db = getDb(dbName)) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = recoverStmt.getTableName();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (table.getType() != Table.TableType.OLAP) {
                throw new DdlException("table[" + tableName + "] is not OLAP table");
            }
            OlapTable olapTable = (OlapTable) table;

            String partitionName = recoverStmt.getPartitionName();
            if (olapTable.getPartition(partitionName) != null) {
                throw new DdlException("partition[" + partitionName + "] already exist in table[" + tableName + "]");
            }

            GlobalStateMgr.getCurrentRecycleBin().recoverPartition(db.getId(), olapTable, partitionName);
        } finally {
            db.writeUnlock();
        }
    }

    // OVERRIDE or special
    @Override
    public void replayRecoverDatabase(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = GlobalStateMgr.getCurrentRecycleBin().replayRecoverDatabase(dbId);

        // add db to catalog
        replayCreateDb(db);

        LOG.info("replay recover db[{}], name: {}", dbId, db.getFullName());
    }

    // special
    @Override
    public void alterDatabaseQuota(AlterDatabaseQuotaStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        AlterDatabaseQuotaStmt.QuotaType quotaType = stmt.getQuotaType();
        if (quotaType == AlterDatabaseQuotaStmt.QuotaType.DATA) {
            db.setDataQuotaWithLock(stmt.getQuota());
        } else if (quotaType == AlterDatabaseQuotaStmt.QuotaType.REPLICA) {
            db.setReplicaQuotaWithLock(stmt.getQuota());
        }
        long quota = stmt.getQuota();
        DatabaseInfo dbInfo = new DatabaseInfo(dbName, "", quota, quotaType);
        editLog.logAlterDb(dbInfo);
    }

    // special
    @Override
    public void replayAlterDatabaseQuota(String dbName, long quota, AlterDatabaseQuotaStmt.QuotaType quotaType) {
        Database db = getDb(dbName);
        Preconditions.checkNotNull(db);
        if (quotaType == AlterDatabaseQuotaStmt.QuotaType.DATA) {
            db.setDataQuotaWithLock(quota);
        } else if (quotaType == AlterDatabaseQuotaStmt.QuotaType.REPLICA) {
            db.setReplicaQuotaWithLock(quota);
        }
    }

    // override
    @Override
    public void renameDatabase(AlterDatabaseRename stmt) throws DdlException {
        String fullDbName = stmt.getDbName();
        String newFullDbName = stmt.getNewDbName();
        String clusterName = stmt.getClusterName();

        if (fullDbName.equals(newFullDbName)) {
            throw new DdlException("Same database name");
        }

        Database db = null;
        Cluster cluster = null;
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            cluster = nameToCluster.get(clusterName);
            if (cluster == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
            }
            // check if db exists
            db = fullNameToDb.get(fullDbName);
            if (db == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, fullDbName);
            }

            if (db.getDbState() == Database.DbState.LINK || db.getDbState() == Database.DbState.MOVE) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_RENAME_DB_ERR, fullDbName);
            }
            // check if name is already used
            if (fullNameToDb.get(newFullDbName) != null) {
                throw new DdlException("Database name[" + newFullDbName + "] is already used");
            }

            cluster.removeDb(db.getFullName(), db.getId());
            cluster.addDb(newFullDbName, db.getId());
            // 1. rename db
            db.setNameWithLock(newFullDbName);

            // 2. add to meta. check again
            fullNameToDb.remove(fullDbName);
            fullNameToDb.put(newFullDbName, db);

            DatabaseInfo dbInfo = new DatabaseInfo(fullDbName, newFullDbName, -1L, AlterDatabaseQuotaStmt.QuotaType.NONE);
            editLog.logDatabaseRename(dbInfo);
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }

        LOG.info("rename database[{}] to [{}], id: {}", fullDbName, newFullDbName, db.getId());
    }

    // special
    @Override
    public void replayRenameDatabase(String dbName, String newDbName) {
        GlobalStateMgr.getCurrentState().tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.removeDb(db.getFullName(), db.getId());
            db.setName(newDbName);
            cluster.addDb(newDbName, db.getId());
            fullNameToDb.remove(dbName);
            fullNameToDb.put(newDbName, db);

            LOG.info("replay rename database {} to {}, id: {}", dbName, newDbName, db.getId());
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    // override
    @Override
    public void createTable(CreateTableStmt stmt) throws DdlException {
        String engineName = stmt.getEngineName();
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        // check if db exists
        Database db = getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // only internal table should check quota and cluster capacity
        if (!stmt.isExternal()) {
            // check cluster capacity
            GlobalStateMgr.getCurrentState().getNodeMgr().getSystemInfo().checkClusterCapacity(stmt.getClusterName());
            // check db quota
            db.checkQuota();
        }

        // check if table exists in db
        db.readLock();
        try {
            if (db.getTable(tableName) != null) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            }
        } finally {
            db.readUnlock();
        }

        if (engineName.equals("olap")) {
            createOlapTable(db, stmt);
            return;
        } else if (engineName.equals("mysql")) {
            createMysqlTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("elasticsearch") || engineName.equalsIgnoreCase("es")) {
            createEsTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("hive")) {
            createHiveTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("iceberg")) {
            createIcebergTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("hudi")) {
            createHudiTable(db, stmt);
            return;
        } else if (engineName.equalsIgnoreCase("jdbc")) {
            createJDBCTable(db, stmt);
            return;
        } else {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_STORAGE_ENGINE, engineName);
        }
        Preconditions.checkState(false);
    }

    // private
    private void createJDBCTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        Map<String, String> properties = stmt.getProperties();
        long tableId = getNextId();
        JDBCTable jdbcTable = new JDBCTable(tableId, tableName, columns, properties);

        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }

        try {
            if (getDb(db.getFullName()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(jdbcTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table [{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }

        LOG.info("successfully create jdbc table[{}-{}]", tableName, tableId);
    }

    // private
    private void createHudiTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        columns.add(new Column("_hoodie_commit_time", Type.STRING, true));
        columns.add(new Column("_hoodie_commit_seqno", Type.STRING, true));
        columns.add(new Column("_hoodie_record_key", Type.STRING, true));
        columns.add(new Column("_hoodie_partition_path", Type.STRING, true));
        columns.add(new Column("_hoodie_file_name", Type.STRING, true));

        long tableId = getNextId();
        HudiTable hudiTable = new HudiTable(tableId, tableName, columns, stmt.getProperties());
        // partition key, commented for show partition key
        String partitionCmt = "PARTITION BY (" + String.join(", ", hudiTable.getPartitionColumnNames()) + ")";
        if (Strings.isNullOrEmpty(stmt.getComment())) {
            hudiTable.setComment(partitionCmt);
        } else {
            hudiTable.setComment(stmt.getComment());
        }

        // check database exists again, because database can be dropped when creating table
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (getDb(db.getFullName()) == null) {
                throw new DdlException("Database has been dropped when creating table");
            }
            if (!db.createTableWithLock(hudiTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("Create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }

        LOG.info("Successfully create table[{}-{}]", tableName, tableId);
    }

    // private
    private void createIcebergTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        long tableId = getNextId();
        IcebergTable icebergTable = new IcebergTable(tableId, tableName, columns, stmt.getProperties());
        if (!Strings.isNullOrEmpty(stmt.getComment())) {
            icebergTable.setComment(stmt.getComment());
        }

        // check database exists again, because database can be dropped when creating table
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(icebergTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }

        LOG.info("successfully create table[{}-{}]", tableName, tableId);
    }

    // private
    private void createHiveTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        long tableId = getNextId();
        HiveTable hiveTable = new HiveTable(tableId, tableName, columns, stmt.getProperties());
        // partition key, commented for show partition key
        String partitionCmt = "PARTITION BY (" + String.join(", ", hiveTable.getPartitionColumnNames()) + ")";
        if (Strings.isNullOrEmpty(stmt.getComment())) {
            hiveTable.setComment(partitionCmt);
        } else {
            hiveTable.setComment(stmt.getComment());
        }

        // check database exists again, because database can be dropped when creating table
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(hiveTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }

        LOG.info("successfully create table[{}-{}]", tableName, tableId);
    }

    // private
    private void validateColumns(List<Column> columns) throws DdlException {
        if (columns.isEmpty()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        boolean encounterValue = false;
        boolean hasKey = false;
        for (Column column : columns) {
            if (column.isKey()) {
                if (encounterValue) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_OLAP_KEY_MUST_BEFORE_VALUE);
                }
                hasKey = true;
            } else {
                encounterValue = true;
            }
        }

        if (!hasKey) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_KEYS);
        }
    }

    // private
    private void createMysqlTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();

        List<Column> columns = stmt.getColumns();

        long tableId = getNextId();
        MysqlTable mysqlTable = new MysqlTable(tableId, tableName, columns, stmt.getProperties());
        mysqlTable.setComment(stmt.getComment());

        // check database exists again, because database can be dropped when creating table
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }

        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(mysqlTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("Create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }

        LOG.info("Successfully create table[{}-{}]", tableName, tableId);
    }

    // private
    private void createEsTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();

        // create columns
        List<Column> baseSchema = stmt.getColumns();
        validateColumns(baseSchema);

        // create partition info
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        PartitionInfo partitionInfo = null;
        Map<String, Long> partitionNameToId = Maps.newHashMap();
        if (partitionDesc != null) {
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId, false);
        } else {
            long partitionId = getNextId();
            // use table name as single partition name
            partitionNameToId.put(tableName, partitionId);
            partitionInfo = new SinglePartitionInfo();
        }

        long tableId = getNextId();
        EsTable esTable = new EsTable(tableId, tableName, baseSchema, stmt.getProperties(), partitionInfo);
        esTable.setComment(stmt.getComment());

        // check database exists again, because database can be dropped when creating table
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(esTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }

        LOG.info("successfully create table{} with id {}", tableName, tableId);
    }

    // private
    // Create olap table and related base index synchronously.
    private void createOlapTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        LOG.debug("begin create olap table: {}", tableName);

        // create columns
        List<Column> baseSchema = stmt.getColumns();
        validateColumns(baseSchema);

        // create partition info
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        PartitionInfo partitionInfo = null;
        Map<String, Long> partitionNameToId = Maps.newHashMap();
        if (partitionDesc != null) {
            // gen partition id first
            if (partitionDesc instanceof RangePartitionDesc) {
                RangePartitionDesc rangeDesc = (RangePartitionDesc) partitionDesc;
                for (SingleRangePartitionDesc desc : rangeDesc.getSingleRangePartitionDescs()) {
                    long partitionId = getNextId();
                    partitionNameToId.put(desc.getPartitionName(), partitionId);
                }
            }
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId, false);
        } else {
            if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(stmt.getProperties())) {
                throw new DdlException("Only support dynamic partition properties on range partition table");
            }
            long partitionId = getNextId();
            // use table name as single partition name
            partitionNameToId.put(tableName, partitionId);
            partitionInfo = new SinglePartitionInfo();
        }

        // get keys type
        KeysDesc keysDesc = stmt.getKeysDesc();
        Preconditions.checkNotNull(keysDesc);
        KeysType keysType = keysDesc.getKeysType();

        // create distribution info
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo distributionInfo = distributionDesc.toDistributionInfo(baseSchema);

        // calc short key column count
        short shortKeyColumnCount = calcShortKeyColumnCount(baseSchema, stmt.getProperties());
        LOG.debug("create table[{}] short key column count: {}", tableName, shortKeyColumnCount);

        // indexes
        TableIndexes indexes = new TableIndexes(stmt.getIndexes());

        // create table
        long tableId = getNextId();
        OlapTable olapTable = null;
        if (stmt.isExternal()) {
            olapTable = new ExternalOlapTable(db.getId(), tableId, tableName, baseSchema, keysType, partitionInfo,
                    distributionInfo, indexes, stmt.getProperties());
        } else {
            olapTable = new OlapTable(tableId, tableName, baseSchema, keysType, partitionInfo,
                    distributionInfo, indexes);
        }
        olapTable.setComment(stmt.getComment());

        // set base index id
        long baseIndexId = getNextId();
        olapTable.setBaseIndexId(baseIndexId);

        // set base index info to table
        // this should be done before create partition.
        Map<String, String> properties = stmt.getProperties();

        // analyze bloom filter columns
        Set<String> bfColumns = null;
        double bfFpp = 0;
        try {
            bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, baseSchema,
                    olapTable.getKeysType() == KeysType.PRIMARY_KEYS);
            if (bfColumns != null && bfColumns.isEmpty()) {
                bfColumns = null;
            }

            bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(properties);
            if (bfColumns != null && bfFpp == 0) {
                bfFpp = FeConstants.default_bloom_filter_fpp;
            } else if (bfColumns == null) {
                bfFpp = 0;
            }

            olapTable.setBloomFilterInfo(bfColumns, bfFpp);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // analyze replication_num
        short replicationNum = FeConstants.default_replication_num;
        try {
            boolean isReplicationNumSet =
                    properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM);
            replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, replicationNum);
            if (isReplicationNumSet) {
                olapTable.setReplicationNum(replicationNum);
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // set in memory
        boolean isInMemory =
                PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);
        olapTable.setIsInMemory(isInMemory);

        TTabletType tabletType = TTabletType.TABLET_TYPE_DISK;
        try {
            tabletType = PropertyAnalyzer.analyzeTabletType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            // if this is an unpartitioned table, we should analyze data property and replication num here.
            // if this is a partitioned table, there properties are already analyzed in RangePartitionDesc analyze phase.

            // use table name as this single partition name
            long partitionId = partitionNameToId.get(tableName);
            DataProperty dataProperty = null;
            try {
                boolean hasMedium = false;
                if (properties != null) {
                    hasMedium = properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
                }
                dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, DataProperty.DEFAULT_DATA_PROPERTY);
                if (hasMedium) {
                    olapTable.setStorageMedium(dataProperty.getStorageMedium());
                }
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            Preconditions.checkNotNull(dataProperty);
            partitionInfo.setDataProperty(partitionId, dataProperty);
            partitionInfo.setReplicationNum(partitionId, replicationNum);
            partitionInfo.setIsInMemory(partitionId, isInMemory);
            partitionInfo.setTabletType(partitionId, tabletType);
        }

        // check colocation properties
        try {
            String colocateGroup = PropertyAnalyzer.analyzeColocate(properties);
            if (!Strings.isNullOrEmpty(colocateGroup)) {
                String fullGroupName = db.getId() + "_" + colocateGroup;
                ColocateGroupSchema groupSchema = GlobalStateMgr.getCurrentColocateIndex().getGroupSchema(fullGroupName);
                if (groupSchema != null) {
                    // group already exist, check if this table can be added to this group
                    groupSchema.checkColocateSchema(olapTable);
                }
                // add table to this group, if group does not exist, create a new one
                GlobalStateMgr.getCurrentColocateIndex().addTableToGroup(db.getId(), olapTable, colocateGroup,
                        null /* generate group id inside */);
                olapTable.setColocateGroup(colocateGroup);
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // get base index storage type. default is COLUMN
        TStorageType baseIndexStorageType = null;
        try {
            baseIndexStorageType = PropertyAnalyzer.analyzeStorageType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        Preconditions.checkNotNull(baseIndexStorageType);
        // set base index meta
        int schemaVersion = 0;
        try {
            schemaVersion = PropertyAnalyzer.analyzeSchemaVersion(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        int schemaHash = Util.schemaHash(schemaVersion, baseSchema, bfColumns, bfFpp);
        olapTable.setIndexMeta(baseIndexId, tableName, baseSchema, schemaVersion, schemaHash,
                shortKeyColumnCount, baseIndexStorageType, keysType);

        for (AlterClause alterClause : stmt.getRollupAlterClauseList()) {
            AddRollupClause addRollupClause = (AddRollupClause) alterClause;

            Long baseRollupIndex = olapTable.getIndexIdByName(tableName);

            // get storage type for rollup index
            TStorageType rollupIndexStorageType = null;
            try {
                rollupIndexStorageType = PropertyAnalyzer.analyzeStorageType(addRollupClause.getProperties());
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            Preconditions.checkNotNull(rollupIndexStorageType);
            // set rollup index meta to olap table
            List<Column> rollupColumns = GlobalStateMgr.getCurrentState().getRollupHandler().checkAndPrepareMaterializedView(addRollupClause,
                    olapTable, baseRollupIndex, false);
            short rollupShortKeyColumnCount =
                    calcShortKeyColumnCount(rollupColumns, alterClause.getProperties());
            int rollupSchemaHash = Util.schemaHash(schemaVersion, rollupColumns, bfColumns, bfFpp);
            long rollupIndexId = getNextId();
            olapTable.setIndexMeta(rollupIndexId, addRollupClause.getRollupName(), rollupColumns, schemaVersion,
                    rollupSchemaHash, rollupShortKeyColumnCount, rollupIndexStorageType, keysType);
        }

        // analyze version info
        Long version = null;
        try {
            version = PropertyAnalyzer.analyzeVersionInfo(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        Preconditions.checkNotNull(version);

        // get storage format
        TStorageFormat storageFormat = TStorageFormat.DEFAULT; // default means it's up to BE's config
        try {
            storageFormat = PropertyAnalyzer.analyzeStorageFormat(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        olapTable.setStorageFormat(storageFormat);

        // a set to record every new tablet created when create table
        // if failed in any step, use this set to do clear things
        Set<Long> tabletIdSet = new HashSet<Long>();

        boolean createTblSuccess = false;
        boolean addToColocateGroupSuccess = false;
        // create partition
        try {
            // do not create partition for external table
            if (olapTable.getType() == Table.TableType.OLAP) {
                if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                    // this is a 1-level partitioned table, use table name as partition name
                    long partitionId = partitionNameToId.get(tableName);
                    Partition partition = createPartition(db, olapTable, partitionId, tableName, version, tabletIdSet);
                    buildPartitions(db, olapTable, Collections.singletonList(partition));
                    olapTable.addPartition(partition);
                } else if (partitionInfo.getType() == PartitionType.RANGE) {
                    try {
                        // just for remove entries in stmt.getProperties(),
                        // and then check if there still has unknown properties
                        boolean hasMedium = false;
                        if (properties != null) {
                            hasMedium = properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
                        }
                        DataProperty dataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                                DataProperty.DEFAULT_DATA_PROPERTY);
                        DynamicPartitionUtil
                                .checkAndSetDynamicPartitionBuckets(properties, distributionDesc.getBuckets());
                        DynamicPartitionUtil.checkAndSetDynamicPartitionProperty(olapTable, properties);
                        if (olapTable.dynamicPartitionExists() && olapTable.getColocateGroup() != null) {
                            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                            if (info.getBucketNum() !=
                                    olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets()) {
                                throw new DdlException("dynamic_partition.buckets should equal the distribution buckets"
                                        + " if creating a colocate table");
                            }
                        }
                        if (hasMedium) {
                            olapTable.setStorageMedium(dataProperty.getStorageMedium());
                        }
                        if (properties != null && !properties.isEmpty()) {
                            // here, all properties should be checked
                            throw new DdlException("Unknown properties: " + properties);
                        }
                    } catch (AnalysisException e) {
                        throw new DdlException(e.getMessage());
                    }

                    // this is a 2-level partitioned tables
                    List<Partition> partitions = new ArrayList<>(partitionNameToId.size());
                    for (Map.Entry<String, Long> entry : partitionNameToId.entrySet()) {
                        Partition partition = createPartition(db, olapTable, entry.getValue(), entry.getKey(), version,
                                tabletIdSet);
                        partitions.add(partition);
                    }
                    // It's ok if partitions is empty.
                    buildPartitions(db, olapTable, partitions);
                    for (Partition partition : partitions) {
                        olapTable.addPartition(partition);
                    }
                } else {
                    throw new DdlException("Unsupported partition method: " + partitionInfo.getType().name());
                }
            }

            // check database exists again, because database can be dropped when creating table
            if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
                throw new DdlException("Failed to acquire catalog lock. Try again");
            }
            try {
                if (getDb(db.getId()) == null) {
                    throw new DdlException("database has been dropped when creating table");
                }
                createTblSuccess = db.createTableWithLock(olapTable, false);
                if (!createTblSuccess) {
                    if (!stmt.isSetIfNotExists()) {
                        ErrorReport
                                .reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                    } else {
                        LOG.info("Create table[{}] which already exists", tableName);
                        return;
                    }
                }
            } finally {
                GlobalStateMgr.getCurrentState().unlock();
            }

            // NOTE: The table has been added to the database, and the following procedure cannot throw exception.

            // we have added these index to memory, only need to persist here
            if (GlobalStateMgr.getCurrentColocateIndex().isColocateTable(tableId)) {
                ColocateTableIndex.GroupId groupId = GlobalStateMgr.getCurrentColocateIndex().getGroup(tableId);
                List<List<Long>> backendsPerBucketSeq = GlobalStateMgr.getCurrentColocateIndex().getBackendsPerBucketSeq(groupId);
                ColocatePersistInfo info =
                        ColocatePersistInfo.createForAddTable(groupId, tableId, backendsPerBucketSeq);
                editLog.logColocateAddTable(info);
                addToColocateGroupSuccess = true;
            }
            LOG.info("Successfully create table[{};{}]", tableName, tableId);
            // register or remove table from DynamicPartition after table created
            DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(db.getId(), olapTable);
            GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().createOrUpdateRuntimeInfo(
                    tableName, DynamicPartitionScheduler.LAST_UPDATE_TIME, TimeUtils.getCurrentFormatTime());
        } finally {
            if (!createTblSuccess) {
                for (Long tabletId : tabletIdSet) {
                    GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tabletId);
                }
            }
            // only remove from memory, because we have not persist it
            if (GlobalStateMgr.getCurrentColocateIndex().isColocateTable(tableId) && !addToColocateGroupSuccess) {
                GlobalStateMgr.getCurrentColocateIndex().removeTable(tableId);
            }
        }
    }

    // private
    private void buildPartitions(Database db, OlapTable table, List<Partition> partitions) throws DdlException {
        if (partitions.isEmpty()) {
            return;
        }
        int numAliveBackends = GlobalStateMgr.getCurrentState().getNodeMgr().getSystemInfo().getBackendIds(true).size();
        int numReplicas = 0;
        for (Partition partition : partitions) {
            numReplicas += partition.getReplicaCount();
        }

        if (partitions.size() >= 3 && numAliveBackends >= 3 && numReplicas >= numAliveBackends * 500) {
            LOG.info("creating {} partitions of table {} concurrently", partitions.size(), table.getName());
            buildPartitionsConcurrently(db.getId(), table, partitions, numReplicas, numAliveBackends);
        } else if (numAliveBackends > 0) {
            buildPartitionsSequentially(db.getId(), table, partitions, numReplicas, numAliveBackends);
        } else {
            throw new DdlException("no alive backend");
        }
    }

    // private
    private void buildPartitionsSequentially(long dbId, OlapTable table, List<Partition> partitions, int numReplicas,
                                             int numBackends) throws DdlException {
        // Try to bundle at least 200 CreateReplicaTask's in a single AgentBatchTask.
        // The number 200 is just an experiment value that seems to work without obvious problems, feel free to
        // change it if you have a better choice.
        int avgReplicasPerPartition = numReplicas / partitions.size();
        int partitionGroupSize = Math.max(1, numBackends * 200 / Math.max(1, avgReplicasPerPartition));
        for (int i = 0; i < partitions.size(); i += partitionGroupSize) {
            int endIndex = Math.min(partitions.size(), i + partitionGroupSize);
            List<CreateReplicaTask> tasks = buildCreateReplicaTasks(dbId, table, partitions.subList(i, endIndex));
            int partitionCount = endIndex - i;
            int indexCountPerPartition = partitions.get(i).getVisibleMaterializedIndicesCount();
            int timeout = Config.tablet_create_timeout_second * countMaxTasksPerBackend(tasks);
            // Compatible with older versions, `Config.max_create_table_timeout_second` is the timeout time for a single index.
            // Here we assume that all partitions have the same number of indexes.
            int maxTimeout = partitionCount * indexCountPerPartition * Config.max_create_table_timeout_second;
            try {
                sendCreateReplicaTasksAndWaitForFinished(tasks, Math.min(timeout, maxTimeout));
                tasks.clear();
            } finally {
                for (CreateReplicaTask task : tasks) {
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CREATE, task.getSignature());
                }
            }
        }
    }

    // private
    private int countMaxTasksPerBackend(List<CreateReplicaTask> tasks) {
        Map<Long, Integer> tasksPerBackend = new HashMap<>();
        for (CreateReplicaTask task : tasks) {
            tasksPerBackend.compute(task.getBackendId(), (k, v) -> (v == null) ? 1 : v + 1);
        }
        return Collections.max(tasksPerBackend.values());
    }

    // private
    private void buildPartitionsConcurrently(long dbId, OlapTable table, List<Partition> partitions, int numReplicas,
                                             int numBackends) throws DdlException {
        int timeout = numReplicas / numBackends * Config.tablet_create_timeout_second;
        int numIndexes = partitions.stream().mapToInt(Partition::getVisibleMaterializedIndicesCount).sum();
        int maxTimeout = numIndexes * Config.max_create_table_timeout_second;
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(numReplicas);
        Thread t = new Thread(() -> {
            Map<Long, List<Long>> taskSignatures = new HashMap<>();
            try {
                int numFinishedTasks;
                int numSendedTasks = 0;
                for (Partition partition : partitions) {
                    if (!countDownLatch.getStatus().ok()) {
                        break;
                    }
                    List<CreateReplicaTask> tasks = buildCreateReplicaTasks(dbId, table, partition);
                    for (CreateReplicaTask task : tasks) {
                        List<Long> signatures =
                                taskSignatures.computeIfAbsent(task.getBackendId(), k -> new ArrayList<>());
                        signatures.add(task.getSignature());
                    }
                    sendCreateReplicaTasks(tasks, countDownLatch);
                    numSendedTasks += tasks.size();
                    numFinishedTasks = numReplicas - (int) countDownLatch.getCount();
                    // Since there is no mechanism to cancel tasks, if we send a lot of tasks at once and some error or timeout
                    // occurs in the middle of the process, it will create a lot of useless replicas that will be deleted soon and
                    // waste machine resources. Sending a lot of tasks at once may also block other users' tasks for a long time.
                    // To avoid these situations, new tasks are sent only when the average number of tasks on each node is less
                    // than 200.
                    // (numSendedTasks - numFinishedTasks) is number of tasks that have been sent but not yet finished.
                    while (numSendedTasks - numFinishedTasks > 200 * numBackends) {
                        ThreadUtil.sleepAtLeastIgnoreInterrupts(100);
                        numFinishedTasks = numReplicas - (int) countDownLatch.getCount();
                    }
                }
                countDownLatch.await();
                if (countDownLatch.getStatus().ok()) {
                    taskSignatures.clear();
                }
            } catch (Exception e) {
                LOG.warn(e);
                countDownLatch.countDownToZero(new Status(TStatusCode.UNKNOWN, e.toString()));
            } finally {
                for (Map.Entry<Long, List<Long>> entry : taskSignatures.entrySet()) {
                    for (Long signature : entry.getValue()) {
                        AgentTaskQueue.removeTask(entry.getKey(), TTaskType.CREATE, signature);
                    }
                }
            }
        }, "partition-build");
        t.start();
        try {
            waitForFinished(countDownLatch, Math.min(timeout, maxTimeout));
        } catch (Exception e) {
            countDownLatch.countDownToZero(new Status(TStatusCode.UNKNOWN, e.getMessage()));
            throw e;
        }
    }

    // private
    private void sendCreateReplicaTasksAndWaitForFinished(List<CreateReplicaTask> tasks, long timeout)
            throws DdlException {
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(tasks.size());
        sendCreateReplicaTasks(tasks, countDownLatch);
        waitForFinished(countDownLatch, timeout);
    }

    // private
    private void waitForFinished(MarkedCountDownLatch<Long, Long> countDownLatch, long timeout) throws DdlException {
        try {
            if (countDownLatch.await(timeout, TimeUnit.SECONDS)) {
                if (!countDownLatch.getStatus().ok()) {
                    String errMsg = "fail to create tablet: " + countDownLatch.getStatus().getErrorMsg();
                    LOG.warn(errMsg);
                    throw new DdlException(errMsg);
                }
            } else { // timed out
                List<Map.Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                List<Map.Entry<Long, Long>> firstThree = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                StringBuilder sb = new StringBuilder("fail to create tablet: timed out. unfinished replicas");
                sb.append("(").append(firstThree.size()).append("/").append(unfinishedMarks.size()).append("): ");
                // Show details of the first 3 unfinished tablets.
                for (Map.Entry<Long, Long> mark : firstThree) {
                    sb.append(mark.getValue()); // TabletId
                    sb.append('(');
                    Backend backend = GlobalStateMgr.getCurrentState().getClusterInfo().getBackend(mark.getKey());
                    sb.append(backend != null ? backend.getHost() : "N/A");
                    sb.append(") ");
                }
                sb.append(" timeout=").append(timeout).append("s");
                String errMsg = sb.toString();
                LOG.warn(errMsg);
                countDownLatch.countDownToZero(new Status(TStatusCode.TIMEOUT, "timed out"));
                throw new DdlException(errMsg);
            }
        } catch (InterruptedException e) {
            LOG.warn(e);
            countDownLatch.countDownToZero(new Status(TStatusCode.CANCELLED, "cancelled"));
        }
    }

    // private
    private void sendCreateReplicaTasks(List<CreateReplicaTask> tasks,
                                        MarkedCountDownLatch<Long, Long> countDownLatch) {
        HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();
        for (CreateReplicaTask task : tasks) {
            task.setLatch(countDownLatch);
            countDownLatch.addMark(task.getBackendId(), task.getTabletId());
            AgentBatchTask batchTask = batchTaskMap.get(task.getBackendId());
            if (batchTask == null) {
                batchTask = new AgentBatchTask();
                batchTaskMap.put(task.getBackendId(), batchTask);
            }
            batchTask.addTask(task);
        }
        for (Map.Entry<Long, AgentBatchTask> entry : batchTaskMap.entrySet()) {
            AgentTaskQueue.addBatchTask(entry.getValue());
            AgentTaskExecutor.submit(entry.getValue());
        }
    }


    // private
    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, List<Partition> partitions) {
        List<CreateReplicaTask> tasks = new ArrayList<>();
        for (Partition partition : partitions) {
            tasks.addAll(buildCreateReplicaTasks(dbId, table, partition));
        }
        return tasks;
    }

    // private
    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, Partition partition) {
        ArrayList<CreateReplicaTask> tasks = new ArrayList<>((int) partition.getReplicaCount());
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            tasks.addAll(buildCreateReplicaTasks(dbId, table, partition, index));
        }
        return tasks;
    }

    // private
    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, Partition partition,
                                                            MaterializedIndex index) {
        List<CreateReplicaTask> tasks = new ArrayList<>((int) index.getReplicaCount());
        boolean useStarOS = partition.isUseStarOS();
        MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(index.getId());
        for (Tablet tablet : index.getTablets()) {
            if (useStarOS) {
                CreateReplicaTask task = new CreateReplicaTask(
                        ((StarOSTablet) tablet).getPrimaryBackendId(),
                        dbId,
                        table.getId(),
                        partition.getId(),
                        index.getId(),
                        tablet.getId(),
                        indexMeta.getShortKeyColumnCount(),
                        indexMeta.getSchemaHash(),
                        partition.getVisibleVersion(),
                        indexMeta.getKeysType(),
                        indexMeta.getStorageType(),
                        table.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium(),
                        indexMeta.getSchema(),
                        table.getBfColumns(),
                        table.getBfFpp(),
                        null,
                        table.getIndexes(),
                        table.getPartitionInfo().getIsInMemory(partition.getId()),
                        table.getPartitionInfo().getTabletType(partition.getId()));
                tasks.add(task);
            } else {
                for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                    CreateReplicaTask task = new CreateReplicaTask(
                            replica.getBackendId(),
                            dbId,
                            table.getId(),
                            partition.getId(),
                            index.getId(),
                            tablet.getId(),
                            indexMeta.getShortKeyColumnCount(),
                            indexMeta.getSchemaHash(),
                            partition.getVisibleVersion(),
                            indexMeta.getKeysType(),
                            indexMeta.getStorageType(),
                            table.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium(),
                            indexMeta.getSchema(),
                            table.getBfColumns(),
                            table.getBfFpp(),
                            null,
                            table.getIndexes(),
                            table.getPartitionInfo().getIsInMemory(partition.getId()),
                            table.getPartitionInfo().getTabletType(partition.getId()));
                    tasks.add(task);
                }
            }
        }
        return tasks;
    }

    // private
    private Partition createPartition(Database db, OlapTable table, long partitionId, String partitionName,
                                      Long version, Set<Long> tabletIdSet) throws DdlException {
        return createPartitionCommon(db, table, partitionId, partitionName, table.getPartitionInfo(), version,
                tabletIdSet);
    }

    // private
    private Partition createPartitionCommon(Database db, OlapTable table, long partitionId, String partitionName,
                                            PartitionInfo partitionInfo, Long version, Set<Long> tabletIdSet)
            throws DdlException {
        Map<Long, MaterializedIndex> indexMap = new HashMap<>();
        for (long indexId : table.getIndexIdToMeta().keySet()) {
            MaterializedIndex rollup = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
            rollup.setUseStarOS(partitionInfo.isUseStarOS(partitionId));
            indexMap.put(indexId, rollup);
        }
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        Partition partition =
                new Partition(partitionId, partitionName, indexMap.get(table.getBaseIndexId()), distributionInfo);
        partition.setPartitionInfo(partitionInfo);

        // version
        if (version != null) {
            partition.updateVisibleVersion(version);
        }

        short replicationNum = partitionInfo.getReplicationNum(partitionId);
        TStorageMedium storageMedium = partitionInfo.getDataProperty(partitionId).getStorageMedium();
        for (Map.Entry<Long, MaterializedIndex> entry : indexMap.entrySet()) {
            long indexId = entry.getKey();
            MaterializedIndex index = entry.getValue();
            MaterializedIndexMeta indexMeta = table.getIndexIdToMeta().get(indexId);

            // create tablets
            TabletMeta tabletMeta =
                    new TabletMeta(db.getId(), table.getId(), partitionId, indexId, indexMeta.getSchemaHash(),
                            storageMedium);
            createTablets(db.getClusterName(), index, Replica.ReplicaState.NORMAL, distributionInfo,
                    partition.getVisibleVersion(),
                    replicationNum, tabletMeta, tabletIdSet);
            if (index.getId() != table.getBaseIndexId()) {
                // add rollup index to partition
                partition.createRollupIndex(index);
            }
        }
        return partition;
    }

    // private
    private void createTablets(String clusterName, MaterializedIndex index, Replica.ReplicaState replicaState,
                               DistributionInfo distributionInfo, long version, short replicationNum,
                               TabletMeta tabletMeta, Set<Long> tabletIdSet) throws DdlException {
        Preconditions.checkArgument(replicationNum > 0);

        DistributionInfo.DistributionInfoType distributionInfoType = distributionInfo.getType();
        if (distributionInfoType != DistributionInfo.DistributionInfoType.HASH) {
            throw new DdlException("Unknown distribution type: " + distributionInfoType);
        }

        if (tabletMeta.isUseStarOS()) {
            // TODO: support colocate table and drop shards when creating table failed
            int bucketNum = distributionInfo.getBucketNum();
            List<Long> shardIds = GlobalStateMgr.getCurrentState().getStarOSAgent().createShards(bucketNum);
            Preconditions.checkState(bucketNum == shardIds.size());
            for (long shardId : shardIds) {
                Tablet tablet = new StarOSTablet(getNextId(), shardId);
                index.addTablet(tablet, tabletMeta);
                tabletIdSet.add(tablet.getId());
            }
        } else {
            ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentColocateIndex();
            List<List<Long>> backendsPerBucketSeq = null;
            ColocateTableIndex.GroupId groupId = null;
            if (colocateIndex.isColocateTable(tabletMeta.getTableId())) {
                // if this is a colocate table, try to get backend seqs from colocation index.
                Database db = getDb(tabletMeta.getDbId());
                groupId = colocateIndex.getGroup(tabletMeta.getTableId());
                // Use db write lock here to make sure the backendsPerBucketSeq is consistent when the backendsPerBucketSeq is updating.
                // This lock will release very fast.
                db.writeLock();
                try {
                    backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeq(groupId);
                } finally {
                    db.writeUnlock();
                }
            }

            // chooseBackendsArbitrary is true, means this may be the first table of colocation group,
            // or this is just a normal table, and we can choose backends arbitrary.
            // otherwise, backends should be chosen from backendsPerBucketSeq;
            boolean chooseBackendsArbitrary = backendsPerBucketSeq == null || backendsPerBucketSeq.isEmpty();
            if (chooseBackendsArbitrary) {
                backendsPerBucketSeq = Lists.newArrayList();
            }
            for (int i = 0; i < distributionInfo.getBucketNum(); ++i) {
                // create a new tablet with random chosen backends
                // TODO
                LocalTablet tablet = new LocalTablet(getNextId());

                // add tablet to inverted index first
                index.addTablet(tablet, tabletMeta);
                tabletIdSet.add(tablet.getId());

                // get BackendIds
                List<Long> chosenBackendIds;
                if (chooseBackendsArbitrary) {
                    // This is the first colocate table in the group, or just a normal table,
                    // randomly choose backends
                    if (Config.enable_strict_storage_medium_check) {
                        chosenBackendIds =
                                chosenBackendIdBySeq(replicationNum, clusterName, tabletMeta.getStorageMedium());
                    } else {
                        chosenBackendIds = chosenBackendIdBySeq(replicationNum, clusterName);
                    }
                    backendsPerBucketSeq.add(chosenBackendIds);
                } else {
                    // get backends from existing backend sequence
                    chosenBackendIds = backendsPerBucketSeq.get(i);
                }

                // create replicas
                for (long backendId : chosenBackendIds) {
                    // TODO
                    long replicaId = getNextId();
                    Replica replica = new Replica(replicaId, backendId, replicaState, version,
                            tabletMeta.getOldSchemaHash());
                    tablet.addReplica(replica);
                }
                Preconditions.checkState(chosenBackendIds.size() == replicationNum,
                        chosenBackendIds.size() + " vs. " + replicationNum);
            }

            if (groupId != null && chooseBackendsArbitrary) {
                colocateIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
                ColocatePersistInfo info =
                        ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
                editLog.logColocateBackendsPerBucketSeq(info);
            }
        }
    }

    // private
    private List<Long> chosenBackendIdBySeq(int replicationNum, String clusterName, TStorageMedium storageMedium)
            throws DdlException {
        // TODO
        List<Long> chosenBackendIds = GlobalStateMgr.getCurrentState().getClusterInfo().seqChooseBackendIdsByStorageMedium(replicationNum,
                true, true, clusterName, storageMedium);
        if (chosenBackendIds == null) {
            throw new DdlException(
                    "Failed to find enough host with storage medium is " + storageMedium + " in all backends. need: " +
                            replicationNum);
        }
        return chosenBackendIds;
    }

    // private
    private List<Long> chosenBackendIdBySeq(int replicationNum, String clusterName) throws DdlException {
        // TODO
        List<Long> chosenBackendIds =
                GlobalStateMgr.getCurrentSystemInfo().seqChooseBackendIds(replicationNum, true, true, clusterName);
        if (chosenBackendIds == null) {
            throw new DdlException("Failed to find enough host in all backends. need: " + replicationNum);
        }
        return chosenBackendIds;
    }

    // override
    @Override
    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        try {
            Database db = getDb(stmt.getExistedDbName());
            List<String> createTableStmt = Lists.newArrayList();
            db.readLock();
            try {
                Table table = db.getTable(stmt.getExistedTableName());
                if (table == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, stmt.getExistedTableName());
                }
                getDdlStmt(stmt.getDbName(), table, createTableStmt, null, null, false, false);
                if (createTableStmt.isEmpty()) {
                    ErrorReport.reportDdlException(ErrorCode.ERROR_CREATE_TABLE_LIKE_EMPTY, "CREATE");
                }
            } finally {
                db.readUnlock();
            }
            StatementBase statementBase =
                    SqlParserUtils.parseAndAnalyzeStmt(createTableStmt.get(0), ConnectContext.get());
            if (statementBase instanceof CreateTableStmt) {
                CreateTableStmt parsedCreateTableStmt =
                        (CreateTableStmt) SqlParserUtils
                                .parseAndAnalyzeStmt(createTableStmt.get(0), ConnectContext.get());
                parsedCreateTableStmt.setTableName(stmt.getTableName());
                if (stmt.isSetIfNotExists()) {
                    parsedCreateTableStmt.setIfNotExists();
                }
                createTable(parsedCreateTableStmt);
            } else if (statementBase instanceof CreateViewStmt) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_CREATE_TABLE_LIKE_UNSUPPORTED_VIEW);
            }
        } catch (UserException e) {
            throw new DdlException("Failed to execute CREATE TABLE LIKE " + stmt.getExistedTableName() + ". Reason: " +
                    e.getMessage());
        }
    }

    // override or special
    @Override
    public void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {
        PartitionDesc partitionDesc = addPartitionClause.getPartitionDesc();
        if (partitionDesc instanceof SingleRangePartitionDesc) {
            addPartitions(db, tableName, ImmutableList.of((SingleRangePartitionDesc) partitionDesc),
                    addPartitionClause);
        } else if (partitionDesc instanceof MultiRangePartitionDesc) {
            db.readLock();
            RangePartitionInfo rangePartitionInfo;
            Map<String, String> tableProperties;
            try {
                Table table = db.getTable(tableName);
                CatalogUtils.checkTableExist(db, tableName);
                CatalogUtils.checkTableTypeOLAP(db, table);
                OlapTable olapTable = (OlapTable) table;
                tableProperties = olapTable.getTableProperty().getProperties();
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            } finally {
                db.readUnlock();
            }

            if (rangePartitionInfo == null) {
                throw new DdlException("Alter batch get partition info failed.");
            }

            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            if (partitionColumns.size() != 1) {
                throw new DdlException("Alter batch build partition only support single range column.");
            }

            Column firstPartitionColumn = partitionColumns.get(0);
            MultiRangePartitionDesc multiRangePartitionDesc = (MultiRangePartitionDesc) partitionDesc;
            Map<String, String> properties = addPartitionClause.getProperties();
            if (properties == null) {
                properties = Maps.newHashMap();
            }
            if (tableProperties != null && tableProperties.containsKey(DynamicPartitionProperty.START_DAY_OF_WEEK)) {
                properties.put(DynamicPartitionProperty.START_DAY_OF_WEEK,
                        tableProperties.get(DynamicPartitionProperty.START_DAY_OF_WEEK));
            }
            List<SingleRangePartitionDesc> singleRangePartitionDescs = multiRangePartitionDesc
                    .convertToSingle(firstPartitionColumn.getType(), properties);
            addPartitions(db, tableName, singleRangePartitionDescs, addPartitionClause);
        }
    }

    // private
    public void addPartitions(Database db, String tableName, List<SingleRangePartitionDesc> singleRangePartitionDescs,
                              AddPartitionClause addPartitionClause) throws DdlException {
        DistributionInfo distributionInfo;
        OlapTable olapTable;
        OlapTable copiedTable;

        boolean isTempPartition = addPartitionClause.isTempPartition();
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            CatalogUtils.checkTableExist(db, tableName);
            CatalogUtils.checkTableTypeOLAP(db, table);
            olapTable = (OlapTable) table;
            CatalogUtils.checkTableState(olapTable, tableName);
            // check partition type
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (partitionInfo.getType() != PartitionType.RANGE) {
                throw new DdlException("Only support adding partition to range partitioned table");
            }
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            Set<String> existPartitionNameSet =
                    CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, singleRangePartitionDescs);
            // partition properties is prior to clause properties
            // clause properties is prior to table properties
            Map<String, String> properties = Maps.newHashMap();
            properties = getOrSetDefaultProperties(olapTable, properties);
            Map<String, String> clauseProperties = addPartitionClause.getProperties();
            if (clauseProperties != null && !clauseProperties.isEmpty()) {
                properties.putAll(clauseProperties);
            }
            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                Map<String, String> cloneProperties = Maps.newHashMap(properties);
                Map<String, String> sourceProperties = singleRangePartitionDesc.getProperties();
                if (sourceProperties != null && !sourceProperties.isEmpty()) {
                    cloneProperties.putAll(sourceProperties);
                }
                singleRangePartitionDesc.analyze(rangePartitionInfo.getPartitionColumns().size(), cloneProperties);
                if (!existPartitionNameSet.contains(singleRangePartitionDesc.getPartitionName())) {
                    rangePartitionInfo.checkAndCreateRange(singleRangePartitionDesc, isTempPartition);
                }
            }

            // get distributionInfo
            List<Column> baseSchema = olapTable.getBaseSchema();
            DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
            DistributionDesc distributionDesc = addPartitionClause.getDistributionDesc();
            if (distributionDesc != null) {
                distributionInfo = distributionDesc.toDistributionInfo(baseSchema);
                // for now. we only support modify distribution's bucket num
                if (distributionInfo.getType() != defaultDistributionInfo.getType()) {
                    throw new DdlException("Cannot assign different distribution type. default is: "
                            + defaultDistributionInfo.getType());
                }

                if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH) {
                    HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                    List<Column> newDistriCols = hashDistributionInfo.getDistributionColumns();
                    List<Column> defaultDistriCols = ((HashDistributionInfo) defaultDistributionInfo)
                            .getDistributionColumns();
                    if (!newDistriCols.equals(defaultDistriCols)) {
                        throw new DdlException("Cannot assign hash distribution with different distribution cols. "
                                + "default is: " + defaultDistriCols);
                    }
                    if (hashDistributionInfo.getBucketNum() <= 0) {
                        throw new DdlException("Cannot assign hash distribution buckets less than 1");
                    }
                }
            } else {
                distributionInfo = defaultDistributionInfo;
            }

            // check colocation
            if (GlobalStateMgr.getCurrentColocateIndex().isColocateTable(olapTable.getId())) {
                String fullGroupName = db.getId() + "_" + olapTable.getColocateGroup();

                ColocateGroupSchema groupSchema = GlobalStateMgr.getCurrentColocateIndex().getGroupSchema(fullGroupName);
                Preconditions.checkNotNull(groupSchema);
                groupSchema.checkDistribution(distributionInfo);
                for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                    groupSchema.checkReplicationNum(singleRangePartitionDesc.getReplicationNum());
                }
            }

            copiedTable = olapTable.selectiveCopy(null, false, MaterializedIndex.IndexExtState.VISIBLE);
            copiedTable.setDefaultDistributionInfo(distributionInfo);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        } finally {
            db.readUnlock();
        }

        Preconditions.checkNotNull(distributionInfo);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkNotNull(copiedTable);

        // create partition outside db lock
        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            DataProperty dataProperty = singleRangePartitionDesc.getPartitionDataProperty();
            Preconditions.checkNotNull(dataProperty);
        }

        Set<Long> tabletIdSetForAll = Sets.newHashSet();
        HashMap<String, Set<Long>> partitionNameToTabletSet = Maps.newHashMap();
        try {
            List<Partition> partitionList = Lists.newArrayListWithCapacity(singleRangePartitionDescs.size());

            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                long partitionId = getNextId();
                DataProperty dataProperty = singleRangePartitionDesc.getPartitionDataProperty();
                String partitionName = singleRangePartitionDesc.getPartitionName();
                Long version = singleRangePartitionDesc.getVersionInfo();
                Set<Long> tabletIdSet = Sets.newHashSet();

                copiedTable.getPartitionInfo().setDataProperty(partitionId, dataProperty);
                copiedTable.getPartitionInfo().setTabletType(partitionId, singleRangePartitionDesc.getTabletType());
                copiedTable.getPartitionInfo()
                        .setReplicationNum(partitionId, singleRangePartitionDesc.getReplicationNum());
                copiedTable.getPartitionInfo().setIsInMemory(partitionId, singleRangePartitionDesc.isInMemory());

                Partition partition =
                        createPartition(db, copiedTable, partitionId, partitionName, version, tabletIdSet);

                partitionList.add(partition);
                tabletIdSetForAll.addAll(tabletIdSet);
                partitionNameToTabletSet.put(partitionName, tabletIdSet);
            }

            buildPartitions(db, copiedTable, partitionList);

            // check again
            db.writeLock();
            Set<String> existPartitionNameSet = Sets.newHashSet();
            try {
                CatalogUtils.checkTableExist(db, tableName);
                Table table = db.getTable(tableName);
                CatalogUtils.checkTableTypeOLAP(db, table);
                olapTable = (OlapTable) table;
                CatalogUtils.checkTableState(olapTable, tableName);
                existPartitionNameSet = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable,
                        singleRangePartitionDescs);

                if (existPartitionNameSet.size() > 0) {
                    for (String partitionName : existPartitionNameSet) {
                        LOG.info("add partition[{}] which already exists", partitionName);
                    }
                }

                // check if meta changed
                // rollup index may be added or dropped during add partition operation.
                // schema may be changed during add partition operation.
                boolean metaChanged = false;
                if (olapTable.getIndexNameToId().size() != copiedTable.getIndexNameToId().size()) {
                    metaChanged = true;
                } else {
                    // compare schemaHash
                    for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getIndexIdToMeta().entrySet()) {
                        long indexId = entry.getKey();
                        if (!copiedTable.getIndexIdToMeta().containsKey(indexId)) {
                            metaChanged = true;
                            break;
                        }
                        if (copiedTable.getIndexIdToMeta().get(indexId).getSchemaHash() !=
                                entry.getValue().getSchemaHash()) {
                            metaChanged = true;
                            break;
                        }
                    }
                }

                if (metaChanged) {
                    throw new DdlException("Table[" + tableName + "]'s meta has been changed. try again.");
                }

                // check partition type
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                if (partitionInfo.getType() != PartitionType.RANGE) {
                    throw new DdlException("Only support adding partition to range partitioned table");
                }

                // update partition info
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                rangePartitionInfo.handleNewRangePartitionDescs(singleRangePartitionDescs,
                        partitionList, existPartitionNameSet, isTempPartition);

                if (isTempPartition) {
                    for (Partition partition : partitionList) {
                        if (!existPartitionNameSet.contains(partition.getName())) {
                            olapTable.addTempPartition(partition);
                        }
                    }
                } else {
                    for (Partition partition : partitionList) {
                        if (!existPartitionNameSet.contains(partition.getName())) {
                            olapTable.addPartition(partition);
                        }
                    }
                }

                // log
                int partitionLen = partitionList.size();
                // Forward compatible with previous log formats
                // Version 1.15 is compatible if users only use single-partition syntax.
                // Otherwise, the followers will be crash when reading the new log
                if (partitionLen == 1) {
                    Partition partition = partitionList.get(0);
                    if (existPartitionNameSet.contains(partition.getName())) {
                        LOG.info("add partition[{}] which already exists", partition.getName());
                        return;
                    }
                    long partitionId = partition.getId();
                    PartitionPersistInfo info = new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                            rangePartitionInfo.getRange(partitionId),
                            singleRangePartitionDescs.get(0).getPartitionDataProperty(),
                            rangePartitionInfo.getReplicationNum(partitionId),
                            rangePartitionInfo.getIsInMemory(partitionId),
                            isTempPartition);
                    editLog.logAddPartition(info);

                    LOG.info("succeed in creating partition[{}], name: {}, temp: {}", partitionId,
                            partition.getName(), isTempPartition);
                } else {
                    List<PartitionPersistInfo> partitionInfoList = Lists.newArrayListWithCapacity(partitionLen);
                    for (int i = 0; i < partitionLen; i++) {
                        Partition partition = partitionList.get(i);
                        if (!existPartitionNameSet.contains(partition.getName())) {
                            PartitionPersistInfo info =
                                    new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                                            rangePartitionInfo.getRange(partition.getId()),
                                            singleRangePartitionDescs.get(i).getPartitionDataProperty(),
                                            rangePartitionInfo.getReplicationNum(partition.getId()),
                                            rangePartitionInfo.getIsInMemory(partition.getId()),
                                            isTempPartition);
                            partitionInfoList.add(info);
                        }
                    }

                    AddPartitionsInfo infos = new AddPartitionsInfo(partitionInfoList);
                    editLog.logAddPartitions(infos);

                    for (Partition partition : partitionList) {
                        LOG.info("succeed in creating partitions[{}], name: {}, temp: {}", partition.getId(),
                                partition.getName(), isTempPartition);
                    }
                }
            } finally {
                for (String partitionName : existPartitionNameSet) {
                    Set<Long> existPartitionTabletSet = partitionNameToTabletSet.get(partitionName);
                    if (existPartitionTabletSet == null) {
                        // should not happen
                        continue;
                    }
                    for (Long tabletId : existPartitionTabletSet) {
                        // createPartitionWithIndices create duplicate tablet that if not exists scenario
                        // so here need to clean up those created tablets which partition already exists from invert index
                        GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
                    }
                }
                db.writeUnlock();
            }
        } catch (DdlException e) {
            for (Long tabletId : tabletIdSetForAll) {
                GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            throw e;
        }
    }

    // private
    private Map<String, String> getOrSetDefaultProperties(OlapTable olapTable, Map<String, String> sourceProperties) {
        // partition properties should inherit table properties
        Short replicationNum = olapTable.getDefaultReplicationNum();
        if (sourceProperties == null) {
            sourceProperties = Maps.newConcurrentMap();
        }
        if (!sourceProperties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
            sourceProperties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, replicationNum.toString());
        }
        if (!sourceProperties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
            sourceProperties.put(PropertyAnalyzer.PROPERTIES_INMEMORY, olapTable.isInMemory().toString());
        }
        Map<String, String> tableProperty = olapTable.getTableProperty().getProperties();
        if (tableProperty != null && tableProperty.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
            sourceProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM,
                    tableProperty.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM));
        }
        return sourceProperties;
    }

    public static void getDdlStmt(Table table, List<String> createTableStmt, List<String> addPartitionStmt,
                                  List<String> createRollupStmt, boolean separatePartition,
                                  boolean hidePassword) {
        getDdlStmt(null, table, createTableStmt, addPartitionStmt, createRollupStmt, separatePartition, hidePassword);
    }

    // special
    public static void getDdlStmt(String dbName, Table table, List<String> createTableStmt,
                                  List<String> addPartitionStmt,
                                  List<String> createRollupStmt, boolean separatePartition, boolean hidePassword) {
        StringBuilder sb = new StringBuilder();

        // 1. create table
        // 1.1 view
        if (table.getType() == Table.TableType.VIEW) {
            View view = (View) table;
            sb.append("CREATE VIEW `").append(table.getName()).append("` (");
            List<String> colDef = Lists.newArrayList();
            for (Column column : table.getBaseSchema()) {
                StringBuilder colSb = new StringBuilder();
                colSb.append(column.getName());
                if (!Strings.isNullOrEmpty(column.getComment())) {
                    colSb.append(" COMMENT ").append("\"").append(column.getComment()).append("\"");
                }
                colDef.add(colSb.toString());
            }
            sb.append(Joiner.on(", ").join(colDef));
            sb.append(")");
            if (!Strings.isNullOrEmpty(view.getComment())) {
                sb.append(" COMMENT \"").append(view.getComment()).append("\"");
            }
            sb.append(" AS ").append(view.getInlineViewDef()).append(";");
            createTableStmt.add(sb.toString());
            return;
        }

        // 1.2 other table type
        sb.append("CREATE ");
        if (table.getType() == Table.TableType.MYSQL || table.getType() == Table.TableType.ELASTICSEARCH
                || table.getType() == Table.TableType.BROKER || table.getType() == Table.TableType.HIVE
                || table.getType() == Table.TableType.OLAP_EXTERNAL || table.getType() == Table.TableType.JDBC) {
            sb.append("EXTERNAL ");
        }
        sb.append("TABLE ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("`").append(dbName).append("`.");
        }
        sb.append("`").append(table.getName()).append("` (\n");
        int idx = 0;
        for (Column column : table.getBaseSchema()) {
            if (idx++ != 0) {
                sb.append(",\n");
            }
            // There MUST BE 2 space in front of each column description line
            // sqlalchemy requires this to parse SHOW CREATE TAEBL stmt.
            if (table.getType() == Table.TableType.OLAP || table.getType() == Table.TableType.OLAP_EXTERNAL) {
                OlapTable olapTable = (OlapTable) table;
                if (olapTable.getKeysType() == KeysType.PRIMARY_KEYS) {
                    sb.append("  ").append(column.toSqlWithoutAggregateTypeName());
                } else {
                    sb.append("  ").append(column.toSql());
                }
            } else {
                sb.append("  ").append(column.toSql());
            }
        }
        if (table.getType() == Table.TableType.OLAP || table.getType() == Table.TableType.OLAP_EXTERNAL) {
            OlapTable olapTable = (OlapTable) table;
            if (CollectionUtils.isNotEmpty(olapTable.getIndexes())) {
                for (Index index : olapTable.getIndexes()) {
                    sb.append(",\n");
                    sb.append("  ").append(index.toSql());
                }
            }
        }
        sb.append("\n) ENGINE=");
        sb.append(table.getType().name()).append(" ");

        if (table.getType() == Table.TableType.OLAP || table.getType() == Table.TableType.OLAP_EXTERNAL) {
            OlapTable olapTable = (OlapTable) table;

            // keys
            sb.append("\n").append(olapTable.getKeysType().toSql()).append("(");
            List<String> keysColumnNames = Lists.newArrayList();
            for (Column column : olapTable.getBaseSchema()) {
                if (column.isKey()) {
                    keysColumnNames.add("`" + column.getName() + "`");
                }
            }
            sb.append(Joiner.on(", ").join(keysColumnNames)).append(")");

            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // partition
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            List<Long> partitionId = null;
            if (separatePartition) {
                partitionId = Lists.newArrayList();
            }
            if (partitionInfo.getType() == PartitionType.RANGE) {
                sb.append("\n").append(partitionInfo.toSql(olapTable, partitionId));
            }

            // distribution
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            sb.append("\n").append(distributionInfo.toSql());

            // properties
            sb.append("\nPROPERTIES (\n");

            // replicationNum
            Short replicationNum = olapTable.getDefaultReplicationNum();
            sb.append("\"").append(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM).append("\" = \"");
            sb.append(replicationNum).append("\"");

            // bloom filter
            Set<String> bfColumnNames = olapTable.getCopiedBfColumns();
            if (bfColumnNames != null) {
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BF_COLUMNS).append("\" = \"");
                sb.append(Joiner.on(", ").join(olapTable.getCopiedBfColumns())).append("\"");
            }

            if (separatePartition) {
                // version info
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_VERSION_INFO).append("\" = \"");
                Partition partition = null;
                if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                    partition = olapTable.getPartition(olapTable.getName());
                } else {
                    Preconditions.checkState(partitionId.size() == 1);
                    partition = olapTable.getPartition(partitionId.get(0));
                }
                sb.append(partition.getVisibleVersion()).append("\"");
            }

            // colocateTable
            String colocateTable = olapTable.getColocateGroup();
            if (colocateTable != null) {
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH).append("\" = \"");
                sb.append(colocateTable).append("\"");
            }

            // dynamic partition
            if (olapTable.dynamicPartitionExists()) {
                sb.append(olapTable.getTableProperty().getDynamicPartitionProperty().toString());
            }

            // in memory
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_INMEMORY).append("\" = \"");
            sb.append(olapTable.isInMemory()).append("\"");

            // storage type
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT).append("\" = \"");
            sb.append(olapTable.getStorageFormat()).append("\"");

            // storage media
            Map<String, String> properties = olapTable.getTableProperty().getProperties();
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
                sb.append("\n");
            } else {
                sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM).append("\" = \"");
                sb.append(properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)).append("\"");
                sb.append("\n");
            }

            if (table.getType() == Table.TableType.OLAP_EXTERNAL) {
                ExternalOlapTable externalOlapTable = (ExternalOlapTable) table;
                // properties
                sb.append("\"host\" = \"").append(externalOlapTable.getSourceTableHost()).append("\",\n");
                sb.append("\"port\" = \"").append(externalOlapTable.getSourceTablePort()).append("\",\n");
                sb.append("\"user\" = \"").append(externalOlapTable.getSourceTableUser()).append("\",\n");
                sb.append("\"password\" = \"").append(hidePassword ? "" : externalOlapTable.getSourceTablePassword())
                        .append("\",\n");
                sb.append("\"database\" = \"").append(externalOlapTable.getSourceTableDbName()).append("\",\n");
                sb.append("\"table\" = \"").append(externalOlapTable.getSourceTableName()).append("\"\n");
            }
            sb.append(")");
        } else if (table.getType() == Table.TableType.MYSQL) {
            MysqlTable mysqlTable = (MysqlTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }
            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"host\" = \"").append(mysqlTable.getHost()).append("\",\n");
            sb.append("\"port\" = \"").append(mysqlTable.getPort()).append("\",\n");
            sb.append("\"user\" = \"").append(mysqlTable.getUserName()).append("\",\n");
            sb.append("\"password\" = \"").append(hidePassword ? "" : mysqlTable.getPasswd()).append("\",\n");
            sb.append("\"database\" = \"").append(mysqlTable.getMysqlDatabaseName()).append("\",\n");
            sb.append("\"table\" = \"").append(mysqlTable.getMysqlTableName()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == Table.TableType.BROKER) {
            BrokerTable brokerTable = (BrokerTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }
            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"broker_name\" = \"").append(brokerTable.getBrokerName()).append("\",\n");
            sb.append("\"path\" = \"").append(Joiner.on(",").join(brokerTable.getEncodedPaths())).append("\",\n");
            sb.append("\"column_separator\" = \"").append(brokerTable.getReadableColumnSeparator()).append("\",\n");
            sb.append("\"line_delimiter\" = \"").append(brokerTable.getReadableRowDelimiter()).append("\"\n");
            sb.append(")");
            if (!brokerTable.getBrokerProperties().isEmpty()) {
                sb.append("\nBROKER PROPERTIES (\n");
                sb.append(new PrintableMap<>(brokerTable.getBrokerProperties(), " = ", true, true,
                        hidePassword).toString());
                sb.append("\n)");
            }
        } else if (table.getType() == Table.TableType.ELASTICSEARCH) {
            EsTable esTable = (EsTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // partition
            PartitionInfo partitionInfo = esTable.getPartitionInfo();
            if (partitionInfo.getType() == PartitionType.RANGE) {
                sb.append("\n");
                sb.append("PARTITION BY RANGE(");
                idx = 0;
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                for (Column column : rangePartitionInfo.getPartitionColumns()) {
                    if (idx != 0) {
                        sb.append(", ");
                    }
                    sb.append("`").append(column.getName()).append("`");
                }
                sb.append(")\n()");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"hosts\" = \"").append(esTable.getHosts()).append("\",\n");
            sb.append("\"user\" = \"").append(esTable.getUserName()).append("\",\n");
            sb.append("\"password\" = \"").append(hidePassword ? "" : esTable.getPasswd()).append("\",\n");
            sb.append("\"index\" = \"").append(esTable.getIndexName()).append("\",\n");
            sb.append("\"type\" = \"").append(esTable.getMappingType()).append("\",\n");
            sb.append("\"transport\" = \"").append(esTable.getTransport()).append("\",\n");
            sb.append("\"enable_docvalue_scan\" = \"").append(esTable.isDocValueScanEnable()).append("\",\n");
            sb.append("\"max_docvalue_fields\" = \"").append(esTable.maxDocValueFields()).append("\",\n");
            sb.append("\"enable_keyword_sniff\" = \"").append(esTable.isKeywordSniffEnable()).append("\"\n");
            sb.append(")");
        } else if (table.getType() == Table.TableType.HIVE) {
            HiveTable hiveTable = (HiveTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"database\" = \"").append(hiveTable.getHiveDb()).append("\",\n");
            sb.append("\"table\" = \"").append(hiveTable.getHiveTable()).append("\",\n");
            sb.append("\"resource\" = \"").append(hiveTable.getResourceName()).append("\",\n");
            sb.append(new PrintableMap<>(hiveTable.getHiveProperties(), " = ", true, true, false).toString());
            sb.append("\n)");
        } else if (table.getType() == Table.TableType.JDBC) {
            JDBCTable jdbcTable = (JDBCTable) table;
            if (!Strings.isNullOrEmpty(table.getComment())) {
                sb.append("\nCOMMENT \"").append(table.getComment()).append("\"");
            }

            // properties
            sb.append("\nPROPERTIES (\n");
            sb.append("\"resource\" = \"").append(jdbcTable.getResourceName()).append("\",\n");
            sb.append("\"table\" = \"").append(jdbcTable.getJdbcTable()).append("\"");
            sb.append("\n)");
        }
        sb.append(";");

        createTableStmt.add(sb.toString());

        // 2. add partition
        if (separatePartition && (table instanceof OlapTable)
                && ((OlapTable) table).getPartitionInfo().getType() == PartitionType.RANGE
                && ((OlapTable) table).getPartitions().size() > 1) {
            OlapTable olapTable = (OlapTable) table;
            RangePartitionInfo partitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            boolean first = true;
            for (Map.Entry<Long, Range<PartitionKey>> entry : partitionInfo.getSortedRangeMap(false)) {
                if (first) {
                    first = false;
                    continue;
                }
                sb = new StringBuilder();
                Partition partition = olapTable.getPartition(entry.getKey());
                sb.append("ALTER TABLE ").append(table.getName());
                sb.append(" ADD PARTITION ").append(partition.getName()).append(" VALUES [");
                sb.append(entry.getValue().lowerEndpoint().toSql());
                sb.append(", ").append(entry.getValue().upperEndpoint().toSql()).append(")");
                sb.append("(\"version_info\" = \"");
                sb.append(partition.getVisibleVersion()).append("\"");
                sb.append(");");
                addPartitionStmt.add(sb.toString());
            }
        }

        // 3. rollup
        if (createRollupStmt != null && (table instanceof OlapTable)) {
            OlapTable olapTable = (OlapTable) table;
            for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getIndexIdToMeta().entrySet()) {
                if (entry.getKey() == olapTable.getBaseIndexId()) {
                    continue;
                }
                MaterializedIndexMeta materializedIndexMeta = entry.getValue();
                sb = new StringBuilder();
                String indexName = olapTable.getIndexNameById(entry.getKey());
                sb.append("ALTER TABLE ").append(table.getName()).append(" ADD ROLLUP ").append(indexName);
                sb.append("(");

                List<Column> indexSchema = materializedIndexMeta.getSchema();
                for (int i = 0; i < indexSchema.size(); i++) {
                    Column column = indexSchema.get(i);
                    sb.append(column.getName());
                    if (i != indexSchema.size() - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(");");
                createRollupStmt.add(sb.toString());
            }
        }
    }

    // special for editlog
    @Override
    public void replayAddPartition(PartitionPersistInfo info) throws DdlException {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            Partition partition = info.getPartition();

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (info.isTempPartition()) {
                olapTable.addTempPartition(partition);
            } else {
                olapTable.addPartition(partition);
            }

            ((RangePartitionInfo) partitionInfo).unprotectHandleNewSinglePartitionDesc(partition.getId(),
                    info.isTempPartition(), info.getRange(), info.getDataProperty(), info.getReplicationNum(),
                    info.isInMemory());
            // TODO
            GlobalStateMgr.getCurrentState();
            if (!isCheckpointThread()) {
                // add to inverted index
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
                for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    long indexId = index.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    TabletMeta tabletMeta = new TabletMeta(info.getDbId(), info.getTableId(), partition.getId(),
                            index.getId(), schemaHash, info.getDataProperty().getStorageMedium());
                    for (Tablet tablet : index.getTablets()) {
                        long tabletId = tablet.getId();
                        invertedIndex.addTablet(tabletId, tabletMeta);
                        for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                            invertedIndex.addReplica(tabletId, replica);
                        }
                    }
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    // override or gai
    @Override
    public void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        OlapTable olapTable = (OlapTable) table;

        String partitionName = clause.getPartitionName();
        boolean isTempPartition = clause.isTempPartition();

        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s state is not NORMAL");
        }

        if (!olapTable.checkPartitionNameExist(partitionName, isTempPartition)) {
            if (clause.isSetIfExists()) {
                LOG.info("drop partition[{}] which does not exist", partitionName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DROP_PARTITION_NON_EXISTENT, partitionName);
            }
        }

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.getType() != PartitionType.RANGE) {
            throw new DdlException("Alter table [" + olapTable.getName() + "] failed. Not a partitioned table");
        }

        // drop
        if (isTempPartition) {
            olapTable.dropTempPartition(partitionName, true);
        } else {
            if (!clause.isForceDrop()) {
                Partition partition = olapTable.getPartition(partitionName);
                if (partition != null) {
                    if (GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                            .existCommittedTxns(db.getId(), olapTable.getId(), partition.getId())) {
                        throw new DdlException(
                                "There are still some transactions in the COMMITTED state waiting to be completed." +
                                        " The partition [" + partitionName +
                                        "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                        " please use \"DROP partition FORCE\".");
                    }
                }
            }
            olapTable.dropPartition(db.getId(), partitionName, clause.isForceDrop());
        }

        // log
        DropPartitionInfo info = new DropPartitionInfo(db.getId(), olapTable.getId(), partitionName, isTempPartition,
                clause.isForceDrop());
        editLog.logDropPartition(info);

        LOG.info("succeed in droping partition[{}], is temp : {}, is force : {}", partitionName, isTempPartition,
                clause.isForceDrop());
    }

    // special
    @Override
    public void replayDropPartition(DropPartitionInfo info) {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            if (info.isTempPartition()) {
                olapTable.dropTempPartition(info.getPartitionName(), true);
            } else {
                olapTable.dropPartition(info.getDbId(), info.getPartitionName(), info.isForceDrop());
            }
        } finally {
            db.writeUnlock();
        }
    }

    // move it to RecycleBin
    public void replayErasePartition(long partitionId) throws DdlException {
        GlobalStateMgr.getCurrentRecycleBin().replayErasePartition(partitionId);
    }

    // move it to RecycleBin
    public void replayRecoverPartition(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = getDb(dbId);
        db.writeLock();
        try {
            Table table = db.getTable(info.getTableId());
            GlobalStateMgr.getCurrentRecycleBin().replayRecoverPartition((OlapTable) table, info.getPartitionId());
        } finally {
            db.writeUnlock();
        }
    }

    // special
    @Override
    public void replayCreateTable(String dbName, Table table) {
        Database db = this.fullNameToDb.get(dbName);
        db.createTableWithLock(table, true);

        if (!isCheckpointThread()) {
            // add to inverted index
            if (table.getType() == Table.TableType.OLAP) {
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
                OlapTable olapTable = (OlapTable) table;
                long dbId = db.getId();
                long tableId = table.getId();
                for (Partition partition : olapTable.getAllPartitions()) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    boolean useStarOS = partition.isUseStarOS();
                    for (MaterializedIndex mIndex : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        long indexId = mIndex.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium);
                        for (Tablet tablet : mIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            if (!useStarOS) {
                                for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                                    invertedIndex.addReplica(tabletId, replica);
                                }
                            }
                        }
                    }
                } // end for partitions
                DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(dbId, olapTable);
            }
        }
    }

    // override
    // Drop table
    @Override
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        // check database
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        Table table = null;
        db.writeLock();
        try {
            table = db.getTable(tableName);
            if (table == null) {
                if (stmt.isSetIfExists()) {
                    LOG.info("drop table[{}] which does not exist", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                }
            }

            // Check if a view
            if (stmt.isView()) {
                if (!(table instanceof View)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_OBJECT, dbName, tableName, "VIEW");
                }
            } else {
                if (table instanceof View) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_OBJECT, dbName, tableName, "TABLE");
                }
            }

            if (!stmt.isForceDrop()) {
                if (GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                        .existCommittedTxns(db.getId(), table.getId(), null)) {
                    throw new DdlException(
                            "There are still some transactions in the COMMITTED state waiting to be completed. " +
                                    "The table [" + tableName +
                                    "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                    " please use \"DROP table FORCE\".");
                }
            }
            unprotectDropTable(db, table.getId(), stmt.isForceDrop(), false);
            DropInfo info = new DropInfo(db.getId(), table.getId(), -1L, stmt.isForceDrop());
            editLog.logDropTable(info);
        } finally {
            db.writeUnlock();
        }

        LOG.info("finished dropping table: {} from db: {}, is force: {}", tableName, dbName, stmt.isForceDrop());
    }

    // private
    public boolean unprotectDropTable(Database db, long tableId, boolean isForceDrop, boolean isReplay) {
        Table table = db.getTable(tableId);
        // delete from db meta
        if (table == null) {
            return false;
        }

        table.onDrop();

        db.dropTable(table.getName());
        if (!isForceDrop) {
            GlobalStateMgr.getCurrentRecycleBin().recycleTable(db.getId(), table);
        } else {
            if (table.getType() == Table.TableType.OLAP) {
                onEraseOlapTable((OlapTable) table, isReplay);
            }
        }

        LOG.info("finished dropping table[{}] in db[{}], tableId: {}", table.getName(), db.getFullName(),
                table.getId());
        return true;
    }

    // special
    @Override
    public void replayDropTable(Database db, long tableId, boolean isForceDrop) {
        db.writeLock();
        try {
            unprotectDropTable(db, tableId, isForceDrop, true);
        } finally {
            db.writeUnlock();
        }
    }

    // move it to RecycleBin
    public void replayEraseTable(long tableId) throws DdlException {
        GlobalStateMgr.getCurrentRecycleBin().replayEraseTable(tableId);
    }

    // private
    // move it to RecycleBin
    public void replayEraseMultiTables(MultiEraseTableInfo multiEraseTableInfo) throws DdlException {
        List<Long> tableIds = multiEraseTableInfo.getTableIds();
        for (Long tableId : tableIds) {
            GlobalStateMgr.getCurrentRecycleBin().replayEraseTable(tableId);
        }
    }

    // special
    @Override
    public void replayRecoverTable(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = getDb(dbId);
        db.writeLock();
        try {
            GlobalStateMgr.getCurrentRecycleBin().replayRecoverTable(db, info.getTableId());
        } finally {
            db.writeUnlock();
        }
    }

    // override or special
    public Database getDbIncludeRecycleBin(long dbId) {
        Database db = idToDb.get(dbId);
        // TODO
        if (db == null) {
            db = GlobalStateMgr.getCurrentRecycleBin().getDatabase(dbId);
        }
        return db;
    }

    // override or special
    public Table getTableIncludeRecycleBin(Database db, long tableId) {
        Table table = db.getTable(tableId);
        if (table == null) {
            table = GlobalStateMgr.getCurrentRecycleBin().getTable(db.getId(), tableId);
        }
        return table;
    }

    // special
    public List<Table> getTablesIncludeRecycleBin(Database db) {
        List<Table> tables = db.getTables();
        tables.addAll(GlobalStateMgr.getCurrentRecycleBin().getTables(db.getId()));
        return tables;
    }

    // private
    private void unprotectAddReplica(ReplicaPersistInfo info) {
        LOG.debug("replay add a replica {}", info);
        Database db = getDbIncludeRecycleBin(info.getDbId());
        OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
        Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());

        // for compatibility
        int schemaHash = info.getSchemaHash();
        if (schemaHash == -1) {
            schemaHash = olapTable.getSchemaHashByIndexId(info.getIndexId());
        }

        Replica replica = new Replica(info.getReplicaId(), info.getBackendId(), info.getVersion(),
                schemaHash, info.getDataSize(), info.getRowCount(),
                Replica.ReplicaState.NORMAL,
                info.getLastFailedVersion(),
                info.getLastSuccessVersion());
        tablet.addReplica(replica);
    }

    // override or special
    public Partition getPartitionIncludeRecycleBin(OlapTable table, long partitionId) {
        Partition partition = table.getPartition(partitionId);
        if (partition == null) {
            partition = GlobalStateMgr.getCurrentRecycleBin().getPartition(partitionId);
        }
        return partition;
    }

    // private
    private void unprotectUpdateReplica(ReplicaPersistInfo info) {
        LOG.debug("replay update a replica {}", info);
        Database db = getDbIncludeRecycleBin(info.getDbId());
        OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
        Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());
        Replica replica = tablet.getReplicaByBackendId(info.getBackendId());
        Preconditions.checkNotNull(replica, info);
        replica.updateRowCount(info.getVersion(), info.getDataSize(), info.getRowCount());
        replica.setBad(false);
    }

    // special or private
    @Override
    public void replayAddReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        db.writeLock();
        try {
            unprotectAddReplica(info);
        } finally {
            db.writeUnlock();
        }
    }

    // special for editlog
    @Override
    public void replayUpdateReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        db.writeLock();
        try {
            unprotectUpdateReplica(info);
        } finally {
            db.writeUnlock();
        }
    }

    // private
    public void unprotectDeleteReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
        Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());
        tablet.deleteReplicaByBackendId(info.getBackendId());
    }

    // special for editlog
    @Override
    public void replayDeleteReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        db.writeLock();
        try {
            unprotectDeleteReplica(info);
        } finally {
            db.writeUnlock();
        }
    }

    // TODO
    public Collection<Partition> getPartitionsIncludeRecycleBin(OlapTable table) {
        Collection<Partition> partitions = new ArrayList<>(table.getPartitions());
        partitions.addAll(GlobalStateMgr.getCurrentRecycleBin().getPartitions(table.getId()));
        return partitions;
    }

    public Collection<Partition> getAllPartitionsIncludeRecycleBin(OlapTable table) {
        Collection<Partition> partitions = table.getAllPartitions();
        partitions.addAll(GlobalStateMgr.getCurrentRecycleBin().getPartitions(table.getId()));
        return partitions;
    }

    // private
    // NOTE: result can be null, cause partition erase is not in db lock
    public DataProperty getDataPropertyIncludeRecycleBin(PartitionInfo info, long partitionId) {
        DataProperty dataProperty = info.getDataProperty(partitionId);
        if (dataProperty == null) {
            dataProperty = GlobalStateMgr.getCurrentRecycleBin().getPartitionDataProperty(partitionId);
        }
        return dataProperty;
    }

    // private
    // NOTE: result can be -1, cause partition erase is not in db lock
    public short getReplicationNumIncludeRecycleBin(PartitionInfo info, long partitionId) {
        short replicaNum = info.getReplicationNum(partitionId);
        if (replicaNum == (short) -1) {
            replicaNum = GlobalStateMgr.getCurrentRecycleBin().getPartitionReplicationNum(partitionId);
        }
        return replicaNum;
    }

    @Override
    public List<String> listTableNames(String dbName) {
        Database db = getDb(dbName);
        return db.getTables().stream().map(Table::getName).collect(Collectors.toList());
    }

    // override
    @Override
    public List<String> listDatabaseNames() {
        return Lists.newArrayList(fullNameToDb.keySet());
    }

    // special for internal discuss
    public List<String> getClusterDbNames(String clusterName) throws AnalysisException {
        final Cluster cluster = nameToCluster.get(clusterName);
        if (cluster == null) {
            throw new AnalysisException("No cluster selected");
        }
        return Lists.newArrayList(cluster.getDbNames());
    }

    // override
    @Override
    public List<Long> getDbIds() {
        return Lists.newArrayList(idToDb.keySet());
    }

    // special
    public List<Long> getDbIdsIncludeRecycleBin() {
        List<Long> dbIds = getDbIds();
        dbIds.addAll(GlobalStateMgr.getCurrentRecycleBin().getAllDbIds());
        return dbIds;
    }

    // special for reportHandler
    public HashMap<Long, TStorageMedium> getPartitionIdToStorageMediumMap() {
        HashMap<Long, TStorageMedium> storageMediumMap = new HashMap<Long, TStorageMedium>();

        // record partition which need to change storage medium
        // dbId -> (tableId -> partitionId)
        HashMap<Long, Multimap<Long, Long>> changedPartitionsMap = new HashMap<Long, Multimap<Long, Long>>();
        long currentTimeMs = System.currentTimeMillis();
        List<Long> dbIds = getDbIds();

        for (long dbId : dbIds) {
            Database db = getDb(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while doing backend report", dbId);
                continue;
            }

            db.readLock();
            try {
                for (Table table : db.getTables()) {
                    if (table.getType() != Table.TableType.OLAP) {
                        continue;
                    }

                    long tableId = table.getId();
                    OlapTable olapTable = (OlapTable) table;
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                    for (Partition partition : olapTable.getAllPartitions()) {
                        long partitionId = partition.getId();
                        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                        Preconditions.checkNotNull(dataProperty,
                                partition.getName() + ", pId:" + partitionId + ", db: " + dbId + ", tbl: " + tableId);
                        // only normal state table can migrate.
                        // PRIMARY_KEYS table does not support local migration.
                        if (dataProperty.getStorageMedium() == TStorageMedium.SSD
                                && dataProperty.getCooldownTimeMs() < currentTimeMs
                                && olapTable.getState() == OlapTable.OlapTableState.NORMAL
                                && olapTable.getKeysType() != KeysType.PRIMARY_KEYS) {
                            // expire. change to HDD.
                            // record and change when holding write lock
                            Multimap<Long, Long> multimap = changedPartitionsMap.get(dbId);
                            if (multimap == null) {
                                multimap = HashMultimap.create();
                                changedPartitionsMap.put(dbId, multimap);
                            }
                            multimap.put(tableId, partitionId);
                        } else {
                            storageMediumMap.put(partitionId, dataProperty.getStorageMedium());
                        }
                    } // end for partitions
                } // end for tables
            } finally {
                db.readUnlock();
            }
        } // end for dbs

        // handle data property changed
        for (Long dbId : changedPartitionsMap.keySet()) {
            Database db = getDb(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while checking backend storage medium", dbId);
                continue;
            }
            Multimap<Long, Long> tableIdToPartitionIds = changedPartitionsMap.get(dbId);

            // use try lock to avoid blocking a long time.
            // if block too long, backend report rpc will timeout.
            if (!db.tryWriteLock(Database.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                LOG.warn("try get db {} writelock but failed when hecking backend storage medium", dbId);
                continue;
            }
            Preconditions.checkState(db.isWriteLockHeldByCurrentThread());
            try {
                for (Long tableId : tableIdToPartitionIds.keySet()) {
                    Table table = db.getTable(tableId);
                    if (table == null) {
                        continue;
                    }
                    OlapTable olapTable = (OlapTable) table;
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();

                    Collection<Long> partitionIds = tableIdToPartitionIds.get(tableId);
                    for (Long partitionId : partitionIds) {
                        Partition partition = olapTable.getPartition(partitionId);
                        if (partition == null) {
                            continue;
                        }
                        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                        if (dataProperty.getStorageMedium() == TStorageMedium.SSD
                                && dataProperty.getCooldownTimeMs() < currentTimeMs) {
                            // expire. change to HDD.
                            partitionInfo.setDataProperty(partition.getId(), new DataProperty(TStorageMedium.HDD));
                            storageMediumMap.put(partitionId, TStorageMedium.HDD);
                            LOG.debug("partition[{}-{}-{}] storage medium changed from SSD to HDD",
                                    dbId, tableId, partitionId);

                            // log
                            ModifyPartitionInfo info =
                                    new ModifyPartitionInfo(db.getId(), olapTable.getId(),
                                            partition.getId(),
                                            DataProperty.DEFAULT_DATA_PROPERTY,
                                            (short) -1,
                                            partitionInfo.getIsInMemory(partition.getId()));
                            editLog.logModifyPartition(info);
                        }
                    } // end for partitions
                } // end for tables
            } finally {
                db.writeUnlock();
            }
        } // end for dbs
        return storageMediumMap;
    }

    // special
    public static short calcShortKeyColumnCount(List<Column> columns, Map<String, String> properties)
            throws DdlException {
        List<Column> indexColumns = new ArrayList<Column>();
        for (Column column : columns) {
            if (column.isKey()) {
                indexColumns.add(column);
            }
        }
        LOG.debug("index column size: {}", indexColumns.size());
        Preconditions.checkArgument(indexColumns.size() > 0);

        // figure out shortKeyColumnCount
        short shortKeyColumnCount = (short) -1;
        try {
            shortKeyColumnCount = PropertyAnalyzer.analyzeShortKeyColumnCount(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        if (shortKeyColumnCount != (short) -1) {
            // use user specified short key column count
            if (shortKeyColumnCount <= 0) {
                throw new DdlException("Invalid short key: " + shortKeyColumnCount);
            }

            if (shortKeyColumnCount > indexColumns.size()) {
                throw new DdlException("Short key is too large. should less than: " + indexColumns.size());
            }

            for (int pos = 0; pos < shortKeyColumnCount; pos++) {
                if (indexColumns.get(pos).getPrimitiveType() == PrimitiveType.VARCHAR &&
                        pos != shortKeyColumnCount - 1) {
                    throw new DdlException("Varchar should not in the middle of short keys.");
                }
            }
        } else {
            /*
             * Calc short key column count. NOTE: short key column count is
             * calculated as follow: 1. All index column are taking into
             * account. 2. Max short key column count is Min(Num of
             * indexColumns, META_MAX_SHORT_KEY_NUM). 3. Short key list can
             * contains at most one VARCHAR column. And if contains, it should
             * be at the last position of the short key list.
             */
            shortKeyColumnCount = 0;
            int shortKeySizeByte = 0;
            int maxShortKeyColumnCount = Math.min(indexColumns.size(), FeConstants.shortkey_max_column_count);
            for (int i = 0; i < maxShortKeyColumnCount; i++) {
                Column column = indexColumns.get(i);
                shortKeySizeByte += column.getOlapColumnIndexSize();
                if (shortKeySizeByte > FeConstants.shortkey_maxsize_bytes) {
                    if (column.getPrimitiveType().isCharFamily()) {
                        ++shortKeyColumnCount;
                    }
                    break;
                }
                if (column.getType().isFloatingPointType() || column.getType().isComplexType()) {
                    break;
                }
                if (column.getPrimitiveType() == PrimitiveType.VARCHAR) {
                    ++shortKeyColumnCount;
                    break;
                }
                ++shortKeyColumnCount;
            }
            if (indexColumns.isEmpty()) {
                throw new DdlException("Empty schema");
            }
            if (shortKeyColumnCount == 0) {
                throw new DdlException("Data type of first column cannot be " + indexColumns.get(0).getType());
            }

        } // end calc shortKeyColumnCount

        return shortKeyColumnCount;
    }

    /*
     * used for handling AlterTableStmt (for client is the ALTER TABLE command).
     * including SchemaChangeHandler and RollupHandler
     */
    // TODO override
    @Override
    public void alterTable(AlterTableStmt stmt) throws UserException {
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(stmt);
    }

    // TODO override
    @Override
    public void alterView(AlterViewStmt stmt) throws DdlException, UserException {
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterView(stmt, ConnectContext.get());
    }

    // TODO override
    @Override
    public void createMaterializedView(CreateMaterializedViewStmt stmt)
            throws AnalysisException, DdlException {
        GlobalStateMgr.getCurrentState().getAlterInstance().processCreateMaterializedView(stmt);
    }

    // TODO override
    @Override
    public void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        GlobalStateMgr.getCurrentState().getAlterInstance().processDropMaterializedView(stmt);
    }

    /*
     * used for handling CacnelAlterStmt (for client is the CANCEL ALTER
     * command). including SchemaChangeHandler and RollupHandler
     */
    @Override
    public void cancelAlter(CancelAlterTableStmt stmt) throws DdlException {
        if (stmt.getAlterType() == ShowAlterStmt.AlterType.ROLLUP) {
            GlobalStateMgr.getCurrentState().getRollupHandler().cancel(stmt);
        } else if (stmt.getAlterType() == ShowAlterStmt.AlterType.COLUMN) {
            GlobalStateMgr.getCurrentState().getSchemaChangeHandler().cancel(stmt);
        } else if (stmt.getAlterType() == ShowAlterStmt.AlterType.MATERIALIZED_VIEW) {
            GlobalStateMgr.getCurrentState().getRollupHandler().cancelMV(stmt);
        } else {
            throw new DdlException("Cancel " + stmt.getAlterType() + " does not implement yet");
        }
    }

    // override
    // entry of rename table operation
    @Override
    public void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {
        OlapTable olapTable = (OlapTable) table;
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + table.getName() + "] is under " + olapTable.getState());
        }

        String oldTableName = table.getName();
        String newTableName = tableRenameClause.getNewTableName();
        if (oldTableName.equals(newTableName)) {
            throw new DdlException("Same table name");
        }

        // check if name is already used
        if (db.getTable(newTableName) != null) {
            throw new DdlException("Table name[" + newTableName + "] is already used");
        }

        olapTable.checkAndSetName(newTableName, false);

        db.dropTable(oldTableName);
        db.createTable(table);

        // TODO
        TableInfo tableInfo = TableInfo.createForTableRename(db.getId(), table.getId(), newTableName);
        editLog.logTableRename(tableInfo);
        LOG.info("rename table[{}] to {}, tableId: {}", oldTableName, newTableName, table.getId());
    }

    // special
    @Override
    public void replayRenameTable(TableInfo tableInfo) throws DdlException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        String newTableName = tableInfo.getNewTableName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            String tableName = table.getName();
            db.dropTable(tableName);
            table.setName(newTableName);
            db.createTable(table);

            LOG.info("replay rename table[{}] to {}, tableId: {}", tableName, newTableName, table.getId());
        } finally {
            db.writeUnlock();
        }
    }

    // override
    @Override
    public void renamePartition(Database db, Table table, PartitionRenameClause renameClause) throws DdlException {
        OlapTable olapTable = (OlapTable) table;
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + table.getName() + "] is under " + olapTable.getState());
        }

        if (olapTable.getPartitionInfo().getType() != PartitionType.RANGE) {
            throw new DdlException("Table[" + table.getName() + "] is single partitioned. "
                    + "no need to rename partition name.");
        }

        String partitionName = renameClause.getPartitionName();
        String newPartitionName = renameClause.getNewPartitionName();
        if (partitionName.equalsIgnoreCase(newPartitionName)) {
            throw new DdlException("Same partition name");
        }

        Partition partition = table.getPartition(partitionName);
        if (partition == null) {
            throw new DdlException("Partition[" + partitionName + "] does not exists");
        }

        // check if name is already used
        if (olapTable.checkPartitionNameExist(newPartitionName)) {
            throw new DdlException("Partition name[" + newPartitionName + "] is already used");
        }

        olapTable.renamePartition(partitionName, newPartitionName);

        // log
        TableInfo tableInfo = TableInfo.createForPartitionRename(db.getId(), table.getId(), partition.getId(),
                newPartitionName);
        editLog.logPartitionRename(tableInfo);
        LOG.info("rename partition[{}] to {}", partitionName, newPartitionName);
    }

    // special
    @Override
    public void replayRenamePartition(TableInfo tableInfo) throws DdlException {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long partitionId = tableInfo.getPartitionId();
        String newPartitionName = tableInfo.getNewPartitionName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            Partition partition = table.getPartition(partitionId);
            table.renamePartition(partition.getName(), newPartitionName);

            LOG.info("replay rename partition[{}] to {}", partition.getName(), newPartitionName);
        } finally {
            db.writeUnlock();
        }
    }

    // override
    public void renameColumn(Database db, OlapTable table, ColumnRenameClause renameClause) throws DdlException {
        throw new DdlException("not implmented");
    }

    // special
    @Override
    public void modifyTableDynamicPartition(Database db, Table table, Map<String, String> properties)
            throws DdlException {
        OlapTable olapTable = (OlapTable) table;
        Map<String, String> logProperties = new HashMap<>(properties);
        TableProperty tableProperty = olapTable.getTableProperty();
        if (tableProperty == null) {
            DynamicPartitionUtil.checkAndSetDynamicPartitionProperty(olapTable, properties);
        } else {
            Map<String, String> analyzedDynamicPartition = DynamicPartitionUtil.analyzeDynamicPartition(properties);
            tableProperty.modifyTableProperties(analyzedDynamicPartition);
            tableProperty.buildDynamicProperty();
        }

        DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(db.getId(), olapTable);
        GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().createOrUpdateRuntimeInfo(
                table.getName(), DynamicPartitionScheduler.LAST_UPDATE_TIME, TimeUtils.getCurrentFormatTime());
        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), logProperties);
        editLog.logDynamicPartition(info);
    }

    // special
    // The caller need to hold the db write lock
    public void modifyTableReplicationNum(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentColocateIndex();
        if (colocateTableIndex.isColocateTable(table.getId())) {
            throw new DdlException("table " + table.getName() + " is colocate table, cannot change replicationNum");
        }

        String defaultReplicationNumName = "default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM;
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (partitionInfo.getType() == PartitionType.RANGE) {
            throw new DdlException(
                    "This is a range partitioned table, you should specify partitions with MODIFY PARTITION clause." +
                            " If you want to set default replication number, please use '" + defaultReplicationNumName +
                            "' instead of '" + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM + "' to escape misleading.");
        }

        // unpartitioned table
        // update partition replication num
        String partitionName = table.getName();
        Partition partition = table.getPartition(partitionName);
        if (partition == null) {
            throw new DdlException("Partition does not exist. name: " + partitionName);
        }

        short replicationNum = Short.parseShort(properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
        boolean isInMemory = partitionInfo.getIsInMemory(partition.getId());
        DataProperty newDataProperty = partitionInfo.getDataProperty(partition.getId());
        partitionInfo.setReplicationNum(partition.getId(), replicationNum);

        // update table default replication num
        table.setReplicationNum(replicationNum);

        // log
        ModifyPartitionInfo info = new ModifyPartitionInfo(db.getId(), table.getId(), partition.getId(),
                newDataProperty, replicationNum, isInMemory);
        editLog.logModifyPartition(info);
        LOG.info("modify partition[{}-{}-{}] replication num to {}", db.getFullName(), table.getName(),
                partition.getName(), replicationNum);
    }

    // special
    public void modifyTableDefaultReplicationNum(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentColocateIndex();
        if (colocateTableIndex.isColocateTable(table.getId())) {
            throw new DdlException("table " + table.getName() + " is colocate table, cannot change replicationNum");
        }

        // check unpartitioned table
        PartitionInfo partitionInfo = table.getPartitionInfo();
        Partition partition = null;
        boolean isUnpartitionedTable = false;
        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            isUnpartitionedTable = true;
            String partitionName = table.getName();
            partition = table.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException("Partition does not exist. name: " + partitionName);
            }
        }

        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
            table.setTableProperty(tableProperty);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildReplicationNum();

        // update partition replication num if this table is unpartitioned table
        if (isUnpartitionedTable) {
            Preconditions.checkNotNull(partition);
            partitionInfo.setReplicationNum(partition.getId(), tableProperty.getReplicationNum());
        }

        // log
        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        editLog.logModifyReplicationNum(info);
        LOG.info("modify table[{}] replication num to {}", table.getName(),
                properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
    }

    // special for schemaChangeHandler
    // The caller need to hold the db write lock
    public void modifyTableInMemoryMeta(Database db, OlapTable table, Map<String, String> properties) {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
            table.setTableProperty(tableProperty);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildInMemory();

        // need to update partition info meta
        for (Partition partition : table.getPartitions()) {
            table.getPartitionInfo().setIsInMemory(partition.getId(), tableProperty.isInMemory());
        }

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        editLog.logModifyInMemory(info);
    }

    public void setHasForbitGlobalDict(String dbName, String tableName, boolean isForbit) throws DdlException {
        Map<String, String> property = new HashMap<>();
        Database db = getDb(dbName);
        if (db == null) {
            throw new DdlException("the DB " + dbName + "isn't  exist");
        }

        Table table = db.getTable(tableName);
        if (table == null) {
            throw new DdlException("the DB " + dbName + " table: " + tableName + "isn't  exist");
        }

        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            olapTable.setHasForbitGlobalDict(isForbit);
            if (isForbit) {
                property.put(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE, PropertyAnalyzer.DISABLE_LOW_CARD_DICT);
            } else {
                property.put(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE, PropertyAnalyzer.ABLE_LOW_CARD_DICT);
            }
            ModifyTablePropertyOperationLog info =
                    new ModifyTablePropertyOperationLog(db.getId(), table.getId(), property);
            editLog.logSetHasForbitGlobalDict(info);
        }
    }

    // special
    @Override
    public void replayModifyTableProperty(short opCode, ModifyTablePropertyOperationLog info) {
        long dbId = info.getDbId();
        long tableId = info.getTableId();
        Map<String, String> properties = info.getProperties();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            if (opCode == OperationType.OP_SET_FORBIT_GLOBAL_DICT) {
                String enAble = properties.get(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE);
                Preconditions.checkState(enAble != null);
                if (olapTable != null) {
                    if (enAble.equals(PropertyAnalyzer.DISABLE_LOW_CARD_DICT)) {
                        olapTable.setHasForbitGlobalDict(true);
                    } else {
                        olapTable.setHasForbitGlobalDict(false);
                    }
                }
            } else {
                TableProperty tableProperty = olapTable.getTableProperty();
                if (tableProperty == null) {
                    tableProperty = new TableProperty(properties);
                    olapTable.setTableProperty(tableProperty.buildProperty(opCode));
                } else {
                    tableProperty.modifyTableProperties(properties);
                    tableProperty.buildProperty(opCode);
                }

                // need to replay partition info meta
                if (opCode == OperationType.OP_MODIFY_IN_MEMORY) {
                    for (Partition partition : olapTable.getPartitions()) {
                        olapTable.getPartitionInfo().setIsInMemory(partition.getId(), tableProperty.isInMemory());
                    }
                } else if (opCode == OperationType.OP_MODIFY_REPLICATION_NUM) {
                    // update partition replication num if this table is unpartitioned table
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                    if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                        String partitionName = olapTable.getName();
                        Partition partition = olapTable.getPartition(partitionName);
                        if (partition != null) {
                            partitionInfo.setReplicationNum(partition.getId(), tableProperty.getReplicationNum());
                        }
                    }
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    // TODO override
    public void alterCluster(AlterSystemStmt stmt) throws UserException {
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterCluster(stmt);
    }

    // TODO override
    public void cancelAlterCluster(CancelAlterSystemStmt stmt) throws DdlException {
        GlobalStateMgr.getCurrentState().getAlterInstance().getClusterHandler().cancel(stmt);
    }

    // special
    @Override
    public void changeDb(ConnectContext ctx, String qualifiedDb) throws DdlException {
        // TODO
        if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(ctx, qualifiedDb, PrivPredicate.SHOW)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_DB_ACCESS_DENIED, ctx.getQualifiedUser(), qualifiedDb);
        }

        if (getDb(qualifiedDb) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, qualifiedDb);
        }

        ctx.setDatabase(qualifiedDb);
    }



    // for test only
    public void clear() {
        if (idToDb != null) {
            idToDb.clear();
        }
        if (fullNameToDb != null) {
            fullNameToDb.clear();
        }

        // TODO
        GlobalStateMgr.getCurrentState().getRollupHandler().unprotectedGetAlterJobs().clear();
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().unprotectedGetAlterJobs().clear();
        System.gc();
    }

    // override
    @Override
    public void createView(CreateViewStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTable();

        // check if db exists
        Database db = this.getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // check if table exists in db
        db.readLock();
        try {
            if (db.getTable(tableName) != null) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create view[{}] which already exists", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            }
        } finally {
            db.readUnlock();
        }

        List<Column> columns = stmt.getColumns();

        long tableId = getNextId();
        View newView = new View(tableId, tableName, columns);
        newView.setComment(stmt.getComment());
        newView.setInlineViewDefWithSqlMode(stmt.getInlineViewDef(),
                ConnectContext.get().getSessionVariable().getSqlMode());
        // init here in case the stmt string from view.toSql() has some syntax error.
        try {
            newView.init();
        } catch (UserException e) {
            throw new DdlException("failed to init view stmt", e);
        }

        // check database exists again, because database can be dropped when creating table
        if (!GlobalStateMgr.getCurrentState().tryLock(false)) {
            throw new DdlException("Failed to acquire catalog lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating view");
            }
            if (!db.createTableWithLock(newView, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }

        LOG.info("successfully create view[" + tableName + "-" + newView.getId() + "]");
    }

    // special
    public void replayCreateCluster(Cluster cluster) {
        GlobalStateMgr.getCurrentState().tryLock(true);
        try {
            unprotectCreateCluster(cluster);
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    // TODO pricate
    private void unprotectCreateCluster(Cluster cluster) {
        for (Long id : cluster.getBackendIdList()) {
            final Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(id);
            backend.setOwnerClusterName(cluster.getName());
            backend.setBackendState(Backend.BackendState.using);
        }

        idToCluster.put(cluster.getId(), cluster);
        nameToCluster.put(cluster.getName(), cluster);

        // create info schema db
        final InfoSchemaDb infoDb = new InfoSchemaDb(cluster.getName());
        infoDb.setClusterName(cluster.getName());
        unprotectCreateDb(infoDb);

        // only need to create default cluster once.
        if (cluster.getName().equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            GlobalStateMgr.getCurrentState().setDefaultClusterCreated(true);
        }
    }

    // special for stmtexecutor
    public void changeCluster(ConnectContext ctx, String clusterName) throws DdlException {
        if (!GlobalStateMgr.getCurrentState().getAuth().checkCanEnterCluster(ConnectContext.get(), clusterName)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_AUTHORITY,
                    ConnectContext.get().getQualifiedUser(), "enter");
        }

        if (!nameToCluster.containsKey(clusterName)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
        }

        ctx.setCluster(clusterName);
    }

    // special
    public Cluster getCluster(String clusterName) {
        return nameToCluster.get(clusterName);
    }

    public List<String> getClusterNames() {
        return new ArrayList<String>(nameToCluster.keySet());
    }

    // special
    public Set<BaseParam> getMigrations() {
        final Set<BaseParam> infos = Sets.newHashSet();
        for (Database db : fullNameToDb.values()) {
            db.readLock();
            try {
                if (db.getDbState() == Database.DbState.MOVE) {
                    int tabletTotal = 0;
                    int tabletQuorum = 0;
                    // TODO
                    final Set<Long> beIds = Sets.newHashSet(GlobalStateMgr.getCurrentSystemInfo().getClusterBackendIds(db.getClusterName()));
                    final Set<String> tableNames = db.getTableNamesWithLock();
                    for (String tableName : tableNames) {

                        Table table = db.getTable(tableName);
                        if (table == null || table.getType() != Table.TableType.OLAP) {
                            continue;
                        }

                        OlapTable olapTable = (OlapTable) table;
                        for (Partition partition : olapTable.getPartitions()) {
                            final short replicationNum = olapTable.getPartitionInfo()
                                    .getReplicationNum(partition.getId());
                            for (MaterializedIndex materializedIndex : partition
                                    .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                                if (materializedIndex.getState() != MaterializedIndex.IndexState.NORMAL) {
                                    continue;
                                }
                                for (Tablet tablet : materializedIndex.getTablets()) {
                                    int replicaNum = 0;
                                    int quorum = replicationNum / 2 + 1;
                                    for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                                        if (replica.getState() != Replica.ReplicaState.CLONE
                                                && beIds.contains(replica.getBackendId())) {
                                            replicaNum++;
                                        }
                                    }
                                    if (replicaNum > quorum) {
                                        replicaNum = quorum;
                                    }

                                    tabletQuorum = tabletQuorum + replicaNum;
                                    tabletTotal = tabletTotal + quorum;
                                }
                            }
                        }
                    }
                    final BaseParam info = new BaseParam();
                    info.addStringParam(db.getClusterName());
                    info.addStringParam(db.getAttachDb());
                    info.addStringParam(db.getFullName());
                    final float percentage = tabletTotal > 0 ? (float) tabletQuorum / (float) tabletTotal : 0f;
                    info.addFloatParam(percentage);
                    infos.add(info);
                }
            } finally {
                db.readUnlock();
            }
        }

        return infos;
    }

    // special
    public long loadCluster(DataInputStream dis, long checksum) throws IOException, DdlException {
        if (getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            int clusterCount = dis.readInt();
            checksum ^= clusterCount;
            for (long i = 0; i < clusterCount; ++i) {
                final Cluster cluster = Cluster.read(dis);
                checksum ^= cluster.getId();

                // TODO
                List<Long> latestBackendIds = GlobalStateMgr.getCurrentState().getClusterInfo().getClusterBackendIds(cluster.getName());
                if (latestBackendIds.size() != cluster.getBackendIdList().size()) {
                    LOG.warn("Cluster:" + cluster.getName() + ", backends in Cluster is "
                            + cluster.getBackendIdList().size() + ", backends in SystemInfoService is "
                            + cluster.getBackendIdList().size());
                }
                // The number of BE in cluster is not same as in SystemInfoService, when perform 'ALTER
                // SYSTEM ADD BACKEND TO ...' or 'ALTER SYSTEM ADD BACKEND ...', because both of them are
                // for adding BE to some Cluster, but loadCluster is after loadBackend.
                cluster.setBackendIdList(latestBackendIds);

                String dbName = InfoSchemaDb.getFullInfoSchemaDbName(cluster.getName());
                InfoSchemaDb db;
                // Use real Catalog instance to avoid InfoSchemaDb id continuously increment
                // when checkpoint thread load image.
                if (fullNameToDb.containsKey(dbName)) {
                    db = (InfoSchemaDb) fullNameToDb.get(dbName);
                } else {
                    db = new InfoSchemaDb(cluster.getName());
                    db.setClusterName(cluster.getName());
                }
                String errMsg = "InfoSchemaDb id shouldn't larger than 10000, please restart your FE server";
                // Every time we construct the InfoSchemaDb, which id will increment.
                // When InfoSchemaDb id larger than 10000 and put it to idToDb,
                // which may be overwrite the normal db meta in idToDb,
                // so we ensure InfoSchemaDb id less than 10000.
                Preconditions.checkState(db.getId() < NEXT_ID_INIT_VALUE, errMsg);
                idToDb.put(db.getId(), db);
                fullNameToDb.put(db.getFullName(), db);
                cluster.addDb(dbName, db.getId());
                idToCluster.put(cluster.getId(), cluster);
                nameToCluster.put(cluster.getName(), cluster);
            }
        }
        LOG.info("finished replay cluster from image");
        return checksum;
    }

    public void refreshExternalTable(String dbName, String tableName, List<String> partitions) throws DdlException {
        Database db = getDb(dbName);
        if (db == null) {
            throw new DdlException("db: " + dbName + " not exists");
        }
        HiveTable table;
        db.readLock();
        try {
            Table tbl = db.getTable(tableName);
            if (!(tbl instanceof HiveTable)) {
                throw new DdlException("table : " + tableName + " not exists, or is not hive external table");
            }
            table = (HiveTable) tbl;
        } finally {
            db.readUnlock();
        }

        if (partitions != null && partitions.size() > 0) {
            table.refreshPartCache(partitions);
        } else {
            table.refreshTableCache();
        }
    }

    // special for refresh
    public void refreshExternalTable(RefreshExternalTableStmt stmt) throws DdlException {
        refreshExternalTable(stmt.getDbName(), stmt.getTableName(), stmt.getPartitions());

        List<Frontend> allFrontends = GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(null);
        Map<String, Future<TStatus>> resultMap = Maps.newHashMapWithExpectedSize(allFrontends.size() - 1);
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(GlobalStateMgr.getCurrentState().getNodeMgr().getSelfNode().first)) {
                continue;
            }

            resultMap.put(fe.getHost(), refreshOtherFesTable(new TNetworkAddress(fe.getHost(), fe.getRpcPort()),
                    stmt.getDbName(), stmt.getTableName(), stmt.getPartitions()));
        }

        String errMsg = "";
        for (Map.Entry<String, Future<TStatus>> entry : resultMap.entrySet()) {
            try {
                TStatus status = entry.getValue().get();
                if (status.getStatus_code() != TStatusCode.OK) {
                    String err = "refresh fe " + entry.getKey() + " failed: ";
                    if (status.getError_msgs() != null && status.getError_msgs().size() > 0) {
                        err += String.join(",", status.getError_msgs());
                    }
                    errMsg += err + ";";
                }
            } catch (Exception e) {
                errMsg += "refresh fe " + entry.getKey() + " failed: " + e.getMessage();
            }
        }
        if (!errMsg.equals("")) {
            ErrorReport.reportDdlException(ErrorCode.ERROR_REFRESH_EXTERNAL_TABLE_FAILED, errMsg);
        }
    }

    public Future<TStatus> refreshOtherFesTable(TNetworkAddress thriftAddress, String dbName, String tableName,
                                                List<String> partitions) {
        int timeout = ConnectContext.get().getSessionVariable().getQueryTimeoutS() * 1000;
        FutureTask<TStatus> task = new FutureTask<TStatus>(() -> {
            TRefreshTableRequest request = new TRefreshTableRequest();
            request.setDb_name(dbName);
            request.setTable_name(tableName);
            request.setPartitions(partitions);
            try {
                TRefreshTableResponse response = FrontendServiceProxy.call(thriftAddress, timeout,
                        client -> client.refreshTable(request));
                return response.getStatus();
            } catch (Exception e) {
                LOG.warn("call fe {} refreshTable rpc method failed", thriftAddress, e);
                TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
                status.setError_msgs(Lists.newArrayList(e.getMessage()));
                return status;
            }
        });

        new Thread(task).start();

        return task;
    }

    public void initDefaultCluster() {
        final List<Long> backendList = Lists.newArrayList();
        // TODO
        final List<Backend> defaultClusterBackends = GlobalStateMgr.getCurrentSystemInfo().getClusterBackends(SystemInfoService.DEFAULT_CLUSTER);
        for (Backend backend : defaultClusterBackends) {
            backendList.add(backend.getId());
        }

        final long id = getNextId();
        final Cluster cluster = new Cluster(SystemInfoService.DEFAULT_CLUSTER, id);

        // make sure one host hold only one backend.
        Set<String> beHost = Sets.newHashSet();
        for (Backend be : defaultClusterBackends) {
            if (beHost.contains(be.getHost())) {
                // we can not handle this situation automatically.
                LOG.error("found more than one backends in same host: {}", be.getHost());
                System.exit(-1);
            } else {
                beHost.add(be.getHost());
            }
        }

        // we create default_cluster to meet the need for ease of use, because
        // most users hava no multi tenant needs.
        cluster.setBackendIdList(backendList);
        unprotectCreateCluster(cluster);
        for (Database db : idToDb.values()) {
            db.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
            cluster.addDb(db.getFullName(), db.getId());
        }

        // no matter default_cluster is created or not,
        // mark isDefaultClusterCreated as true
        GlobalStateMgr.getCurrentState().setDefaultClusterCreated(true);
        editLog.logCreateCluster(cluster);
    }

    // special
    @Override
    public void replayUpdateDb(DatabaseInfo info) {
        final Database db = fullNameToDb.get(info.getDbName());
        db.setClusterName(info.getClusterName());
        db.setDbState(info.getDbState());
    }

    // special
    public long saveCluster(DataOutputStream dos, long checksum) throws IOException {
        final int clusterCount = idToCluster.size();
        checksum ^= clusterCount;
        dos.writeInt(clusterCount);
        for (Map.Entry<Long, Cluster> entry : idToCluster.entrySet()) {
            long clusterId = entry.getKey();
            if (clusterId >= NEXT_ID_INIT_VALUE) {
                checksum ^= clusterId;
                final Cluster cluster = entry.getValue();
                cluster.write(dos);
            }
        }
        return checksum;
    }

    // special
    public void replayUpdateClusterAndBackends(BackendIdsUpdateInfo info) {
        for (long id : info.getBackendList()) {
            final Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(id);
            final Cluster cluster = nameToCluster.get(backend.getOwnerClusterName());
            cluster.removeBackend(id);
            backend.setDecommissioned(false);
            backend.clearClusterName();
            backend.setBackendState(Backend.BackendState.free);
        }
    }

    // override
    @Override
    public void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
        TableRef tblRef = truncateTableStmt.getTblRef();
        TableName dbTbl = tblRef.getName();

        // check, and save some info which need to be checked again later
        Map<String, Long> origPartitions = Maps.newHashMap();
        OlapTable copiedTbl;
        Database db = getDb(dbTbl.getDb());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbTbl.getDb());
        }

        boolean truncateEntireTable = tblRef.getPartitionNames() == null;
        db.readLock();
        try {
            Table table = db.getTable(dbTbl.getTbl());
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, dbTbl.getTbl());
            }

            if (table.getType() != Table.TableType.OLAP) {
                throw new DdlException("Only support truncate OLAP table");
            }

            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new DdlException("Table' state is not NORMAL: " + olapTable.getState());
            }

            if (!truncateEntireTable) {
                for (String partName : tblRef.getPartitionNames().getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        throw new DdlException("Partition " + partName + " does not exist");
                    }

                    origPartitions.put(partName, partition.getId());
                }
            } else {
                for (Partition partition : olapTable.getPartitions()) {
                    origPartitions.put(partition.getName(), partition.getId());
                }
            }

            copiedTbl = olapTable.selectiveCopy(origPartitions.keySet(), true, MaterializedIndex.IndexExtState.VISIBLE);
        } finally {
            db.readUnlock();
        }

        // 2. use the copied table to create partitions
        List<Partition> newPartitions = Lists.newArrayListWithCapacity(origPartitions.size());
        // tabletIdSet to save all newly created tablet ids.
        Set<Long> tabletIdSet = Sets.newHashSet();
        try {
            for (Map.Entry<String, Long> entry : origPartitions.entrySet()) {
                long oldPartitionId = entry.getValue();
                long newPartitionId = getNextId();
                String newPartitionName = entry.getKey();

                PartitionInfo partitionInfo = copiedTbl.getPartitionInfo();
                partitionInfo.setTabletType(newPartitionId, partitionInfo.getTabletType(oldPartitionId));
                partitionInfo.setIsInMemory(newPartitionId, partitionInfo.getIsInMemory(oldPartitionId));
                partitionInfo.setReplicationNum(newPartitionId, partitionInfo.getReplicationNum(oldPartitionId));
                partitionInfo.setDataProperty(newPartitionId, partitionInfo.getDataProperty(oldPartitionId));

                Partition newPartition =
                        createPartition(db, copiedTbl, newPartitionId, newPartitionName, null, tabletIdSet);
                newPartitions.add(newPartition);
            }
            buildPartitions(db, copiedTbl, newPartitions);
        } catch (DdlException e) {
            // create partition failed, remove all newly created tablets
            for (Long tabletId : tabletIdSet) {
                GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            throw e;
        }
        Preconditions.checkState(origPartitions.size() == newPartitions.size());

        // all partitions are created successfully, try to replace the old partitions.
        // before replacing, we need to check again.
        // Things may be changed outside the database lock.
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(copiedTbl.getId());
            if (olapTable == null) {
                throw new DdlException("Table[" + copiedTbl.getName() + "] is dropped");
            }

            if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new DdlException("Table' state is not NORMAL: " + olapTable.getState());
            }

            // check partitions
            for (Map.Entry<String, Long> entry : origPartitions.entrySet()) {
                Partition partition = copiedTbl.getPartition(entry.getValue());
                if (partition == null || !partition.getName().equalsIgnoreCase(entry.getKey())) {
                    throw new DdlException("Partition [" + entry.getKey() + "] is changed");
                }
            }

            // check if meta changed
            // rollup index may be added or dropped, and schema may be changed during creating partition operation.
            boolean metaChanged = false;
            if (olapTable.getIndexNameToId().size() != copiedTbl.getIndexNameToId().size()) {
                metaChanged = true;
            } else {
                // compare schemaHash
                Map<Long, Integer> copiedIndexIdToSchemaHash = copiedTbl.getIndexIdToSchemaHash();
                for (Map.Entry<Long, Integer> entry : olapTable.getIndexIdToSchemaHash().entrySet()) {
                    long indexId = entry.getKey();
                    if (!copiedIndexIdToSchemaHash.containsKey(indexId)) {
                        metaChanged = true;
                        break;
                    }
                    if (!copiedIndexIdToSchemaHash.get(indexId).equals(entry.getValue())) {
                        metaChanged = true;
                        break;
                    }
                }
            }

            if (metaChanged) {
                throw new DdlException("Table[" + copiedTbl.getName() + "]'s meta has been changed. try again.");
            }

            // replace
            truncateTableInternal(olapTable, newPartitions, truncateEntireTable);

            // write edit log
            TruncateTableInfo info = new TruncateTableInfo(db.getId(), olapTable.getId(), newPartitions,
                    truncateEntireTable);
            editLog.logTruncateTable(info);
        } finally {
            db.writeUnlock();
        }

        LOG.info("finished to truncate table {}, partitions: {}",
                tblRef.getName().toSql(), tblRef.getPartitionNames());
    }

    // private
    private void truncateTableInternal(OlapTable olapTable, List<Partition> newPartitions, boolean isEntireTable) {
        // use new partitions to replace the old ones.
        Set<Long> oldTabletIds = Sets.newHashSet();
        for (Partition newPartition : newPartitions) {
            Partition oldPartition = olapTable.replacePartition(newPartition);
            // save old tablets to be removed
            for (MaterializedIndex index : oldPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                index.getTablets().stream().forEach(t -> {
                    oldTabletIds.add(t.getId());
                });
            }
        }

        if (isEntireTable) {
            // drop all temp partitions
            olapTable.dropAllTempPartitions();
        }

        // remove the tablets in old partitions
        for (Long tabletId : oldTabletIds) {
            GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
        }
    }

    // special
    @Override
    public void replayTruncateTable(TruncateTableInfo info) {
        Database db = getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTblId());
            truncateTableInternal(olapTable, info.getPartitions(), info.isEntireTable());

            if (!isCheckpointThread()) {
                // add tablet to inverted index
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
                for (Partition partition : info.getPartitions()) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    for (MaterializedIndex mIndex : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        long indexId = mIndex.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(db.getId(), olapTable.getId(),
                                partitionId, indexId, schemaHash, medium);
                        for (Tablet tablet : mIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                                invertedIndex.addReplica(tabletId, replica);
                            }
                        }
                    }
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    // override
    public void createFunction(CreateFunctionStmt stmt) throws UserException {
        FunctionName name = stmt.getFunctionName();
        Database db = getDb(name.getDb());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, name.getDb());
        }
        db.addFunction(stmt.getFunction());
    }

    // special
    public void replayCreateFunction(Function function) {
        String dbName = function.getFunctionName().getDb();
        Database db = getDb(dbName);
        if (db == null) {
            throw new Error("unknown database when replay log, db=" + dbName);
        }
        db.replayAddFunction(function);
    }

    // override
    public void dropFunction(DropFunctionStmt stmt) throws UserException {
        FunctionName name = stmt.getFunctionName();
        Database db = getDb(name.getDb());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, name.getDb());
        }
        db.dropFunction(stmt.getFunction());
    }

    // special
    public void replayDropFunction(FunctionSearchDesc functionSearchDesc) {
        String dbName = functionSearchDesc.getName().getDb();
        Database db = getDb(dbName);
        if (db == null) {
            throw new Error("unknown database when replay log, db=" + dbName);
        }
        db.replayDropFunction(functionSearchDesc);
    }

    // special
    @Override
    public void replayBackendTabletsInfo(BackendTabletsInfo backendTabletsInfo) {
        List<Pair<Long, Integer>> tabletsWithSchemaHash = backendTabletsInfo.getTabletSchemaHash();
        if (!tabletsWithSchemaHash.isEmpty()) {
            // In previous version, we save replica info in `tabletsWithSchemaHash`,
            // but it is wrong because we can not get replica from `tabletInvertedIndex` when doing checkpoint,
            // because when doing checkpoint, the tabletInvertedIndex is not initialized at all.
            //
            // So we can only discard this information, in this case, it is equivalent to losing the record of these operations.
            // But it doesn't matter, these records are currently only used to record whether a replica is in a bad state.
            // This state has little effect on the system, and it can be restored after the system has processed the bad state replica.
            for (Pair<Long, Integer> tabletInfo : tabletsWithSchemaHash) {
                LOG.warn("find an old backendTabletsInfo for tablet {}, ignore it", tabletInfo.first);
            }
            return;
        }

        // in new version, replica info is saved here.
        // but we need to get replica from db->tbl->partition->...
        List<ReplicaPersistInfo> replicaPersistInfos = backendTabletsInfo.getReplicaPersistInfos();
        for (ReplicaPersistInfo info : replicaPersistInfos) {
            long dbId = info.getDbId();
            Database db = getDb(dbId);
            if (db == null) {
                continue;
            }
            db.writeLock();
            try {
                OlapTable tbl = (OlapTable) db.getTable(info.getTableId());
                if (tbl == null) {
                    continue;
                }
                Partition partition = tbl.getPartition(info.getPartitionId());
                if (partition == null) {
                    continue;
                }
                MaterializedIndex mindex = partition.getIndex(info.getIndexId());
                if (mindex == null) {
                    continue;
                }
                LocalTablet tablet = (LocalTablet) mindex.getTablet(info.getTabletId());
                if (tablet == null) {
                    continue;
                }
                Replica replica = tablet.getReplicaById(info.getReplicaId());
                if (replica != null) {
                    replica.setBad(true);
                    LOG.debug("get replica {} of tablet {} on backend {} to bad when replaying",
                            info.getReplicaId(), info.getTabletId(), info.getBackendId());
                }
            } finally {
                db.writeUnlock();
            }
        }
    }

    // special
    public void convertDistributionType(Database db, OlapTable tbl) throws DdlException {
        db.writeLock();
        try {
            if (!tbl.convertRandomDistributionToHashDistribution()) {
                throw new DdlException("Table " + tbl.getName() + " is not random distributed");
            }
            TableInfo tableInfo = TableInfo.createForModifyDistribution(db.getId(), tbl.getId());
            editLog.logModifyDistributionType(tableInfo);
            LOG.info("finished to modify distribution type of table: " + tbl.getName());
        } finally {
            db.writeUnlock();
        }
    }

    @Override
    public void replayConvertDistributionType(TableInfo tableInfo) {
        Database db = getDb(tableInfo.getDbId());
        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableInfo.getTableId());
            tbl.convertRandomDistributionToHashDistribution();
            LOG.info("replay modify distribution type of table: " + tbl.getName());
        } finally {
            db.writeUnlock();
        }
    }

    // private or special
    public void replaceTempPartition(Database db, String tableName, ReplacePartitionClause clause) throws DdlException {
        List<String> partitionNames = clause.getPartitionNames();
        // duplicate temp partition will cause Incomplete transaction
        List<String> tempPartitionNames =
                clause.getTempPartitionNames().stream().distinct().collect(Collectors.toList());

        boolean isStrictRange = clause.isStrictRange();
        boolean useTempPartitionName = clause.useTempPartitionName();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (table.getType() != Table.TableType.OLAP) {
                throw new DdlException("Table[" + tableName + "] is not OLAP table");
            }

            OlapTable olapTable = (OlapTable) table;
            // check partition exist
            for (String partName : partitionNames) {
                if (!olapTable.checkPartitionNameExist(partName, false)) {
                    throw new DdlException("Partition[" + partName + "] does not exist");
                }
            }
            for (String partName : tempPartitionNames) {
                if (!olapTable.checkPartitionNameExist(partName, true)) {
                    throw new DdlException("Temp partition[" + partName + "] does not exist");
                }
            }

            olapTable.replaceTempPartitions(partitionNames, tempPartitionNames, isStrictRange, useTempPartitionName);

            // write log
            ReplacePartitionOperationLog info = new ReplacePartitionOperationLog(db.getId(), olapTable.getId(),
                    partitionNames, tempPartitionNames, isStrictRange, useTempPartitionName);
            editLog.logReplaceTempPartition(info);
            LOG.info("finished to replace partitions {} with temp partitions {} from table: {}",
                    clause.getPartitionNames(), clause.getTempPartitionNames(), tableName);
        } finally {
            db.writeUnlock();
        }
    }

    // private
    @Override
    public void replayReplaceTempPartition(ReplacePartitionOperationLog replaceTempPartitionLog) {
        Database db = getDb(replaceTempPartitionLog.getDbId());
        if (db == null) {
            return;
        }
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(replaceTempPartitionLog.getTblId());
            if (olapTable == null) {
                return;
            }
            olapTable.replaceTempPartitions(replaceTempPartitionLog.getPartitions(),
                    replaceTempPartitionLog.getTempPartitions(),
                    replaceTempPartitionLog.isStrictRange(),
                    replaceTempPartitionLog.useTempPartitionName());
        } catch (DdlException e) {
            LOG.warn("should not happen.", e);
        } finally {
            db.writeUnlock();
        }
    }

    // special
    // Set specified replica's status. If replica does not exist, just ignore it.
    public void setReplicaStatus(AdminSetReplicaStatusStmt stmt) {
        long tabletId = stmt.getTabletId();
        long backendId = stmt.getBackendId();
        Replica.ReplicaStatus status = stmt.getStatus();
        setReplicaStatusInternal(tabletId, backendId, status, false);
    }

    // private or special
    public void replaySetReplicaStatus(SetReplicaStatusOperationLog log) {
        setReplicaStatusInternal(log.getTabletId(), log.getBackendId(), log.getReplicaStatus(), true);
    }

    // private
    private void setReplicaStatusInternal(long tabletId, long backendId, Replica.ReplicaStatus status, boolean isReplay) {
        TabletMeta meta = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletMeta(tabletId);
        if (meta == null) {
            LOG.info("tablet {} does not exist", tabletId);
            return;
        }
        long dbId = meta.getDbId();
        Database db = getDb(dbId);
        if (db == null) {
            LOG.info("database {} of tablet {} does not exist", dbId, tabletId);
            return;
        }
        db.writeLock();
        try {
            Replica replica = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplica(tabletId, backendId);
            if (replica == null) {
                LOG.info("replica of tablet {} does not exist", tabletId);
                return;
            }
            if (status == Replica.ReplicaStatus.BAD || status == Replica.ReplicaStatus.OK) {
                if (replica.setBadForce(status == Replica.ReplicaStatus.BAD)) {
                    if (!isReplay) {
                        SetReplicaStatusOperationLog log =
                                new SetReplicaStatusOperationLog(backendId, tabletId, status);
                        editLog.logSetReplicaStatus(log);
                    }
                    LOG.info("set replica {} of tablet {} on backend {} as {}. is replay: {}",
                            replica.getId(), tabletId, backendId, status, isReplay);
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void onEraseDatabase(long dbId) {
        // remove jobs
        GlobalStateMgr.getCurrentState().getSchemaChangeHandler().removeDbAlterJob(dbId);
        GlobalStateMgr.getCurrentState().getRollupHandler().removeDbAlterJob(dbId);

        // remove database transaction manager
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().removeDatabaseTransactionMgr(dbId);
    }

    // remove or private
    public void onEraseOlapTable(OlapTable olapTable, boolean isReplay) {
        // inverted index
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        Collection<Partition> allPartitions = olapTable.getAllPartitions();
        for (Partition partition : allPartitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }

        if (!isReplay) {
            // drop all replicas
            AgentBatchTask batchTask = new AgentBatchTask();
            for (Partition partition : olapTable.getAllPartitions()) {
                List<MaterializedIndex> allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                for (MaterializedIndex materializedIndex : allIndices) {
                    long indexId = materializedIndex.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    for (Tablet tablet : materializedIndex.getTablets()) {
                        long tabletId = tablet.getId();
                        if (partition.isUseStarOS()) {
                            DropReplicaTask dropTask = new DropReplicaTask(
                                    ((StarOSTablet) tablet).getPrimaryBackendId(), tabletId, schemaHash, true);
                            batchTask.addTask(dropTask);
                        } else {
                            List<Replica> replicas = ((LocalTablet) tablet).getReplicas();
                            for (Replica replica : replicas) {
                                long backendId = replica.getBackendId();
                                DropReplicaTask dropTask = new DropReplicaTask(backendId, tabletId, schemaHash, true);
                                batchTask.addTask(dropTask);
                            } // end for replicas
                        }
                    } // end for tablets
                } // end for indices
            } // end for partitions
            AgentTaskExecutor.submit(batchTask);
        }

        // colocation
        GlobalStateMgr.getCurrentColocateIndex().removeTable(olapTable.getId());
    }

    // special or override
    public void onErasePartition(Partition partition) {
        // remove tablet in inverted index
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                invertedIndex.deleteTablet(tablet.getId());
            }
        }
    }
}
