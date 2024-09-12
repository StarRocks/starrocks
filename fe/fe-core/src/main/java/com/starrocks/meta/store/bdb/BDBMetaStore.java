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
package com.starrocks.meta.store.bdb;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.journal.bdbje.CloseSafeDatabase;
import com.starrocks.meta.MetaStore;
import com.starrocks.meta.MetadataHandler;
import com.starrocks.meta.TxnMeta;
import com.starrocks.meta.kv.ByteCoder;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.AddSubPartitionsInfoV2;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.BatchDeleteReplicaInfo;
import com.starrocks.persist.BatchDropInfo;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.ColumnRenameInfo;
import com.starrocks.persist.ConsistencyCheckInfo;
import com.starrocks.persist.CreateDbInfo;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropDbInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.DropPartitionsInfo;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.RenameMaterializedViewLog;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.SwapTableOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTabletMetaType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class BDBMetaStore implements MetaStore {
    private static final Logger LOG = LogManager.getLogger(BDBMetaStore.class);

    private static final int OUTPUT_BUFFER_INIT_SIZE = 128;

    private static final String EDIT_LOG_ID = "edit_log_id";

    public long getEditLogReplayId() {
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        Long id = metadataHandler.get(null, ByteCoder.encode(EDIT_LOG_ID), Long.class);
        if (id == null) {
            return 0;
        } else {
            return id;
        }
    }

    public void setEditLogID(long editLogID) {
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        metadataHandler.put(null, ByteCoder.encode(EDIT_LOG_ID), editLogID, Long.class);
    }

    public void commit(Transaction transaction, short op, TxnMeta writable, DatabaseEntry compatible) {
        long journalId;
        if (writable.isReplay()) {
            journalId = writable.getReplayedJournalId();
        } else {
            ReentrantLock journalLock = GlobalStateMgr.getCurrentState().getJournalLock();
            journalLock.lock();
            try {
                Journal journal = GlobalStateMgr.getCurrentState().getJournal();
                CloseSafeDatabase journalDatabase = ((BDBJEJournal) journal).getCurrentJournalDB();
                journalId = GlobalStateMgr.getCurrentState().getNextVisibleJournalId();

                TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
                DatabaseEntry theKey = new DatabaseEntry();
                idBinding.objectToEntry(journalId, theKey);
                journalDatabase.put(transaction, theKey, compatible);
            } finally {
                journalLock.unlock();
            }
        }

        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        metadataHandler.put(transaction, ByteCoder.encode(EDIT_LOG_ID), journalId, Long.class);
        transaction.commit();
    }

    public DatabaseEntry buildCompatibleEditLog(short op, TxnMeta writable) {
        JournalEntity journalEntity = new JournalEntity();
        journalEntity.setOpCode(op);
        journalEntity.setData(out -> Text.writeString(out, GsonUtils.GSON.toJson(writable)));
        DataOutputBuffer buffer = new DataOutputBuffer(OUTPUT_BUFFER_INIT_SIZE);
        try {
            journalEntity.write(buffer);
        } catch (IOException e) {
            // The old implementation swallow exception like this
            LOG.info("failed to serialize, ", e);
        }

        return new DatabaseEntry(buffer.getData(), 0, buffer.getLength());
    }

    @Override
    public void createDb(CreateDbInfo createDbInfo) {
        LOG.warn("createDb");
        DatabaseEntry compatible = buildCompatibleEditLog(OperationType.OP_CREATE_DB_V2, createDbInfo);

        Database db = new Database(createDbInfo.getId(), createDbInfo.getDbName());

        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        Transaction transaction = metadataHandler.starTransaction();
        metadataHandler.put(transaction,
                ByteCoder.encode("meta_object", "db", db.getId()), db, Database.class);
        metadataHandler.put(transaction,
                ByteCoder.encode("meta_name", "db", db.getFullName()), db.getId(), Long.class);
        metadataHandler.put(transaction,
                ByteCoder.encode("meta_id", "instance", db.getId()), "", String.class);

        commit(transaction, OperationType.OP_CREATE_DB_V2, createDbInfo, compatible);
    }

    @Override
    public void dropDb(DropDbInfo dropDbInfo) {
        LOG.warn("dropDb");
        DatabaseEntry compatible = buildCompatibleEditLog(OperationType.OP_CREATE_DB_V2, dropDbInfo);

        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();

        long databaseId = getDatabaseId(dropDbInfo.getDbName());
        String dbName = dropDbInfo.getDbName();

        Transaction transaction = metadataHandler.starTransaction();
        metadataHandler.delete(transaction, ByteCoder.encode("meta_object", "db", databaseId));
        metadataHandler.delete(transaction, ByteCoder.encode("meta_name", "db", dbName));
        metadataHandler.delete(transaction, ByteCoder.encode("meta_id", "instance", databaseId));

        commit(transaction, OperationType.OP_DROP_DB, dropDbInfo, compatible);
    }

    @Override
    public void recoverDatabase(Database db) {
        LOG.warn("recoverDatabase");
    }

    @Override
    public void alterDatabaseQuota(DatabaseInfo dbInfo) {
        LOG.warn("alterDatabaseQuota");
    }

    @Override
    public void renameDatabase(String dbName, String newDbName) {
        LOG.warn("renameDatabase");
    }

    @Override
    public List<String> listDbNames() {
        LOG.warn("listDbNames");
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();

        List<List<Object>> values = metadataHandler.getPrefixNoReturnValue(null,
                ByteCoder.encode("meta_name", "db"));

        List<String> dbNameList = new ArrayList<>();
        for (List<Object> value : values) {
            Object dbName = value.get(2);
            dbNameList.add((String) dbName);
        }

        return dbNameList;
    }

    @Override
    public ConcurrentHashMap<Long, Database> getIdToDb() {
        LOG.warn("getIdToDb");

        ConcurrentHashMap<Long, Database> result = new ConcurrentHashMap<>();
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        List<List<Object>> values = metadataHandler.getPrefixNoReturnValue(null,
                ByteCoder.encode("meta_id", "instance"));
        for (List<Object> value : values) {
            Long databaseId = (Long) value.get(2);
            Database database = getDb(databaseId);

            result.put(databaseId, database);
        }

        return result;
    }

    @Override
    public List<Long> getDbIds() {
        LOG.warn("getDbIds");

        List<Long> result = new ArrayList<>();
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        List<List<Object>> values = metadataHandler.getPrefixNoReturnValue(null,
                ByteCoder.encode("meta_id", "instance"));
        for (List<Object> value : values) {
            Long databaseId = (Long) value.get(2);
            result.add(databaseId);
        }

        return result;
    }

    @Override
    public ConcurrentHashMap<String, Database> getFullNameToDb() {
        LOG.warn("getFullNameToDb");

        ConcurrentHashMap<String, Database> result = new ConcurrentHashMap<>();
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        List<List<Object>> values = metadataHandler.getPrefixNoReturnValue(null,
                ByteCoder.encode("meta_id", "instance"));
        for (List<Object> value : values) {
            Long databaseId = (Long) value.get(2);
            Database database = getDb(databaseId);

            result.put(database.getFullName(), database);
        }

        return result;
    }

    @Override
    public Database getDb(String name) {
        LOG.warn("getDb");
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        Transaction transaction = metadataHandler.starTransaction();
        Long databaseId = metadataHandler.get(transaction, ByteCoder.encode("meta_name", "db", name), Long.class);
        if (databaseId == null) {
            return null;
        }

        return metadataHandler.get(transaction,
                ByteCoder.encode("meta_object", "db", databaseId), Database.class);
    }

    private Long getDatabaseId(String name) {
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        Transaction transaction = metadataHandler.starTransaction();
        return metadataHandler.get(transaction, ByteCoder.encode("meta_name", "db", name), Long.class);
    }

    @Override
    public Database getDb(long dbId) {
        LOG.warn("getDb");

        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        Transaction transaction = metadataHandler.starTransaction();
        return metadataHandler.get(transaction, ByteCoder.encode("meta_object", "db", dbId), Database.class);
    }

    @Override
    public void createTable(CreateTableInfo createTableInfo) {
        LOG.warn("createTable");
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();

        DatabaseEntry compatible = buildCompatibleEditLog(OperationType.OP_CREATE_TABLE_V2, createTableInfo);
        Long databaseId = getDatabaseId(createTableInfo.getDbName());

        Transaction transaction = metadataHandler.starTransaction();
        Table table = createTableInfo.getTable();

        if (table instanceof OlapTable) {
            buildTabletInvertedIndex(transaction, databaseId, (OlapTable) table);

            OlapTable olapTable = (OlapTable) table;
            olapTable.clearHierarchy();
        }

        metadataHandler.put(transaction,
                ByteCoder.encode("meta_object", "table", table.getId()), table, Table.class);

        metadataHandler.put(transaction,
                ByteCoder.encode("meta_name", "table", createTableInfo.getDbName(), table.getName()),
                table.getId(), Long.class);

        metadataHandler.put(transaction,
                ByteCoder.encode("meta_id", "table", databaseId, table.getId()),
                "", String.class);

        commit(transaction, OperationType.OP_CREATE_TABLE_V2, createTableInfo, compatible);
    }

    private void buildTabletInvertedIndex(Transaction transaction, long dbId, OlapTable olapTable) {
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();

        long tableId = olapTable.getId();
        for (PhysicalPartition partition : olapTable.getAllPhysicalPartitions()) {
            long physicalPartitionId = partition.getId();
            TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                    partition.getParentId()).getStorageMedium();
            for (MaterializedIndex mIndex : partition
                    .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                long indexId = mIndex.getId();
                int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partition.getParentId(), physicalPartitionId,
                        indexId, schemaHash, medium, olapTable.isCloudNativeTableOrMaterializedView());
                for (Tablet tablet : mIndex.getTablets()) {
                    long tabletId = tablet.getId();

                    metadataHandler.put(transaction,
                            ByteCoder.encode("tablet_inverted_index", "tablet_meta", tabletId),
                            tabletMeta, TabletMeta.class);

                    if (tablet instanceof LocalTablet) {
                        for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                            metadataHandler.put(transaction,
                                    ByteCoder.encode("replica_id_to_tablet_id", replica.getId()),
                                    tabletId, Long.class);
                            metadataHandler.put(transaction,
                                    ByteCoder.encode("backend_replica_meta", replica.getBackendId(), tableId),
                                    replica.getId(), Long.class);
                        }
                    }
                }
            }
        } // end for partitions
    }

    @Override
    public void dropTable(DropInfo dropInfo) {
        LOG.warn("dropTable");
    }

    @Override
    public void renameTable(TableInfo tableInfo) {
        LOG.warn("renameTable");
    }

    @Override
    public void truncateTable(TruncateTableInfo info) {
        LOG.warn("truncateTable");
    }

    @Override
    public void swapTable(SwapTableOperationLog log) {
        LOG.warn("swapTable");
    }

    @Override
    public void alterTable(ModifyTablePropertyOperationLog log) {
        LOG.warn("alterTable");
    }

    @Override
    public void renameColumn(ColumnRenameInfo columnRenameInfo) {
        LOG.warn("renameColumn");
    }

    @Override
    public void updateTableMeta(Database db, String tableName, Map<String, String> properties, TTabletMetaType metaType) {
        LOG.warn("updateTableMeta");
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        Transaction transaction = metadataHandler.starTransaction();

        long tabletId = 0;
        String olapTableJson = metadataHandler.get(transaction, ByteCoder.encode(String.valueOf(tabletId)), String.class);
        OlapTable table = GsonUtils.GSON.fromJson(olapTableJson, OlapTable.class);
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
            table.setTableProperty(tableProperty);
        } else {
            tableProperty.modifyTableProperties(properties);
        }

        metadataHandler.put(transaction,
                ByteCoder.encode("meta object", "table", String.valueOf(table.getId())),
                table, OlapTable.class);

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);

        switch (metaType) {
            case INMEMORY:
                GlobalStateMgr.getCurrentState().getEditLog().logModifyInMemory(info);
                break;
            case WRITE_QUORUM:
                GlobalStateMgr.getCurrentState().getEditLog().logModifyWriteQuorum(info);
                break;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        LOG.warn("listTableNames");
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();

        List<List<Object>> values = metadataHandler.getPrefixNoReturnValue(null,
                ByteCoder.encode("meta_name", "table", dbName));

        List<String> dbNameList = new ArrayList<>();
        for (List<Object> value : values) {
            dbNameList.add((String) value.get(3));
        }

        return dbNameList;
    }

    @Override
    public List<Table> getTables(Long dbId) {
        LOG.warn("getTables");

        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        List<List<Object>> values = metadataHandler.getPrefixNoReturnValue(null,
                ByteCoder.encode("meta_id", "table", dbId));

        List<Table> result = new ArrayList<>();
        for (List<Object> value : values) {
            Long tableId = (Long) value.get(3);
            Table table = getTable(dbId, tableId);
            result.add(table);
        }

        return result;
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        LOG.warn("getTable");
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();

        Long tableId = metadataHandler.get(null,
                ByteCoder.encode("meta_name", "table", dbName, tblName), Long.class);

        return metadataHandler.get(null,
                ByteCoder.encode("meta_object", "table", tableId), Table.class);
    }

    @Override
    public Table getTable(Long dbId, Long tableId) {
        LOG.warn("getTable");
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        return metadataHandler.get(null, ByteCoder.encode("meta_object", "table", tableId),
                Table.class);
    }

    @Override
    public void modifyViewDef(AlterViewInfo alterViewInfo) {
        LOG.warn("modifyViewDef");
    }

    @Override
    public void renameMaterializedView(RenameMaterializedViewLog log) {
        LOG.warn("renameMaterializedView");
    }

    @Override
    public void alterMvBaseTableInfos(AlterMaterializedViewBaseTableInfosLog log) {
        LOG.warn("alterMvBaseTableInfos");
    }

    @Override
    public void alterMvStatus(AlterMaterializedViewStatusLog log) {
        LOG.warn("alterMvStatus");
    }

    @Override
    public void alterMaterializedViewProperties(ModifyTablePropertyOperationLog log) {
        LOG.warn("alterMaterializedViewProperties");
    }

    @Override
    public void changeMaterializedRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log) {
        LOG.warn("changeMaterializedRefreshScheme");
    }

    @Override
    public void addPartitionLog(Database db,
                                OlapTable olapTable,
                                List<PartitionDesc> partitionDescs,
                                boolean isTempPartition,
                                PartitionInfo partitionInfo,
                                List<Partition> partitionList,
                                Set<String> existPartitionNameSet) throws DdlException {
        LOG.warn("addPartitionLog");
    }

    @Override
    public void addPartition(AddPartitionsInfoV2 addPartitionsInfo) {
        LOG.warn("addPartition");
    }

    @Override
    public void dropPartition(DropPartitionInfo dropPartitionInfo) {
        LOG.warn("dropPartition");
    }

    @Override
    public void dropPartitions(DropPartitionsInfo dropPartitionsInfo) {
        LOG.warn("dropPartitions");
    }

    @Override
    public void renamePartition(TableInfo tableInfo) {
        LOG.warn("renamePartition");
    }

    @Override
    public void replaceTempPartition(ReplacePartitionOperationLog info) {
        LOG.warn("replaceTempPartition");
    }

    @Override
    public void modifyPartition(ModifyPartitionInfo info) {
        LOG.warn("modifyPartition");
    }

    @Override
    public void setPartitionVersion(PartitionVersionRecoveryInfo info) {
        LOG.warn("setPartitionVersion");
    }

    @Override
    public void addSubPartitionLog(AddSubPartitionsInfoV2 addSubPartitionsInfo) {
        LOG.warn("addSubPartitionLog");
    }

    @Override
    public List<PhysicalPartition> getAllPhysicalPartition(OlapTable olapTable) {
        return null;
    }

    @Override
    public List<PhysicalPartition> getAllPhysicalPartition(Partition partition) {
        LOG.warn("getAllPhysicalPartition");
        return null;
    }

    @Override
    public PhysicalPartition getPhysicalPartition(Partition partition, Long physicalPartitionId) {
        LOG.warn("getPhysicalPartition");
        return null;
    }

    @Override
    public void addPhysicalPartition(Partition partition, PhysicalPartition physicalPartition) {
        LOG.warn("addPhysicalPartition");
    }

    @Override
    public void dropPhysicalPartition(Partition partition, Long physicalPartitionId) {
        LOG.warn("dropPhysicalPartition");
    }

    @Override
    public List<MaterializedIndex> getMaterializedIndices(PhysicalPartition physicalPartition,
                                                          MaterializedIndex.IndexExtState indexExtState) {
        LOG.warn("getMaterializedIndices");
        return null;
    }

    @Override
    public MaterializedIndex getMaterializedIndex(PhysicalPartition physicalPartition, Long mIndexId) {
        LOG.warn("getMaterializedIndex");
        return null;
    }

    @Override
    public void addMaterializedIndex(PhysicalPartition physicalPartition, MaterializedIndex materializedIndex) {
        LOG.warn("addMaterializedIndex");
    }

    @Override
    public void dropMaterializedIndex(PhysicalPartition physicalPartition, Long mIndexId) {
        LOG.warn("dropMaterializedIndex");
    }

    @Override
    public void dropRollup(DropInfo dropInfo) {
        LOG.warn("dropRollup");
    }

    @Override
    public void batchDropRollup(BatchDropInfo batchDropInfo) {
        LOG.warn("batchDropRollup");
    }

    @Override
    public void renameRollup(TableInfo tableInfo) {
        LOG.warn("renameRollup");
    }

    @Override
    public List<Tablet> getAllTablets(MaterializedIndex materializedIndex) {
        LOG.warn("getAllTablets");
        return null;
    }

    @Override
    public List<Long> getAllTabletIDs(MaterializedIndex materializedIndex) {
        LOG.warn("getAllTabletIDs");
        return null;
    }

    @Override
    public Tablet getTablet(MaterializedIndex materializedIndex, Long tabletId) {
        LOG.warn("getTablet");
        return null;
    }

    @Override
    public List<Replica> getAllReplicas(Tablet tablet) {
        LOG.warn("getAllReplicas");
        return null;
    }

    @Override
    public Replica getReplica(LocalTablet tablet, Long replicaId) {
        LOG.warn("getReplica");
        return null;
    }

    @Override
    public void addReplica(ReplicaPersistInfo replicaPersistInfo) {
        LOG.warn("addReplica");
    }

    @Override
    public void deleteReplica(ReplicaPersistInfo replicaPersistInfo) {
        LOG.warn("deleteReplica");
    }

    @Override
    public void batchDeleteReplicaInfo(BatchDeleteReplicaInfo replicaPersistInfo) {
        LOG.warn("batchDeleteReplicaInfo");
    }

    @Override
    public void updateReplica(ReplicaPersistInfo replicaPersistInfo) {
        LOG.warn("updateReplica");
    }

    @Override
    public void setReplicaStatus(SetReplicaStatusOperationLog log) {
        LOG.warn("setReplicaStatus");
    }

    @Override
    public void backendTabletsInfo(BackendTabletsInfo backendTabletsInfo) {
        LOG.warn("backendTabletsInfo");
    }

    @Override
    public void finishConsistencyCheck(ConsistencyCheckInfo info) {
        LOG.warn("finishConsistencyCheck");
    }
}
