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
package com.starrocks.meta;

import com.sleepycat.je.Transaction;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.persist.CreateDbInfo;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.DropPartitionsInfo;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionDesc;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LocalMetastoreV2 implements LocalMetastoreInterface {
    @Override
    public void createDb(Database db, String storageVolumeId) {
        MetadataHandler metadataHandler = GlobalStateMgr.getCurrentState().getMetadataHandler();
        Transaction transaction = metadataHandler.starTransaction();
        metadataHandler.put(transaction,
                ByteCoder.encode("meta object", "db", String.valueOf(db.getId())),
                GsonUtils.GSON.toJson(db, Database.class));
        metadataHandler.put(transaction,
                ByteCoder.encode("meta name", "db", db.getFullName()),
                String.valueOf(db.getId()));
        metadataHandler.put(transaction,
                ByteCoder.encode("meta id", "instance",String.valueOf(db.getId())),
                "");

        CreateDbInfo createDbInfo = new CreateDbInfo(db.getId(), db.getFullName());
        createDbInfo.setStorageVolumeId(storageVolumeId);
        createDbInfo.setTransaction(transaction);
        GlobalStateMgr.getCurrentState().getEditLog().logJsonObject(OperationType.OP_CREATE_DB_V2, createDbInfo);
    }

    @Override
    public void dropDb(Database db, boolean isForceDrop) {

    }

    @Override
    public void recoverDatabase(Database db) {

    }

    @Override
    public void alterDatabaseQuota(DatabaseInfo dbInfo) {

    }

    @Override
    public void renameDatabase(String dbName, String newDbName) {

    }

    @Override
    public List<String> listDbNames() {
        return null;
    }

    @Override
    public ConcurrentHashMap<Long, Database> getIdToDb() {
        return null;
    }

    @Override
    public List<Long> getDbIds() {
        return null;
    }

    @Override
    public ConcurrentHashMap<String, Database> getFullNameToDb() {
        return null;
    }

    @Override
    public Database getDb(String name) {
        MetadataHandler metadataHandler = GlobalStateMgr.getCurrentState().getMetadataHandler();
        Transaction transaction = metadataHandler.starTransaction();
        Long databaseId = metadataHandler.get(transaction, ByteCoder.encode("meta name", "db", name), Long.class);

        String value = metadataHandler.get(transaction,
                ByteCoder.encode("meta object", "db", String.valueOf(databaseId)), String.class);

        Database database = GsonUtils.GSON.fromJson(value, Database.class);
        return database;
    }

    @Override
    public Database getDb(long dbId) {
        return null;
    }

    @Override
    public void createTable(CreateTableInfo createTableInfo) {

    }

    @Override
    public void dropTable(DropInfo dropInfo) {

    }

    @Override
    public void renameTable(TableInfo tableInfo) {

    }

    @Override
    public void truncateTable(TruncateTableInfo info) {

    }

    @Override
    public void alterTable(ModifyTablePropertyOperationLog log) {

    }

    @Override
    public void modifyTableProperty(Database db, OlapTable table, Map<String, String> properties, short operationType) {
        MetadataHandler metadataHandler = GlobalStateMgr.getCurrentState().getMetadataHandler();
        Transaction transaction = metadataHandler.starTransaction();
        metadataHandler.put(transaction,
                ByteCoder.encode("meta object", "table", String.valueOf(table.getId())),
                GsonUtils.GSON.toJson(table, OlapTable.class));

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        info.setTransaction(transaction);

        switch (operationType) {
            case OperationType.OP_MODIFY_IN_MEMORY:
                GlobalStateMgr.getCurrentState().getEditLog().logModifyInMemory(info);
                break;
            case OperationType.OP_MODIFY_WRITE_QUORUM:
                GlobalStateMgr.getCurrentState().getEditLog().logModifyWriteQuorum(info);
                break;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return null;
    }

    @Override
    public List<Table> getTables(Long dbId) {
        return null;
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        return null;
    }

    @Override
    public Table getTable(Long dbId, Long tableId) {
        return null;
    }

    @Override
    public void addPartitionLog(Database db,
                                OlapTable olapTable,
                                List<PartitionDesc> partitionDescs,
                                boolean isTempPartition,
                                PartitionInfo partitionInfo,
                                List<Partition> partitionList,
                                Set<String> existPartitionNameSet) throws DdlException {

    }

    @Override
    public void dropPartition(DropPartitionInfo dropPartitionInfo) {

    }

    @Override
    public void dropPartitions(DropPartitionsInfo dropPartitionsInfo) {

    }

    @Override
    public void renamePartition(TableInfo tableInfo) {

    }

    @Override
    public void replaceTempPartition(ReplacePartitionOperationLog info) {

    }

    @Override
    public void setPartitionVersion(PartitionVersionRecoveryInfo info) {

    }

    @Override
    public void addSubPartitionLog(Database db, OlapTable olapTable, Partition partition, List<PhysicalPartition> subPartitions) {

    }
}
