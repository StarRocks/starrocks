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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.starrocks.authorization.IdGenerator;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.transaction.GlobalTransactionMgr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MockedLocalMetaStore extends LocalMetastore {
    private static final short DEFAULT_REPLICATION_NUM = 3;

    private final IdGenerator idGenerator;
    private final Map<String, Database> nameToDb;
    private final Map<Long, Database> idToDb;
    private final GlobalStateMgr globalStateMgr;

    public MockedLocalMetaStore(GlobalStateMgr globalStateMgr,
                                CatalogRecycleBin recycleBin,
                                ColocateTableIndex colocateTableIndex) {
        super(globalStateMgr, recycleBin, colocateTableIndex);

        this.globalStateMgr = globalStateMgr;
        this.nameToDb = new HashMap<>();
        this.idToDb = new HashMap<>();
        this.idGenerator = new IdGenerator();
    }

    @Override
    public Database getDb(String dbName) {
        return nameToDb.get(dbName);
    }

    @Override
    public Database getDb(long databaseId) {
        return idToDb.get(databaseId);
    }

    @Override
    public ConcurrentHashMap<String, Database> getFullNameToDb() {
        return new ConcurrentHashMap<>(nameToDb);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        Database db = nameToDb.get(dbName);
        return db.getTable(tblName);
    }

    @Override
    public List<Table> getTables(Long dbId) {
        Database db = idToDb.get(dbId);
        return db.getTables();
    }

    @Override
    public void createDb(String name) {
        Database database = new Database(idGenerator.getNextId(), name);
        nameToDb.put(name, database);
        idToDb.put(database.getId(), database);

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        globalTransactionMgr.addDatabaseTransactionMgr(database.getId());
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        Database db = getDb(stmt.getDbName());
        String tableName = stmt.getTableName();

        // create distribution info
        Preconditions.checkNotNull(db, "Database %s not found", stmt.getDbName());
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo distributionInfo = DistributionInfoBuilder.build(distributionDesc, null);

        long tableId = idGenerator.getNextId();
        long partitionId = idGenerator.getNextId();
        SinglePartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(partitionId, DEFAULT_REPLICATION_NUM);

        OlapTable olapTable = new OlapTable(
                tableId,
                tableName,
                stmt.getColumns(),
                null,
                partitionInfo,
                distributionInfo);

        long indexId = idGenerator.getNextId();
        olapTable.setIndexMeta(
                indexId,
                tableName,
                stmt.getColumns(),
                0,
                0,
                (short) 0,
                TStorageType.COLUMN,
                KeysType.DUP_KEYS);
        olapTable.setBaseIndexMetaId(indexId);
        olapTable.setReplicationNum(DEFAULT_REPLICATION_NUM);

        Partition partition = createPartition(
                db.getId(),
                olapTable.getId(),
                partitionId,
                tableName,
                distributionInfo,
                indexId);
        olapTable.addPartition(partition);

        db.registerTableUnlocked(olapTable);

        return true;
    }

    private Partition createPartition(long dbId,
                                      long tableId,
                                      long partitionId,
                                      String partitionName,
                                      DistributionInfo distributionInfo,
                                      long baseIndexId) {
        MaterializedIndex baseIndex = new MaterializedIndex(baseIndexId, IndexState.NORMAL);
        long physicalPartitionId = globalStateMgr.getNextId();
        Partition partition = new Partition(
                partitionId,
                physicalPartitionId,
                partitionName,
                baseIndex,
                distributionInfo);

        int bucketNum = distributionInfo != null ? distributionInfo.getBucketNum() : 1;
        bucketNum = bucketNum > 0 ? bucketNum : 1;

        for (int i = 0; i < bucketNum; i++) {
            long tabletId = globalStateMgr.getNextId();
            TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, baseIndexId, TStorageMedium.HDD);
            com.starrocks.catalog.LocalTablet tablet = new com.starrocks.catalog.LocalTablet(tabletId);
            baseIndex.addTablet(tablet, tabletMeta, false);
            globalStateMgr.getTabletInvertedIndex().addTablet(tabletId, tabletMeta);
        }

        return partition;
    }

    @Override
    public void createView(CreateViewStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTable();
        List<Column> columns = stmt.getColumns();

        long tableId = idGenerator.getNextId();
        View view = new View(tableId, tableName, columns);
        view.setComment(stmt.getComment());
        view.setInlineViewDefWithSqlMode(stmt.getInlineViewDef(),
                ConnectContext.get().getSessionVariable().getSqlMode());

        Database db = getDb(dbName);
        db.registerTableUnlocked(view);
    }

}
