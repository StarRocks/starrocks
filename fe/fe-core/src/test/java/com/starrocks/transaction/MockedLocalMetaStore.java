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

package com.starrocks.transaction;

import com.google.common.base.Preconditions;
import com.starrocks.authorization.IdGenerator;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfoBuilder;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.thrift.TStorageType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MockedLocalMetaStore extends LocalMetastore {
    private final IdGenerator idGenerator;
    private final Map<String, Database> nameToDb;
    private final Map<Long, Database> idToDb;

    public MockedLocalMetaStore(GlobalStateMgr globalStateMgr,
                                CatalogRecycleBin recycleBin,
                                ColocateTableIndex colocateTableIndex) {
        super(globalStateMgr, recycleBin, colocateTableIndex);

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
        for (Database database : nameToDb.values()) {
            if (database.getId() == databaseId) {
                return database;
            }
        }

        return null;
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
        Database db = nameToDb.get(dbId);
        return db.getTables();
    }

    @Override
    public void createDb(String name) {
        Database database = new Database(idGenerator.getNextId(), name);
        nameToDb.put(name, database);

        GlobalTransactionMgr globalTransactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        globalTransactionMgr.addDatabaseTransactionMgr(database.getId());
    }

    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        Database db = getDb(stmt.getDbName());
        String tableName = stmt.getTableName();

        // create distribution info
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo distributionInfo = DistributionInfoBuilder.build(distributionDesc, null);

        long partitionId = idGenerator.getNextId();
        SinglePartitionInfo partitionInfo = new SinglePartitionInfo();
        Partition partition = createPartition(partitionId, tableName, distributionInfo);

        OlapTable olapTable = new OlapTable(
                idGenerator.getNextId(),
                tableName,
                stmt.getColumns(),
                null,
                partitionInfo,
                distributionInfo);
        olapTable.addPartition(partition);

        long indexId = idGenerator.getNextId();
        olapTable.setIndexMeta(
                indexId,
                "",
                stmt.getColumns(),
                0,
                0,
                (short) 0,
                TStorageType.COLUMN,
                KeysType.UNIQUE_KEYS);
        olapTable.setBaseIndexId(indexId);

        db.registerTableUnlocked(olapTable);

        return true;
    }

    private Partition createPartition(long partitionId, String partitionName, DistributionInfo distributionInfo) {
        Partition logicalPartition = new Partition(
                partitionId,
                partitionName,
                distributionInfo);

        long physicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        PhysicalPartition physicalPartition = new PhysicalPartition(
                physicalPartitionId,
                logicalPartition.generatePhysicalPartitionName(physicalPartitionId),
                partitionId,
                null);
        physicalPartition.setBucketNum(distributionInfo.getBucketNum());

        logicalPartition.addSubPartition(physicalPartition);

        return logicalPartition;
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
