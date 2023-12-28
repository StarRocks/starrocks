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
package com.starrocks.qe;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.privilege.IdGenerator;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ShowTableMockMeta extends MetadataMgr {
    private final LocalMetastore localMetastore;
    private final IdGenerator idGenerator;
    private final Map<String, Database> databaseSet;
    private final Map<String, Table> tableMap;

    public ShowTableMockMeta(LocalMetastore localMetastore, ConnectorMgr connectorMgr) {
        super(localMetastore, connectorMgr, new ConnectorTblMetaInfoMgr());
        this.localMetastore = localMetastore;
        idGenerator = new IdGenerator();
        this.databaseSet = new HashMap<>();
        this.tableMap = new HashMap<>();
    }

    public void init() {
        Database db = new Database(idGenerator.getNextId(), "testDb");
        databaseSet.put("testDb", db);

        Database db2 = new Database(idGenerator.getNextId(), "test");
        databaseSet.put("test", db2);

        OlapTable t0 = new OlapTable();
        t0.setId(idGenerator.getNextId());
        t0.setName("testTbl");
        tableMap.put("testTbl", t0);

        HiveTable table = new HiveTable();
        table.setId(idGenerator.getNextId());
        table.setName("hive_test");
        tableMap.put("hive_test", table);

        MaterializedView mv = new MaterializedView();
        mv.setId(idGenerator.getNextId());
        mv.setName("testMv");
        tableMap.put("testMv", mv);
    }

    @Override
    public Database getDb(String catalogName, String dbName) {
        return databaseSet.get(dbName);
    }

    @Override
    public Database getDb(Long databaseId) {
        for (Database database : databaseSet.values()) {
            if (database.getId() == databaseId) {
                return database;
            }
        }

        return null;
    }

    @Override
    public List<String> listDbNames(String catalogName) {
        return new ArrayList<>(databaseSet.keySet());
    }

    @Override
    public Optional<Table> getTable(TableName tableName) {
        return Optional.ofNullable(tableMap.get(tableName.getTbl()));
    }

    @Override
    public Table getTable(String catalogName, String dbName, String tblName) {
        return tableMap.get(tblName);
    }

    @Override
    public Table getTable(Long databaseId, Long tableId) {
        for (Table table : tableMap.values()) {
            if (table.getId() == tableId) {
                return table;
            }
        }

        return null;
    }

    @Override
    public List<String> listTableNames(String catalogName, String dbName) {
        return new ArrayList<>(tableMap.keySet());
    }
}
