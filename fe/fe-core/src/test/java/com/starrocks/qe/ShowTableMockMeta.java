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

import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.privilege.IdGenerator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowTableMockMeta extends LocalMetastore {
    private final IdGenerator idGenerator;

    private final Map<String, Database> databaseSet;
    private final Map<String, Database> externalDbSet;

    private final Map<String, Table> tableMap;
    private final Map<String, Table> externalTbSet;

    public ShowTableMockMeta(GlobalStateMgr globalStateMgr, CatalogRecycleBin recycleBin,
                             ColocateTableIndex colocateTableIndex) {
        super(globalStateMgr, recycleBin, colocateTableIndex);
        idGenerator = new IdGenerator();

        this.databaseSet = new HashMap<>();
        this.externalDbSet = new HashMap<>();

        this.tableMap = new HashMap<>();
        this.externalTbSet = new HashMap<>();

        Database db = new Database(idGenerator.getNextId(), "testDb");
        databaseSet.put("testDb", db);

        Database db2 = new Database(idGenerator.getNextId(), "test");
        databaseSet.put("test", db2);

        OlapTable t0 = new OlapTable();
        t0.setId(idGenerator.getNextId());
        t0.setName("testTbl");
        tableMap.put("testTbl", t0);

        MaterializedView mv = new MaterializedView();
        mv.setId(idGenerator.getNextId());
        mv.setName("testMv");
        tableMap.put("testMv", mv);

        Database db3 = new Database(idGenerator.getNextId(), "hive_db");
        externalDbSet.put("hive_db", db3);

        HiveTable table = new HiveTable();
        table.setId(idGenerator.getNextId());
        table.setName("hive_test");
        externalTbSet.put("hive_test", table);
    }

    @Override
    public Database getDb(String dbName) {
        return databaseSet.get(dbName);
    }

    @Override
    public Database getDb(long databaseId) {
        for (Database database : databaseSet.values()) {
            if (database.getId() == databaseId) {
                return database;
            }
        }

        return null;
    }

    @Override
    public List<String> listDbNames() {
        return new ArrayList<>(databaseSet.keySet());
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        //if (catalogName.equals("hive_catalog")) {
        //    return externalTbSet.get(tblName);
        //}
        return tableMap.get(tblName);
    }

    @Override
    public List<String> listTableNames(String dbName) {
        //if (catalogName.equals("hive_catalog")) {
        //    return new ArrayList<>(externalTbSet.keySet());
        //}
        return new ArrayList<>(tableMap.keySet());
    }
}
