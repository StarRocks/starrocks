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
package com.starrocks.privilege;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RBACMockedMetadataMgr extends MetadataMgr {
    private final LocalMetastore localMetastore;
    private final IdGenerator idGenerator;
    private final Map<String, Database> databaseSet;
    private final Map<String, Table> tableMap;

    public RBACMockedMetadataMgr(LocalMetastore localMetastore, ConnectorMgr connectorMgr) {
        super(localMetastore, connectorMgr, new ConnectorTblMetaInfoMgr());
        this.localMetastore = localMetastore;
        idGenerator = new IdGenerator();
        this.databaseSet = new HashMap<>();
        this.tableMap = new HashMap<>();
    }

    public void init() {
        Database db = new Database(idGenerator.getNextId(), "db");
        databaseSet.put("db", db);

        Database db1 = new Database(idGenerator.getNextId(), "db3");
        databaseSet.put("db1", db1);

        Database db3 = new Database(idGenerator.getNextId(), "db3");
        databaseSet.put("db3", db3);

        OlapTable t0 = new OlapTable();
        t0.setId(idGenerator.getNextId());
        t0.setName("tbl0");
        tableMap.put("tbl0", t0);

        OlapTable t1 = new OlapTable();
        t1.setId(idGenerator.getNextId());
        t1.setName("tbl1");
        tableMap.put("tbl1", t1);

        OlapTable t2 = new OlapTable();
        t2.setId(idGenerator.getNextId());
        t2.setName("tbl2");
        tableMap.put("tbl2", t2);

        OlapTable t3 = new OlapTable();
        t3.setId(idGenerator.getNextId());
        t3.setName("tbl3");
        tableMap.put("tbl3", t3);

        MaterializedView mv = new MaterializedView();
        mv.setId(idGenerator.getNextId());
        mv.setName("mv1");
        tableMap.put("mv1", mv);

        View view = new View();
        view.setId(idGenerator.getNextId());
        view.setName("view1");
        tableMap.put("view1", view);
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
