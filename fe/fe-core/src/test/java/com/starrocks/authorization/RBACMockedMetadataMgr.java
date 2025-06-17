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

package com.starrocks.authorization;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTblMetaInfoMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TemporaryTableMgr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class RBACMockedMetadataMgr extends MetadataMgr {
    private final LocalMetastore localMetastore;

    public RBACMockedMetadataMgr(LocalMetastore localMetastore, ConnectorMgr connectorMgr) {
        super(localMetastore, new TemporaryTableMgr(), connectorMgr, new ConnectorTblMetaInfoMgr());
        this.localMetastore = localMetastore;

    }

    @Override
    public Database getDb(ConnectContext context, String catalogName, String dbName) {
        return this.localMetastore.getDb(dbName);
    }

    @Override
    public Database getDb(Long databaseId) {
        return this.localMetastore.getDb(databaseId);
    }

    @Override
    public List<String> listDbNames(ConnectContext context, String catalogName) {
        Map<String, Database> dbs = localMetastore.getFullNameToDb();
        return new ArrayList<>(dbs.keySet());
    }

    @Override
    public Optional<Table> getTable(ConnectContext context, TableName tableName) {
        return Optional.ofNullable(localMetastore.getTable(tableName.getDb(), tableName.getTbl()));
    }

    @Override
    public Table getTable(ConnectContext context, String catalogName, String dbName, String tblName) {
        return localMetastore.getTable(dbName, tblName);
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String catalogName, String dbName) {
        Database database = localMetastore.getDb(dbName);

        List<Table> tables = localMetastore.getTables(database.getId());
        return tables.stream().map(Table::getName).collect(Collectors.toList());
    }
}
