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


package com.starrocks.connector.informationschema;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.connector.ConnectorMetadata;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.starrocks.catalog.system.information.InfoSchemaDb.isInfoSchemaDb;
import static com.starrocks.server.CatalogMgr.isInternalCatalog;
import static java.util.Objects.requireNonNull;

public class InformationSchemaMetadata implements ConnectorMetadata {
    private final InfoSchemaDb infoSchemaDb;

    public InformationSchemaMetadata(String catalogName) {
        requireNonNull(catalogName, "catalogName is null");
        checkArgument(!isInternalCatalog(catalogName), "this class is not for internal catalog");
        this.infoSchemaDb = new InfoSchemaDb(catalogName);
    }

    @Override
    public List<String> listDbNames() {
        return Lists.newArrayList(InfoSchemaDb.DATABASE_NAME);
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return infoSchemaDb.getTables().stream().map(Table::getName).collect(Collectors.toList());
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        if (isInfoSchemaDb(dbName)) {
            return infoSchemaDb.getTable(tblName);
        }
        return null;
    }

    @Override
    public boolean dbExists(String dbName) {
        return isInfoSchemaDb(dbName);
    }

    @Override
    public Database getDb(String name) {
        if (isInfoSchemaDb(name)) {
            return this.infoSchemaDb;
        }
        return null;
    }
}
