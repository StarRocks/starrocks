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

package com.starrocks.sql.automv.pieces;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;

import java.util.Objects;

public class FQTable {
    private final Catalog catalog;
    private final Database database;
    private final Table table;
    private final TableName tableName;

    private FQTable(Catalog catalog, Database database, Table table, TableName tableName) {
        this.catalog = catalog;
        this.database = Objects.requireNonNull(database);
        this.table = Objects.requireNonNull(table);
        this.tableName = Objects.requireNonNull(tableName);
    }

    public static FQTable of(Catalog catalog, Database database, Table table, TableName tableName) {
        return new FQTable(catalog, database, table, tableName);
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public Database getDatabase() {
        return database;
    }

    public Table getTable() {
        return table;
    }

    public String getFQName() {
        return tableName.toSql();
    }

    public TableName getFqTableName() {
        return tableName;
    }
}