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

package com.starrocks.connector.benchmark;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.BenchmarkTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.qe.ConnectContext;

import java.util.List;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class BenchmarkMetadata implements ConnectorMetadata {
    private final String catalogName;
    private final BenchmarkCatalogConfig config;
    private final BenchmarkTableSchemas tableSchemas;

    public BenchmarkMetadata(String catalogName, BenchmarkCatalogConfig config) {
        this.catalogName = catalogName;
        this.config = config;
        this.tableSchemas = new BenchmarkTableSchemas(null);
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.BENCHMARK;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (BenchmarkSuite suite : BenchmarkSuiteFactory.getSuites()) {
            builder.add(BenchmarkNameUtils.normalizeDbName(suite.getName()));
        }
        return builder.build();
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        BenchmarkSuite suite = BenchmarkSuiteFactory.getSuiteIfExists(dbName);
        if (suite == null) {
            return null;
        }
        String normalizedName = BenchmarkNameUtils.normalizeDbName(suite.getName());
        Database db = new Database(suite.getDbId(), normalizedName);
        db.setCatalogName(catalogName);
        return db;
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        return tableSchemas.getTableNames(dbName);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        String normalizedDbName = BenchmarkNameUtils.normalizeDbName(dbName);
        String normalizedTableName = BenchmarkNameUtils.normalizeTableName(tblName);
        List<Column> columns = tableSchemas.getTableSchema(normalizedDbName, normalizedTableName);
        if (columns == null) {
            return null;
        }
        return new BenchmarkTable(CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalogName, normalizedDbName,
                normalizedTableName, columns, config);
    }
}
