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

package com.starrocks.connector.lance;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LanceTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.qe.ConnectContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class LanceMetadata implements ConnectorMetadata {
    private final String catalogName;
    private final Map<String, String> properties;
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<String, List<Table>> tables = new ConcurrentHashMap<>();

    public LanceMetadata(String catalogName, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.properties = properties;
        bootstrapMetadata();
    }

    private void bootstrapMetadata() {
        // Bootstraps basic database and table definitions from the catalog configuration properties.
        // This ensures SHOW DATABASES / SHOW TABLES can discover configured tables in production.
        // e.g. properties can contain:
        // "database" -> "default"
        // "table.vectors.uri" -> "s3://bucket/vectors"
        // "table.vectors.schema" -> "id:int64,embedding:fixed_size_list<float32, 128>"
        String dbName = properties.getOrDefault("database", "default");
        Database db = new Database(CONNECTOR_ID_GENERATOR.getNextId().asLong(), dbName);
        addDatabase(db);

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("table.") && key.endsWith(".uri")) {
                String tblName = key.substring(6, key.length() - 4);
                String uri = entry.getValue();
                String schemaStr = properties.get("table." + tblName + ".schema");
                ArrayList<Column> columns = new ArrayList<>();
                if (schemaStr != null) {
                    for (String field : LanceApiConverter.splitTopLevel(schemaStr, ',')) {
                        int colonIdx = field.indexOf(':');
                        if (colonIdx > 0) {
                            String name = field.substring(0, colonIdx).trim();
                            String typeStr = field.substring(colonIdx + 1).trim();
                            columns.add(new Column(name, LanceApiConverter.parseType(typeStr)));
                        }
                    }
                }
                LanceTable table = new LanceTable(CONNECTOR_ID_GENERATOR.getNextId().asLong(), tblName, columns, uri);
                addTable(dbName, table);
            }
        }
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.LANCE;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        return ImmutableList.copyOf(databases.keySet());
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        List<Table> tableList = tables.get(dbName);
        if (tableList == null) {
            return ImmutableList.of();
        }
        return tableList.stream().map(Table::getName).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        return databases.get(dbName);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        List<Table> tableList = tables.get(dbName);
        if (tableList == null) {
            return null;
        }
        return tableList.stream()
                .filter(t -> t.getName().equalsIgnoreCase(tblName))
                .findFirst()
                .orElse(null);
    }

    // Helpers for unit tests to register metadata manually in Phase 1 (local catalogs)
    public void addDatabase(Database db) {
        databases.put(db.getFullName(), db);
    }

    public void addTable(String dbName, Table table) {
        tables.computeIfAbsent(dbName, k -> Lists.newCopyOnWriteArrayList()).add(table);
    }
}
