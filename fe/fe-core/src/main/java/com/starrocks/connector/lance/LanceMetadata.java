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
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LanceTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.qe.ConnectContext;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class LanceMetadata implements ConnectorMetadata {
    private final String catalogName;
    private final CloudConfiguration cloudConfiguration;
    private final Map<String, String> properties;
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<String, List<Table>> tables = new ConcurrentHashMap<>();

    private final LanceCatalogBridge rest;
    private final Map<String, Long> remoteIds = new ConcurrentHashMap<>();

    public LanceMetadata(String catalogName, Map<String, String> properties) {
        this(catalogName, properties, new LanceCatalogBridge());
    }

    LanceMetadata(String catalogName, Map<String, String> properties, LanceCatalogBridge bridge) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        String mode = properties.getOrDefault("lance.catalog.type", "static");
        if (!"static".equalsIgnoreCase(mode) && !"rest".equalsIgnoreCase(mode)) {
            throw new IllegalArgumentException("lance.catalog.type must be static or rest");
        }
        this.rest = "rest".equalsIgnoreCase(mode) ? bridge : null;
        if (rest != null) {
            if (properties.getOrDefault("lance.catalog.uri", "").isBlank()) {
                throw new IllegalArgumentException("Lance REST metadata requires lance.catalog.uri");
            }
            if (properties.keySet().stream().anyMatch(key -> key.startsWith("table."))) {
                throw new IllegalArgumentException("Lance REST metadata cannot be combined with static table properties");
            }
        } else {
            bootstrapMetadata();
        }
    }

    private String invoke(String method, String... args) {
        List<String> parameters = new ArrayList<>();
        parameters.add(properties.get("lance.catalog.uri"));
        parameters.add(properties.getOrDefault("lance.catalog.bearer-token-file", ""));
        parameters.addAll(Arrays.asList(args));
        return rest.invoke(method, parameters.toArray(String[]::new));
    }

    // The root is "$"; literal dots, percent signs and dollar signs are escaped in each component.
    static String databaseName(List<String> namespace) {
        return namespace.isEmpty() ? "$" : namespace.stream()
                .map(part -> part.replace("%", "%25").replace(".", "%2E").replace("$", "%24"))
                .collect(Collectors.joining("."));
    }

    static List<String> namespace(String database) {
        if ("$".equals(database)) {
            return List.of();
        }
        List<String> result = Arrays.stream(database.split("\\.", -1))
                .map(part -> part.replace("%24", "$").replace("%2E", ".").replace("%25", "%"))
                .collect(Collectors.toList());
        if (result.stream().anyMatch(String::isEmpty) || !databaseName(result).equals(database)) {
            throw new StarRocksConnectorException("Invalid Lance database name");
        }
        return result;
    }

    private long remoteId(String key) {
        return remoteIds.computeIfAbsent(key, ignored -> CONNECTOR_ID_GENERATOR.getNextId().asLong());
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
                LanceTable table = new LanceTable(CONNECTOR_ID_GENERATOR.getNextId().asLong(),
                        tblName, columns, uri, catalogName, dbName);
                addTable(dbName, table);
            }
        }
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return cloudConfiguration;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.LANCE;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        if (rest != null) {
            List<String> names = new ArrayList<>();
            for (var value : JsonParser.parseString(invoke("listNamespaces")).getAsJsonArray()) {
                List<String> id = new ArrayList<>();
                value.getAsJsonArray().forEach(part -> id.add(part.getAsString()));
                names.add(databaseName(id));
            }
            return names;
        }
        return ImmutableList.copyOf(databases.keySet());
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        if (rest != null) {
            List<String> names = new ArrayList<>();
            String json = invoke("listTables", new Gson().toJson(namespace(dbName)));
            JsonParser.parseString(json).getAsJsonArray().forEach(value -> names.add(value.getAsString()));
            return names;
        }
        List<Table> tableList = tables.get(dbName);
        if (tableList == null) {
            return ImmutableList.of();
        }
        return tableList.stream().map(Table::getName).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        if (rest != null) {
            return listDbNames(context).contains(dbName) ? new Database(remoteId("db:" + dbName), dbName) : null;
        }
        return databases.get(dbName);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        if (rest != null) {
            if (!listTableNames(context, dbName).contains(tblName)) {
                return null;
            }
            List<String> id = new ArrayList<>(namespace(dbName));
            id.add(tblName);
            String identifier = new Gson().toJson(id);
            var result = JsonParser.parseString(invoke("loadTable", identifier)).getAsJsonObject();
            List<Column> columns = new ArrayList<>();
            try {
                for (var field : Schema.fromJSON(result.get("schema").getAsString()).getFields()) {
                    columns.add(new Column(field.getName(), LanceApiConverter.fromArrowField(field), field.isNullable()));
                }
            } catch (java.io.IOException e) {
                throw new StarRocksConnectorException("Invalid Lance dataset schema");
            }
            LanceTable table = new LanceTable(remoteId("table:" + identifier), tblName, columns,
                    result.get("location").getAsString(), catalogName, dbName);
            table.setRestCatalog(properties.get("lance.catalog.uri"),
                    properties.getOrDefault("lance.catalog.bearer-token-file", ""), id, result.get("version").getAsLong());
            return table;
        }
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
