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
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LanceTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalcUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class LanceMetadata implements ConnectorMetadata {
    private final String catalogName;
    private final CloudConfiguration cloudConfiguration;
    private final String warehouse;
    private final String rootDatabase;
    private final String storageOptions;
    private final LanceDirectoryCatalog directory;
    private final Map<String, Long> tableIds = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<String, List<Table>> tables = new ConcurrentHashMap<>();

    public LanceMetadata(String catalogName, Map<String, String> properties) {
        this(catalogName, properties, new LanceDirectoryCatalog());
    }

    LanceMetadata(String catalogName, Map<String, String> properties, LanceDirectoryCatalog directory) {
        this.catalogName = catalogName;
        String type = properties.getOrDefault("lance.catalog.type", "directory");
        if (!"directory".equalsIgnoreCase(type)) {
            throw new StarRocksConnectorException("Only lance.catalog.type=directory is supported; REST is not yet supported");
        }
        if (properties.containsKey("database") || properties.keySet().stream().anyMatch(key -> key.startsWith("table."))) {
            throw new StarRocksConnectorException("Use lance.catalog.warehouse and lance.namespace.root_database; "
                    + "table URIs and schemas are discovered from Lance datasets");
        }
        warehouse = properties.getOrDefault("lance.catalog.warehouse", "").trim();
        if (warehouse.isEmpty()) {
            throw new StarRocksConnectorException("lance.catalog.warehouse is required");
        }
        try {
            URI uri = URI.create(warehouse);
            if (uri.getRawQuery() != null || uri.getRawFragment() != null || uri.getRawAuthority() == null
                    && uri.getScheme() != null && !"file".equals(uri.getScheme())) {
                throw new IllegalArgumentException();
            }
            if (uri.getScheme() == null && !warehouse.startsWith("/")) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException e) {
            throw new StarRocksConnectorException("Invalid Lance warehouse URI; use an absolute path or storage URI "
                    + "without query parameters or fragments");
        }
        rootDatabase = properties.getOrDefault("lance.namespace.root_database", "default").trim();
        if (rootDatabase.isEmpty()) {
            throw new StarRocksConnectorException("lance.namespace.root_database must not be empty");
        }
        this.directory = directory;
        this.cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        this.storageOptions = new Gson().toJson(LanceStorageOptions.from(warehouse, cloudConfiguration));
        addDatabase(new Database(CONNECTOR_ID_GENERATOR.getNextId().asLong(), rootDatabase));
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
        return ImmutableList.copyOf(databases.keySet());
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        if (!databases.containsKey(dbName)) {
            return ImmutableList.of();
        }
        List<String> names = new ArrayList<>();
        if (rootDatabase.equals(dbName)) {
            JsonParser.parseString(directory.listTables(warehouse, storageOptions)).getAsJsonArray()
                    .forEach(name -> names.add(name.getAsString()));
        }
        tables.getOrDefault(dbName, List.of()).forEach(table -> names.add(table.getName()));
        return names.stream().distinct().sorted().collect(Collectors.toList());
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        return databases.get(dbName);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        Table registered = tables.getOrDefault(dbName, List.of()).stream()
                .filter(table -> table.getName().equalsIgnoreCase(tblName)).findFirst().orElse(null);
        if (registered != null || !rootDatabase.equals(dbName)) {
            return registered;
        }
        List<String> matches = listTableNames(context, dbName).stream()
                .filter(name -> name.equalsIgnoreCase(tblName)).collect(Collectors.toList());
        if (matches.isEmpty()) {
            return null;
        }
        if (matches.size() != 1) {
            throw new StarRocksConnectorException("Ambiguous Lance table name: " + tblName);
        }
        String name = matches.get(0);
        JsonObject description = JsonParser.parseString(directory.describeTable(warehouse, storageOptions, name))
                .getAsJsonObject();
        List<Column> columns = LanceApiConverter.fromSchema(description.getAsJsonObject("schema"));
        long id = tableIds.computeIfAbsent(name, ignored -> CONNECTOR_ID_GENERATOR.getNextId().asLong());
        return new LanceTable(id, name, columns, description.get("location").getAsString(), catalogName, dbName);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit,
                                         TvrVersionRange versionRange) {
        // Dataset statistics are not available yet. Leave predicate and LIMIT evaluation to the optimizer.
        return StatisticsCalcUtils.estimateScanColumns(table, columns, session)
                .setOutputRowCount(Config.default_statistics_output_row_count)
                .build();
    }

    // In-memory registrations used by planner test fixtures; production metadata comes from the directory.
    public void addDatabase(Database db) {
        databases.put(db.getFullName(), db);
    }

    public void addTable(String dbName, Table table) {
        tables.computeIfAbsent(dbName, k -> Lists.newCopyOnWriteArrayList()).add(table);
    }
}
