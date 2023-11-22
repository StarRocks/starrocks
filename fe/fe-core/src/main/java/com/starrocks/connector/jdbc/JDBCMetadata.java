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


package com.starrocks.connector.jdbc;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class JDBCMetadata implements ConnectorMetadata {

    private static Logger LOG = LogManager.getLogger(JDBCMetadata.class);

    private Map<String, String> properties;
    private String catalogName;
    private JDBCSchemaResolver schemaResolver;
    private JDBCMetaResolver metaResolver;

    private final @NonNull JDBCAsyncCache<ImmutableMap<String, String>, List<String>> catalogCache
            = JDBCCacheBuilder.buildAsync();
    private final @NonNull JDBCAsyncCache<ImmutableMap<String, String>, Table> tableCache
            = JDBCCacheBuilder.buildAsync();
    private final @NonNull JDBCAsyncCache<ImmutableMap<String, Object>, List<PartitionInfo>> partitionCache
            = JDBCCacheBuilder.buildAsync();

    public JDBCMetadata(Map<String, String> properties, String catalogName) {
        this.properties = properties;
        this.catalogName = catalogName;
        try {
            Class.forName(properties.get(JDBCResource.DRIVER_CLASS));
        } catch (ClassNotFoundException e) {
            LOG.warn(e.getMessage());
            throw new StarRocksConnectorException("doesn't find class: " + e.getMessage());
        }
        if (properties.get(JDBCResource.DRIVER_CLASS).toLowerCase().contains("mysql")) {
            schemaResolver = new MysqlSchemaResolver();
        } else if (properties.get(JDBCResource.DRIVER_CLASS).toLowerCase().contains("postgresql")) {
            schemaResolver = new PostgresSchemaResolver();
        } else {
            LOG.warn("{} not support yet", properties.get(JDBCResource.DRIVER_CLASS));
            throw new StarRocksConnectorException(properties.get(JDBCResource.DRIVER_CLASS) + " not support yet");
        }
        metaResolver = new JDBCMetaResolver(properties, catalogName, schemaResolver);
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(properties.get(JDBCResource.URI),
                properties.get(JDBCResource.USER), properties.get(JDBCResource.PASSWORD));
    }


    @Override
    public List<String> listDbNames() {
        ImmutableMap<String, String> metaInfo =
                ImmutableMap.of("func", "listDbNames");
        return catalogCache.get(metaInfo, k -> metaResolver.refreshCacheForListDbNames());
    }

    @Override
    public Database getDb(String name) {
        try {
            if (listDbNames().contains(name)) {
                return new Database(0, name);
            } else {
                return null;
            }
        } catch (StarRocksConnectorException e) {
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        ImmutableMap<String, String> metaInfo =
                ImmutableMap.of("func", "listTableNames", "dbName", dbName);
        return catalogCache.get(metaInfo, k -> metaResolver.refreshCacheForListTableNames(k));
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        ImmutableMap<String, String> metaInfo =
                ImmutableMap.of("func", "listPartitionNames", "dbName", dbName, "tblName", tblName);
        return tableCache.get(metaInfo, k -> metaResolver.refreshCacheForGetTable(k));
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        ImmutableMap<String, String> metaInfo =
                ImmutableMap.of("func", "listPartitionNames", "databaseName", databaseName, "tableName", tableName);
        return catalogCache.get(metaInfo, k -> metaResolver.refreshCacheForListPartitionNames(k));
    }

    public List<Column> listPartitionColumns(String databaseName, String tableName, List<Column> fullSchema) {
        return metaResolver.listPartitionColumns(databaseName, tableName, fullSchema);
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        ImmutableMap<String, Object> metaInfo =
                ImmutableMap.of("func", "getPartitions", "table", table, "partitionNames", partitionNames);
        return partitionCache.get(metaInfo, k -> metaResolver.refreshCacheForGetPartitions(k));
    }
}
