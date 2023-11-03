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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class JDBCMetadata implements ConnectorMetadata {

    private static Logger LOG = LogManager.getLogger(JDBCMetadata.class);

    private Map<String, String> properties;
    private String catalogName;
    private JDBCSchemaResolver schemaResolver;

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
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(properties.get(JDBCResource.URI),
                properties.get(JDBCResource.USER), properties.get(JDBCResource.PASSWORD));
    }

    @Override
    public List<String> listDbNames() {
        try (Connection connection = getConnection()) {
            return Lists.newArrayList(schemaResolver.listSchemas(connection));
        } catch (SQLException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
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
        try (Connection connection = getConnection()) {
            try (ResultSet resultSet = schemaResolver.getTables(connection, dbName)) {
                ImmutableList.Builder<String> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    list.add(tableName);
                }
                return list.build();
            }
        } catch (SQLException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try (Connection connection = getConnection()) {
            ResultSet columnSet = schemaResolver.getColumns(connection, dbName, tblName);
            List<Column> fullSchema = schemaResolver.convertToSRTable(columnSet);
            List<Column> partitionColumns = listPartitionColumns(dbName, tblName, fullSchema);
            if (fullSchema.isEmpty()) {
                return null;
            }
            JDBCTableName tableKey = JDBCTableName.of(catalogName, dbName, tblName);
            if (JDBCTableIdCache.containsTableId(tableKey)) {
                return schemaResolver.getTable(JDBCTableIdCache.getTableId(tableKey),
                        tblName, fullSchema, partitionColumns, dbName, catalogName, properties);
            } else {
                Integer tableId = ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt();
                JDBCTableIdCache.putTableId(tableKey, tableId);
                return schemaResolver.getTable(tableId, tblName, fullSchema, partitionColumns, dbName, catalogName, properties);
            }
        } catch (SQLException | DdlException e) {
            LOG.warn(e.getMessage());
            return null;
        }
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        try (Connection connection = getConnection()) {
            return schemaResolver.listPartitionNames(connection, databaseName, tableName);
        } catch (SQLException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    public List<Column> listPartitionColumns(String databaseName, String tableName, List<Column> fullSchema) {
        try (Connection connection = getConnection()) {
            Set<String> partitionColumnNames = schemaResolver.listPartitionColumns(connection, databaseName, tableName)
                    .stream().map(columnName -> columnName.toLowerCase()).collect(Collectors.toSet());
            if (partitionColumnNames.size() > 0) {
                return fullSchema.stream().filter(column -> partitionColumnNames.contains(column.getName().toLowerCase()))
                        .collect(Collectors.toList());
            } else {
                return Lists.newArrayList();
            }
        } catch (SQLException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        try (Connection connection = getConnection()) {
            List<Partition> partitions = schemaResolver.getPartitions(connection, table);
            String maxInt = IntLiteral.createMaxValue(Type.INT).getStringValue();
            String maxDate = DateLiteral.createMaxValue(Type.DATE).getStringValue();

            ImmutableList.Builder<PartitionInfo> list = ImmutableList.builder();
            if (!partitions.isEmpty()) {
                for (Partition partition : partitions) {
                    String partitionName = partition.getPartitionName();
                    if (partitionNames.contains(partitionName)) {
                        list.add(partition);
                    }
                    // Determine boundary value
                    if (partitionName.equalsIgnoreCase(PartitionUtil.MYSQL_PARTITION_MAXVALUE)) {
                        if (partitionNames.contains(maxInt) || partitionNames.contains(maxDate)) {
                            list.add(partition);
                        }
                    }
                }
                return list.build();
            } else {
                return Lists.newArrayList();
            }
        } catch (SQLException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

}
