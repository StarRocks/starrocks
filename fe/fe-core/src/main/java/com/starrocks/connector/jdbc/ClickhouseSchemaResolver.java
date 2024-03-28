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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ClickhouseSchemaResolver extends JDBCSchemaResolver {
    Map<String, String> properties;
    public static final String KEY_FOR_TABLE_NAME_FOR_PARTITION_INFO = "table_name_for_partition_info";
    public static final String KEY_FOR_TABLE_NAME_FOR_TABLE_INFO = "table_name_for_table_info";
    private static final String DEFAULT_TABLE_NAME_FOR_PARTITION_INFO = "system.parts";
    private static final String DEFAULT_TABLE_NAME_FOR_TABLE_INFO = "system.tables";
    private static final Set<String> SUPPORTED_TABLE_TYPES = new HashSet<>(
            Arrays.asList("LOG TABLE", "MEMORY TABLE", "TEMPORARY TABLE", "VIEW", "DICTIONARY", "SYSTEM TABLE",
                    "REMOTE TABLE", "TABLE"));

    public ClickhouseSchemaResolver(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Collection<String> listSchemas(Connection connection) {

        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("NAME");
                // skip internal schemas
                if (!schemaName.equalsIgnoreCase("INFORMATION_SCHEMA") && !schemaName.equalsIgnoreCase("system")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
    }


    @Override
    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        String tableTypes = properties.get("table_types");
        if (null != tableTypes) {
            String[] tableTypesArray = tableTypes.split(",");
            if (tableTypesArray.length == 0) {
                throw new StarRocksConnectorException("table_types should be populated with table types separated by " +
                        "comma, e.g. 'TABLE,VIEW'. Currently supported type includes:" +
                        String.join(",", SUPPORTED_TABLE_TYPES));
            }

            for (String tt : tableTypesArray) {
                if (!SUPPORTED_TABLE_TYPES.contains(tt)) {
                    throw new StarRocksConnectorException("Unsupported table type found: " + tt,
                            ",Currently supported table types includes:" + String.join(",", SUPPORTED_TABLE_TYPES));
                }
            }
            return connection.getMetaData().getTables(connection.getCatalog(), dbName, null, tableTypesArray);
        }
        return connection.getMetaData().getTables(connection.getCatalog(), dbName, null, new String[] {"TABLE", "VIEW"});

    }

    @Override
    public ResultSet getColumns(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(connection.getCatalog(), dbName, tblName, "%");
    }

    @Override
    public boolean checkAndSetSupportPartitionInformation(Connection connection) {
        // The architecture of user's clickhouse is undermined, so we allow user to specify <part_into_table>
        // and fall back to clickhouse's default system table for partition information, i.e. `system.parts`.
        String tableNameForPartInfo = getTableNameForPartInfo();
        String[] schemaAndTable = tableNameForPartInfo.split("\\.");
        if (schemaAndTable.length != 2) {
            throw new StarRocksConnectorException(String.format("Invalid table name for partition information: %s," +
                    "Please specify the full table name  <schema>.<table_name>", tableNameForPartInfo));
        }
        String catalogSchema = schemaAndTable[0];
        String partitionInfoTable = schemaAndTable[1];
        // Different types of MySQL protocol databases have different case names for schema and table names,
        // which need to be converted to lowercase for comparison
        try (ResultSet catalogSet = connection.getMetaData().getCatalogs()) {
            while (catalogSet.next()) {
                String schemaName = catalogSet.getString("TABLE_CAT");
                if (schemaName.equalsIgnoreCase(catalogSchema)) {
                    try (ResultSet tableSet = connection.getMetaData().getTables(catalogSchema, null, null, null)) {
                        while (tableSet.next()) {
                            String tableName = tableSet.getString("TABLE_NAME");
                            if (tableName.equalsIgnoreCase(partitionInfoTable)) {
                                return this.supportPartitionInformation = true;
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
        return this.supportPartitionInformation = false;
    }

    private String getTableNameForPartInfo() {
        return properties.getOrDefault(KEY_FOR_TABLE_NAME_FOR_PARTITION_INFO, DEFAULT_TABLE_NAME_FOR_PARTITION_INFO);
    }

    private String getTableNameForTableInfo() {
        return properties.getOrDefault(KEY_FOR_TABLE_NAME_FOR_TABLE_INFO, DEFAULT_TABLE_NAME_FOR_TABLE_INFO);
    }

    @Override
    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {

        PrimitiveType primitiveType;
        boolean isUnsigned = typeName.toLowerCase().startsWith("uint");

        switch (typeName) {
            case "Int8":
                primitiveType = PrimitiveType.TINYINT;
                break;
            case "UInt8":
            case "Int16":
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case "UInt16":
            case "Int32":
                primitiveType = PrimitiveType.INT;
                break;
            case "UInt32":
            case "Int64":
                primitiveType = PrimitiveType.BIGINT;
                break;
            case "UInt64":
            case "Int128":
            case "UInt128":
            case "Int256":
            case "UInt256":
                primitiveType = PrimitiveType.LARGEINT;
                break;
            case "Float32":
                primitiveType = PrimitiveType.FLOAT;
                break;
            case "Float64":
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case "Bool":
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case "String":
                return ScalarType.createVarcharType(65533);
            case "Date":
                primitiveType = PrimitiveType.DATE;
                break;
            case "DateTime":
                primitiveType = PrimitiveType.DATETIME;
                break;
            default:
                // Decimal(9,9), first 9 is precision, second 9 is scale
                if (typeName.startsWith("Decimal")) {
                    String[] precisionAndScale =
                            typeName.replace("Decimal", "").replace("(", "")
                                    .replace(")", "").replace(" ", "")
                                    .split(",");
                    if (precisionAndScale.length != 2) {
                        // should not go here, but if it does, we make it DECIMALV2.
                        throw new StarRocksConnectorException(
                                "Cannot extract precision and scale from Decimal typename:" + typeName);
                    } else {
                        int precision = Integer.parseInt(precisionAndScale[0]);
                        int scale = Integer.parseInt(precisionAndScale[1]);
                        return ScalarType.createUnifiedDecimalType(precision, scale);
                    }
                }
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }
        return ScalarType.createType(primitiveType);
    }

    @Override
    public List<String> listPartitionNames(Connection connection, String databaseName, String tableName) {
        String tableNameForPartInfo = getTableNameForPartInfo();
        String partitionNamesQuery = "SELECT DISTINCT partition FROM " + tableNameForPartInfo + " WHERE database = ? " +
                "AND table = ? AND name IS NOT NULL " + "ORDER BY name";
        try (PreparedStatement ps = connection.prepareStatement(partitionNamesQuery)) {
            ps.setString(1, databaseName);
            ps.setString(2, tableName);
            ResultSet rs = ps.executeQuery();
            ImmutableList.Builder<String> list = ImmutableList.builder();
            if (null != rs) {
                while (rs.next()) {
                    String partitionName = rs.getString("name");
                    list.add(partitionName);
                }
                return list.build();
            } else {
                return Lists.newArrayList();
            }
        } catch (SQLException | NullPointerException e) {
            throw new StarRocksConnectorException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> listPartitionColumns(Connection connection, String databaseName, String tableName) {
        String tableNameForTableInfo = getTableNameForTableInfo();
        String localTableName = getLocalTableNameFromDistributedTableName(tableName);

        String partitionColumnsQuery = "SELECT DISTINCT partition_key FROM " + tableNameForTableInfo +
                " WHERE database = ? AND name = ? AND partition_key IS NOT NULL ";
        try (PreparedStatement ps = connection.prepareStatement(partitionColumnsQuery)) {
            ps.setString(1, databaseName);
            ps.setString(2, localTableName);
            ResultSet rs = ps.executeQuery();
            ImmutableList.Builder<String> list = ImmutableList.builder();
            if (null != rs) {
                while (rs.next()) {
                    String partitionColumn = rs.getString("PARTITION_EXPRESSION").replace("`", "");
                    list.add(partitionColumn);
                }
                try {
                    rs.close();
                } catch (Exception e) {
                    ;
                }
                return list.build();
            } else {
                return Lists.newArrayList();
            }
        } catch (SQLException | NullPointerException e) {
            throw new StarRocksConnectorException(e.getMessage(), e);
        }
    }

    @NotNull
    private String getLocalTableNameFromDistributedTableName(String tableName) {
        String prefix = properties.getOrDefault("prefix_for_local_table", "");
        String suffix = properties.getOrDefault("suffix_for_local_table", "");
        String localTableName = prefix + tableName + suffix;
        return localTableName;
    }

    public List<Partition> getPartitions(Connection connection, Table table) {
        JDBCTable jdbcTable = (JDBCTable) table;
        String query = getPartitionQuery(table);
        String localTableName = getLocalTableNameFromDistributedTableName(jdbcTable.getJdbcTable());
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, jdbcTable.getDbName());
            ps.setString(2, localTableName);
            ResultSet rs = ps.executeQuery();
            ImmutableList.Builder<Partition> list = ImmutableList.builder();
            if (null != rs) {
                while (rs.next()) {
                    String partitionName = rs.getString("NAME");
                    long createTime = rs.getTimestamp("MODIFIED_TIME").getTime();
                    list.add(new Partition(partitionName, createTime));
                }
                return list.build();
            } else {
                return Lists.newArrayList();
            }
        } catch (SQLException | NullPointerException e) {
            throw new StarRocksConnectorException(e.getMessage(), e);
        }
    }

    /**
     * Fetch jdbc table's partition info from `system.parts` or user-specified table with the same table structure.
     * eg:
     * clickhouse> desc system.parts;
     * ┌─name──────────────────────────────────┬─type────────────┬─default_type─┬─default_expression─┬
     * │ partition                             │ String          │              │                    │
     * │ name                                  │ String          │              │                    │
     * │ uuid                                  │ UUID            │              │                    │
     * │ part_type                             │ String          │              │                    │
     * │ active                                │ UInt8           │              │                    │
     * │ marks                                 │ UInt64          │              │                    │
     * │ rows                                  │ UInt64          │              │                    │
     * │ bytes_on_disk                         │ UInt64          │              │                    │
     * │ data_compressed_bytes                 │ UInt64          │              │                    │
     * │ data_uncompressed_bytes               │ UInt64          │              │                    │
     * │ marks_bytes                           │ UInt64          │              │                    │
     * │ secondary_indices_compressed_bytes    │ UInt64          │              │                    │
     * │ secondary_indices_uncompressed_bytes  │ UInt64          │              │                    │
     * │ secondary_indices_marks_bytes         │ UInt64          │              │                    │
     * │ modification_time                     │ DateTime        │              │                    │
     * │ remove_time                           │ DateTime        │              │                    │
     * │ refcount                              │ UInt32          │              │                    │
     * │ min_date                              │ Date            │              │                    │
     * │ max_date                              │ Date            │              │                    │
     * │ min_time                              │ DateTime        │              │                    │
     * │ max_time                              │ DateTime        │              │                    │
     * │ partition_id                          │ String          │              │                    │
     * │ min_block_number                      │ Int64           │              │                    │
     * │ max_block_number                      │ Int64           │              │                    │
     * │ level                                 │ UInt32          │              │                    │
     * │ data_version                          │ UInt64          │              │                    │
     * │ primary_key_bytes_in_memory           │ UInt64          │              │                    │
     * │ primary_key_bytes_in_memory_allocated │ UInt64          │              │                    │
     * │ is_frozen                             │ UInt8           │              │                    │
     * │ database                              │ String          │              │                    │
     * │ table                                 │ String          │              │                    │
     * │ engine                                │ String          │              │                    │
     * │ disk_name                             │ String          │              │                    │
     * │ path                                  │ String          │              │                    │
     * │ hash_of_all_files                     │ String          │              │                    │
     * │ hash_of_uncompressed_files            │ String          │              │                    │
     * │ uncompressed_hash_of_compressed_files │ String          │              │                    │
     * │ delete_ttl_info_min                   │ DateTime        │              │                    │
     * │ delete_ttl_info_max                   │ DateTime        │              │                    │
     * │ move_ttl_info.expression              │ Array(String)   │              │                    │
     * │ move_ttl_info.min                     │ Array(DateTime) │              │                    │
     * │ move_ttl_info.max                     │ Array(DateTime) │              │                    │
     * │ default_compression_codec             │ String          │              │                    │
     * │ recompression_ttl_info.expression     │ Array(String)   │              │                    │
     * │ recompression_ttl_info.min            │ Array(DateTime) │              │                    │
     * │ recompression_ttl_info.max            │ Array(DateTime) │              │                    │
     * │ group_by_ttl_info.expression          │ Array(String)   │              │                    │
     * │ group_by_ttl_info.min                 │ Array(DateTime) │              │                    │
     * │ group_by_ttl_info.max                 │ Array(DateTime) │              │                    │
     * │ rows_where_ttl_info.expression        │ Array(String)   │              │                    │
     * │ rows_where_ttl_info.min               │ Array(DateTime) │              │                    │
     * │ rows_where_ttl_info.max               │ Array(DateTime) │              │                    │
     * │ projections                           │ Array(String)   │              │                    │
     * │ bytes                                 │ UInt64          │ ALIAS        │ bytes_on_disk      │
     * │ marks_size                            │ UInt64          │ ALIAS        │ marks_bytes        │
     * └───────────────────────────────────────┴─────────────────┴──────────────┴────────────────────┴
     *
     * @param table
     * @return
     */
    @NotNull
    private String getPartitionQuery(Table table) {
        if (table.isPartitioned()) {
            String tableNameForPartInfo = getTableNameForPartInfo();
            final String partitionQuery =
                    "SELECT  partition AS NAME, max(modification_time) AS MODIFIED_TIME FROM " + tableNameForPartInfo +
                            " WHERE database = ? " + "AND table = ? AND name IS NOT NULL" +
                            " GROUP BY partition ORDER BY partition";
            return partitionQuery;
        } else {
            String tableNameForTableInfo = getTableNameForTableInfo();
            final String nonPartitionQuery =
                    " SELECT  name AS NAME, max(metadata_modification_time) AS MODIFIED_TIME FROM " +
                            tableNameForTableInfo +
                            " WHERE database = ? AND name = ? AND name IS NOT NULL GROUP BY name";
            return nonPartitionQuery;
        }
    }
}
