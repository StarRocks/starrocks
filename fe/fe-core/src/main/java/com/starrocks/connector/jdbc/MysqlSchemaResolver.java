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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;

import static java.lang.Math.max;

public class MysqlSchemaResolver extends JDBCSchemaResolver {

    @Override
    public Collection<String> listSchemas(Connection connection) {
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (!schemaName.equalsIgnoreCase("information_schema") &&
                        !schemaName.equalsIgnoreCase("mysql") &&
                        !schemaName.equalsIgnoreCase("performance_schema") &&
                        !schemaName.equalsIgnoreCase("sys")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    @Override
    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {
        PrimitiveType primitiveType;
        boolean isUnsigned = typeName.toLowerCase().contains("unsigned");

        switch (dataType) {
            case Types.BOOLEAN:
            case Types.BIT:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case Types.TINYINT:
                if (isUnsigned) {
                    primitiveType = PrimitiveType.SMALLINT;
                } else {
                    primitiveType = PrimitiveType.TINYINT;
                }
                break;
            case Types.SMALLINT:
                if (isUnsigned) {
                    primitiveType = PrimitiveType.INT;
                } else {
                    primitiveType = PrimitiveType.SMALLINT;
                }
                break;
            case Types.INTEGER:
                if (isUnsigned) {
                    primitiveType = PrimitiveType.BIGINT;
                } else {
                    primitiveType = PrimitiveType.INT;
                }
                break;
            case Types.BIGINT:
                if (isUnsigned) {
                    primitiveType = PrimitiveType.LARGEINT;
                } else {
                    primitiveType = PrimitiveType.BIGINT;
                }
                break;
            case Types.FLOAT:
            case Types.REAL: // real => short float
                primitiveType = PrimitiveType.FLOAT;
                break;
            case Types.DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case Types.DECIMAL:
                primitiveType = PrimitiveType.DECIMAL32;
                break;
            case Types.CHAR:
                return ScalarType.createCharType(columnSize);
            case Types.VARCHAR:
            case Types.LONGVARCHAR: //text type in mysql
                return ScalarType.createVarcharType(columnSize);
            case Types.DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case Types.TIMESTAMP:
                primitiveType = PrimitiveType.DATETIME;
                break;
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }

        if (primitiveType != PrimitiveType.DECIMAL32) {
            return ScalarType.createType(primitiveType);
        } else {
            int precision = columnSize + max(-digits, 0);
            return ScalarType.createUnifiedDecimalType(precision, max(digits, 0));
        }
    }

    @Override
    public List<String> listPartitionNames(Connection connection, String databaseName, String tableName) {
        String partitionNamesQuery =
                "SELECT PARTITION_DESCRIPTION as NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = ? " +
                "AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL";
        try (PreparedStatement ps = connection.prepareStatement(partitionNamesQuery)) {
            ps.setString(1, databaseName);
            ps.setString(2, tableName);
            ResultSet rs = ps.executeQuery();
            ImmutableList.Builder<String> list = ImmutableList.builder();
            if (null != rs) {
                while (rs.next()) {
                    String[] partitionNames = rs.getString("NAME").
                            replace("'", "").split(",");
                    for (String partitionName : partitionNames) {
                        list.add(partitionName);
                    }
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
        String partitionColumnsQuery = "SELECT DISTINCT PARTITION_EXPRESSION FROM INFORMATION_SCHEMA.PARTITIONS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL";
        try (PreparedStatement ps = connection.prepareStatement(partitionColumnsQuery)) {
            ps.setString(1, databaseName);
            ps.setString(2, tableName);
            ResultSet rs = ps.executeQuery();
            ImmutableList.Builder<String> list = ImmutableList.builder();
            if (null != rs) {
                while (rs.next()) {
                    String partitionColumn = rs.getString("PARTITION_EXPRESSION")
                            .replace("`", "");
                    list.add(partitionColumn);
                }
                return list.build();
            } else {
                return Lists.newArrayList();
            }
        } catch (SQLException | NullPointerException e) {
            throw new StarRocksConnectorException(e.getMessage(), e);
        }
    }

    public List<Partition> getPartitions(Connection connection, Table table) {
        JDBCTable jdbcTable = (JDBCTable) table;
        String query = getPartitionQuery(table);
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, jdbcTable.getDbName());
            ps.setString(2, jdbcTable.getJdbcTable());
            ResultSet rs = ps.executeQuery();
            ImmutableList.Builder<Partition> list = ImmutableList.builder();
            if (null != rs) {
                while (rs.next()) {
                    String[] partitionNames = rs.getString("NAME").
                            replace("'", "").split(",");
                    long createTime = rs.getDate("MODIFIED_TIME").getTime();
                    for (String partitionName : partitionNames) {
                        list.add(new Partition(partitionName, createTime));
                    }
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
    private static String getPartitionQuery(Table table) {
        final String partitionsQuery = "SELECT PARTITION_DESCRIPTION AS NAME, " +
                "IF(UPDATE_TIME IS NULL, CREATE_TIME, UPDATE_TIME) AS MODIFIED_TIME " +
                "FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                "AND PARTITION_NAME IS NOT NULL";
        final String nonPartitionQuery = "SELECT TABLE_NAME AS NAME, " +
                "IF(UPDATE_TIME IS NULL, CREATE_TIME, UPDATE_TIME) AS MODIFIED_TIME " +
                "FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ";
        return table.isUnPartitioned() ? nonPartitionQuery : partitionsQuery;
    }
}
