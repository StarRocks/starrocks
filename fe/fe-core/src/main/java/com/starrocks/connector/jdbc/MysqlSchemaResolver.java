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
    public boolean checkAndSetSupportPartitionInformation(Connection connection) {
        String catalogSchema = "information_schema";
        String partitionInfoTable = "partitions";
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
                "AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL " +
                "AND ( PARTITION_METHOD = 'RANGE' or PARTITION_METHOD = 'RANGE COLUMNS') ORDER BY PARTITION_DESCRIPTION";
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
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL " +
                "AND ( PARTITION_METHOD = 'RANGE' or PARTITION_METHOD = 'RANGE COLUMNS')";
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
                    long createTime = rs.getTimestamp("MODIFIED_TIME").getTime();
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

    /**
     * Fetch jdbc table's partition info from `INFORMATION_SCHEMA.PARTITIONS`.
     * eg:
     * mysql> desc INFORMATION_SCHEMA.PARTITIONS;
     * +-------------------------------+---------------------+------+-----+---------+-------+
     * | Field                         | Type                | Null | Key | Default | Extra |
     * +-------------------------------+---------------------+------+-----+---------+-------+
     * | TABLE_CATALOG                 | varchar(512)        | NO   |     |         |       |
     * | TABLE_SCHEMA                  | varchar(64)         | NO   |     |         |       |
     * | TABLE_NAME                    | varchar(64)         | NO   |     |         |       |
     * | PARTITION_NAME                | varchar(64)         | YES  |     | NULL    |       |
     * | SUBPARTITION_NAME             | varchar(64)         | YES  |     | NULL    |       |
     * | PARTITION_ORDINAL_POSITION    | bigint(21) unsigned | YES  |     | NULL    |       |
     * | SUBPARTITION_ORDINAL_POSITION | bigint(21) unsigned | YES  |     | NULL    |       |
     * | PARTITION_METHOD              | varchar(18)         | YES  |     | NULL    |       |
     * | SUBPARTITION_METHOD           | varchar(12)         | YES  |     | NULL    |       |
     * | PARTITION_EXPRESSION          | longtext            | YES  |     | NULL    |       |
     * | SUBPARTITION_EXPRESSION       | longtext            | YES  |     | NULL    |       |
     * | PARTITION_DESCRIPTION         | longtext            | YES  |     | NULL    |       |
     * | TABLE_ROWS                    | bigint(21) unsigned | NO   |     | 0       |       |
     * | AVG_ROW_LENGTH                | bigint(21) unsigned | NO   |     | 0       |       |
     * | DATA_LENGTH                   | bigint(21) unsigned | NO   |     | 0       |       |
     * | MAX_DATA_LENGTH               | bigint(21) unsigned | YES  |     | NULL    |       |
     * | INDEX_LENGTH                  | bigint(21) unsigned | NO   |     | 0       |       |
     * | DATA_FREE                     | bigint(21) unsigned | NO   |     | 0       |       |
     * | CREATE_TIME                   | datetime            | YES  |     | NULL    |       |
     * | UPDATE_TIME                   | datetime            | YES  |     | NULL    |       |
     * | CHECK_TIME                    | datetime            | YES  |     | NULL    |       |
     * | CHECKSUM                      | bigint(21) unsigned | YES  |     | NULL    |       |
     * | PARTITION_COMMENT             | varchar(80)         | NO   |     |         |       |
     * | NODEGROUP                     | varchar(12)         | NO   |     |         |       |
     * | TABLESPACE_NAME               | varchar(64)         | YES  |     | NULL    |       |
     * +-------------------------------+---------------------+------+-----+---------+-------+
     * @param table
     * @return
     */
    @NotNull
    private static String getPartitionQuery(Table table) {
        final String partitionsQuery = "SELECT PARTITION_DESCRIPTION AS NAME, " +
                "IF(UPDATE_TIME IS NULL, CREATE_TIME, UPDATE_TIME) AS MODIFIED_TIME " +
                "FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? " +
                "AND PARTITION_NAME IS NOT NULL " +
                "AND ( PARTITION_METHOD = 'RANGE' or PARTITION_METHOD = 'RANGE COLUMNS') ORDER BY PARTITION_DESCRIPTION";
        final String nonPartitionQuery = "SELECT TABLE_NAME AS NAME, " +
                "IF(UPDATE_TIME IS NULL, CREATE_TIME, UPDATE_TIME) AS MODIFIED_TIME " +
                "FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ";
        return table.isUnPartitioned() ? nonPartitionQuery : partitionsQuery;
    }
}
