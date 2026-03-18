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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;

public class OracleSchemaResolver extends JDBCSchemaResolver {

    public static final String ORACLE_NUMBER_DEFAULT_SCALE = "oracle.number.default-scale";
    public static final String ORACLE_NUMBER_ROUNDING_MODE = "oracle.number.rounding-mode";
    // control DATE / TIMESTAMP / TIMESTAMP WITH LOCAL TIME ZONE
    public static final String ORACLE_TEMPORAL_TO_DATETIME = "oracle.temporal.to-datetime";
    // control TIMESTAMP WITH TIME ZONE
    public static final String ORACLE_TIMESTAMPTZ_TO_DATETIME = "oracle.timestamptz.to-datetime";

    // Fallback scale used when Oracle does not provide explicit scale information (e.g. NUMBER).
    private final int defaultNumberScale;
    private final boolean timestampToDatetime;
    private final boolean timestampTzToDatetime;

    public OracleSchemaResolver() {
        this(Map.of());
    }

    public OracleSchemaResolver(Map<String, String> properties) {
        this.defaultTableTypes = new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"};
        String scaleValue = properties.getOrDefault(ORACLE_NUMBER_DEFAULT_SCALE, "6");
        this.timestampToDatetime = Boolean.parseBoolean(properties.getOrDefault(ORACLE_TEMPORAL_TO_DATETIME, "false"));
        this.timestampTzToDatetime = Boolean.parseBoolean(properties.getOrDefault(ORACLE_TIMESTAMPTZ_TO_DATETIME, "false"));

        int parsedScale;
        try {
            parsedScale = Integer.parseInt(scaleValue);
        } catch (NumberFormatException e) {
            parsedScale = 6;
        }
        // Scale must be non-negative and cannot exceed the maximum decimal precision (38).
        parsedScale = Math.min(Math.max(parsedScale, 0),
                PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128));
        this.defaultNumberScale = parsedScale;
    }

    @Override
    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        return connection.getMetaData().getTables(connection.getCatalog(), dbName, null, defaultTableTypes);
    }

    @Override
    public ResultSet getTables(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getTables(connection.getCatalog(), dbName, tblName, defaultTableTypes);
    }

    @Override
    public ResultSet getColumns(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(connection.getCatalog(), dbName, tblName, "%");
    }

    @Override
    public Table getTable(long id, String name, List<Column> schema, String dbName, String catalogName,
                          Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "\"" + dbName + "\"" + "." + "\"" + name + "\"");
        return new JDBCTable(id, name, schema, dbName, catalogName, newProp);
    }

    @Override
    public Table getTable(long id, String name, List<Column> schema, List<Column> partitionColumns, String dbName,
                          String catalogName, Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "\"" + dbName + "\"" + "." + "\"" + name + "\"");
        return new JDBCTable(id, name, schema, partitionColumns, dbName, catalogName, newProp);
    }

    @Override
    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {
        PrimitiveType primitiveType;
        switch (dataType) {
            case Types.SMALLINT:
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case Types.FLOAT:
            // BINARY_FLOAT
            case 100:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case Types.DOUBLE:
            // BINARY_DOUBLE
            case 101:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case Types.NUMERIC:
            // NUMBER
            case 3:
                primitiveType = PrimitiveType.DECIMAL128;
                break;
            case Types.CHAR:
            case Types.NCHAR:
                return TypeFactory.createCharType(columnSize);
            case Types.VARCHAR:
            // NVARCHAR2
            case Types.NVARCHAR:
                if (columnSize > 0) {
                    return TypeFactory.createVarcharType(columnSize);
                } else {
                    return TypeFactory.createVarcharType(TypeFactory.CATALOG_MAX_VARCHAR_LENGTH);
                }
            case Types.CLOB:
            case Types.NCLOB:
            // LONG
            case Types.LONGVARCHAR:
                return TypeFactory.createVarcharType(TypeFactory.CATALOG_MAX_VARCHAR_LENGTH);
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            // raw
            case 23:
                if (columnSize > 0) {
                    return TypeFactory.createVarbinary(columnSize);
                } else {
                    return TypeFactory.createVarbinary(TypeFactory.CATALOG_MAX_VARCHAR_LENGTH);
                }
            case Types.DATE:
                primitiveType = timestampToDatetime ? PrimitiveType.DATETIME : PrimitiveType.DATE;
                break;
            // Timestamp types are mapped to VARCHAR by default and can be promoted to DATETIME via catalog properties.
            case Types.TIMESTAMP:
            // TIMESTAMP WITH LOCAL TIME ZONE
            case -102:
                if (timestampToDatetime) {
                    primitiveType = PrimitiveType.DATETIME;
                } else {
                    return TypeFactory.createVarcharType(64);
                }
                break;
            // TIMESTAMP WITH TIME ZONE
            case -101:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                if (timestampTzToDatetime) {
                    primitiveType = PrimitiveType.DATETIME;
                } else {
                    return TypeFactory.createVarcharType(64);
                }
                break;

            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }

        if (primitiveType != PrimitiveType.DECIMAL128) {
            return TypeFactory.createType(primitiveType);
        } else {
            // Map negative scale to decimal(p + abs(s), 0) to preserve Oracle NUMBER integer width after rounding.
            int maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
            int precision = columnSize + max(-digits, 0);
            int scale = digits;
            boolean undefinedPrecisionNumber = (digits == -127) || (columnSize <= 0 && digits <= 0);

            if (undefinedPrecisionNumber) {
                precision = maxPrecision;
                scale = defaultNumberScale;
            }

            if (precision < scale) {
                scale = (maxPrecision > scale ? scale : maxPrecision);
                precision = scale;
            }

            // Ensure precision covers the chosen scale and does not exceed the maximum supported.
            if (precision > maxPrecision || precision <= 0) {
                throw new StarRocksConnectorException(String.format(
                        "Oracle column precision %d/column size %d exceeds StarRocks limit (%d) or is invalid; " +
                                "please redefine the column or set catalog properties %s/%s to control mapping.",
                        precision, columnSize, maxPrecision, ORACLE_NUMBER_DEFAULT_SCALE, ORACLE_NUMBER_ROUNDING_MODE));
            }

            return TypeFactory.createUnifiedDecimalType(precision, max(scale, 0));
        }
    }

    @Override
    public List<String> listPartitionNames(Connection connection, String databaseName, String tableName) {
        final String partitionNamesQuery = "SELECT PARTITION_NAME AS NAME FROM ALL_TAB_PARTITIONS " +
                "WHERE TABLE_OWNER = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL " +
                "ORDER BY PARTITION_POSITION";
        try (PreparedStatement ps = connection.prepareStatement(partitionNamesQuery)) {
            ps.setString(1, databaseName.toUpperCase());
            ps.setString(2, tableName.toUpperCase());
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            final ResultSet rs = ps.executeQuery();
            final ImmutableList.Builder<String> list = ImmutableList.builder();
            while (rs.next()) {
                final String partitionName = rs.getString("NAME");
                list.add(partitionName);
            }
            return list.build();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch partition names: " + e.getMessage(), e);
        }
    }

    /**
     * desc ALL_PART_KEY_COLUMNS;
     *  Name                       Null?    Type
     *  ----------------------------------------- -------- ----------------------------
     *  OWNER                            VARCHAR2(128)
     *  NAME                            VARCHAR2(128)
     *  OBJECT_TYPE                        CHAR(5)
     *  COLUMN_NAME                        VARCHAR2(4000)
     *  COLUMN_POSITION                    NUMBER
     *  COLLATED_COLUMN_ID                    NUMBER
     */
    @Override
    public List<String> listPartitionColumns(Connection connection, String databaseName, String tableName) {
        final String partitionColumnsQuery = "SELECT DISTINCT COLUMN_NAME FROM ALL_PART_KEY_COLUMNS " +
                "WHERE OWNER = ? AND NAME = ? ORDER BY COLUMN_POSITION";
        try (PreparedStatement ps = connection.prepareStatement(partitionColumnsQuery)) {
            ps.setString(1, databaseName.toUpperCase());
            ps.setString(2, tableName.toUpperCase());
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            final ResultSet rs = ps.executeQuery();
            final ImmutableList.Builder<String> list = ImmutableList.builder();
            while (rs.next()) {
                String partitionColumn = rs.getString("COLUMN_NAME");
                list.add(partitionColumn);
            }
            return list.build();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch partition columns: " + e.getMessage(), e);
        }
    }

    public List<Partition> getPartitions(Connection connection, Table table) {
        final JDBCTable jdbcTable = (JDBCTable) table;
        final String query = getPartitionQuery(table);
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, jdbcTable.getCatalogDBName());
            ps.setString(2, jdbcTable.getCatalogTableName());
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            final ResultSet rs = ps.executeQuery();
            final ImmutableList.Builder<Partition> list = ImmutableList.builder();
            long createTime = TimeUtils.getEpochSeconds();
            if (null != rs) {
                while (rs.next()) {
                    final String[] partitionNames = rs.getString("NAME").
                            replace("'", "").split(",");
                    try {
                        createTime = rs.getTimestamp("MODIFIED_TIME").getTime();
                    } catch (Exception e) {
                        // ignore
                    }
                    for (String partitionName : partitionNames) {
                        list.add(new Partition(partitionName, createTime));
                    }
                }
                final ImmutableList<Partition> partitions = list.build();
                return partitions.isEmpty()
                        ? Lists.newArrayList(new Partition(table.getName(), createTime))
                        : partitions;
            } else {
                return Lists.newArrayList(new Partition(table.getName(), createTime));
            }
        } catch (SQLException | NullPointerException e) {
            throw new StarRocksConnectorException(e.getMessage(), e);
        }
    }

    private static String getPartitionQuery(Table table) {
        final String partitionsQuery = "SELECT PARTITION_NAME AS NAME, " +
                "LAST_ANALYZED AS MODIFIED_TIME " +
                "FROM ALL_TAB_PARTITIONS WHERE TABLE_OWNER = ? AND TABLE_NAME = ? " +
                "AND PARTITION_NAME IS NOT NULL ORDER BY PARTITION_POSITION";
        final String nonPartitionQuery = "SELECT TABLE_NAME AS NAME, " +
                "LAST_ANALYZED AS MODIFIED_TIME " +
                "FROM ALL_TABLES WHERE OWNER = ? AND TABLE_NAME = ? ";
        return table.isUnPartitioned() ? nonPartitionQuery : partitionsQuery;
    }
}
