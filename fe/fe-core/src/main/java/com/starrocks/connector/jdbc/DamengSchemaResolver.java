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

/**
 * Schema resolver for DM (Dameng) database.
 *
 * DM reuses many Oracle conventions: identifiers are quoted with double quotes and unquoted
 * identifiers fold to upper case on a case-sensitive instance, and the Oracle-compatible
 * dictionary views (ALL_TABLES, ALL_TAB_PARTITIONS, ALL_PART_KEY_COLUMNS) are available.
 * The JDBC driver is `dm.jdbc.driver.DmDriver` and the URL scheme is `jdbc:dm://host:port`.
 */
public class DamengSchemaResolver extends JDBCSchemaResolver {

    public static final String DAMENG_NUMBER_DEFAULT_SCALE = "dameng.number.default-scale";
    // control TIMESTAMP WITH TIME ZONE
    public static final String DAMENG_TIMESTAMPTZ_TO_DATETIME = "dameng.timestamptz.to-datetime";

    // Fallback scale used when DM metadata does not report precision/scale for NUMBER/DECIMAL.
    private final int defaultNumberScale;
    private final boolean timestamptzToDatetime;

    public DamengSchemaResolver() {
        this(Map.of());
    }

    public DamengSchemaResolver(Map<String, String> properties) {
        this.defaultTableTypes = new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"};
        String scaleValue = properties.getOrDefault(DAMENG_NUMBER_DEFAULT_SCALE, "6");
        this.timestamptzToDatetime = Boolean.parseBoolean(
                properties.getOrDefault(DAMENG_TIMESTAMPTZ_TO_DATETIME, "false"));

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

    // SYS holds the data dictionary, SYSSSO/CTISYS/SYSAUDITOR are system accounts for security,
    // full-text index and audit. SYSDBA is kept because it is the schema users commonly work in.
    @Override
    protected boolean isInternalSchema(String schemaName) {
        return schemaName.equalsIgnoreCase("SYS")
                || schemaName.equalsIgnoreCase("SYSSSO")
                || schemaName.equalsIgnoreCase("SYSAUDITOR")
                || schemaName.equalsIgnoreCase("CTISYS")
                || super.isInternalSchema(schemaName);
    }

    @Override
    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        // DM ignores the catalog argument but must NOT receive the schema in the catalog slot,
        // otherwise the driver returns tables of all schemas.
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
            case Types.TINYINT:
                primitiveType = PrimitiveType.TINYINT;
                break;
            case Types.SMALLINT:
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case Types.INTEGER:
                primitiveType = PrimitiveType.INT;
                break;
            case Types.BIGINT:
                primitiveType = PrimitiveType.BIGINT;
                break;
            case Types.BOOLEAN:
            case Types.BIT:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            // DM REAL / BINARY_FLOAT are single-precision floats
            case Types.REAL:
                primitiveType = PrimitiveType.FLOAT;
                break;
            // DM FLOAT is a double-precision binary float
            case Types.FLOAT:
            // BINARY_DOUBLE
            case Types.DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            // DM reports NUMBER/DECIMAL/NUMERIC/DEC columns as Types.DECIMAL (3)
            case Types.DECIMAL:
            case Types.NUMERIC:
                primitiveType = PrimitiveType.DECIMAL128;
                break;
            case Types.CHAR:
            case Types.NCHAR:
                return TypeFactory.createCharType(max(columnSize, 1));
            case Types.VARCHAR:
            // VARCHAR2 / NVARCHAR
            case Types.NVARCHAR:
                if (columnSize > 0) {
                    return TypeFactory.createVarcharType(columnSize);
                } else {
                    return TypeFactory.createVarcharType(TypeFactory.CATALOG_MAX_VARCHAR_LENGTH);
                }
            // TEXT
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return TypeFactory.createVarcharType(TypeFactory.CATALOG_MAX_VARCHAR_LENGTH);
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                if (columnSize > 0 && columnSize <= TypeFactory.CATALOG_MAX_VARCHAR_LENGTH) {
                    return TypeFactory.createVarbinary(columnSize);
                } else {
                    return TypeFactory.createVarbinary(TypeFactory.CATALOG_MAX_VARCHAR_LENGTH);
                }
            // DM DATE is a pure date (year-month-day) without a time part
            case Types.DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case Types.TIME:
                primitiveType = PrimitiveType.TIME;
                break;
            case Types.TIMESTAMP:
                primitiveType = PrimitiveType.DATETIME;
                break;
            // DM reports TIMESTAMP WITH TIME ZONE as 95 ("DATETIME WITH TIME ZONE") from
            // DatabaseMetaData#getColumns and as Types.TIMESTAMP_WITH_TIMEZONE (2014) from
            // ResultSetMetaData.
            case 95:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                if (timestamptzToDatetime) {
                    primitiveType = PrimitiveType.DATETIME;
                } else {
                    return TypeFactory.createVarcharType(64);
                }
                break;
            // TIME WITH TIME ZONE: DM reports the non-standard code 94 from
            // DatabaseMetaData#getColumns, the standard code is 2013. INTERVAL types are
            // reported as 110 from getColumns and as Types.OTHER from ResultSetMetaData.
            // None of them has a StarRocks counterpart, keep the value readable as string.
            case 94:
            case Types.TIME_WITH_TIMEZONE:
            case 110:
            case Types.OTHER:
                return TypeFactory.createVarcharType(64);
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }

        if (primitiveType != PrimitiveType.DECIMAL128) {
            return TypeFactory.createType(primitiveType);
        } else {
            // Map negative scale to decimal(p + abs(s), 0) to preserve the integer width after rounding.
            int maxPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
            int precision = columnSize + max(-digits, 0);
            int scale = digits;
            // DM reports precision 0 for NUMBER/DECIMAL columns declared without precision and
            // scale; such columns store values like Oracle NUMBER and need a fallback scale.
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
                        "DM column precision %d/column size %d exceeds StarRocks limit (%d) or is invalid; " +
                                "please redefine the column or set catalog properties %s to control mapping.",
                        precision, columnSize, maxPrecision, DAMENG_NUMBER_DEFAULT_SCALE));
            }

            return TypeFactory.createUnifiedDecimalType(precision, max(scale, 0));
        }
    }

    @Override
    public boolean checkAndSetSupportPartitionInformation(Connection connection) {
        // ALL_TAB_PARTITIONS / ALL_PART_KEY_COLUMNS are available on regular DM deployments;
        // fall back to non-partitioned handling when they are not queryable.
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT COLUMN_NAME FROM ALL_PART_KEY_COLUMNS WHERE OWNER IS NULL")) {
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            try (ResultSet ignored = ps.executeQuery()) {
                this.supportPartitionInformation = true;
            }
        } catch (SQLException e) {
            this.supportPartitionInformation = false;
        }
        return this.supportPartitionInformation;
    }

    @Override
    public List<String> listPartitionNames(Connection connection, String databaseName, String tableName) {
        final String partitionNamesQuery = "SELECT PARTITION_NAME AS NAME FROM ALL_TAB_PARTITIONS " +
                "WHERE TABLE_OWNER = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL " +
                "ORDER BY PARTITION_POSITION";
        try (PreparedStatement ps = connection.prepareStatement(partitionNamesQuery)) {
            ps.setString(1, unqualifiedName(databaseName));
            ps.setString(2, unqualifiedName(tableName));
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            final ResultSet rs = ps.executeQuery();
            final ImmutableList.Builder<String> list = ImmutableList.builder();
            while (rs.next()) {
                final String partitionName = rs.getString(1);
                if (partitionName != null) {
                    list.add(partitionName);
                }
            }
            return list.build();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch partition names: " + e.getMessage(), e);
        }
    }

    // DM's ALL_PART_KEY_COLUMNS exposes OWNER, table NAME and COLUMN_NAME all under the column
    // name "NAME", so the values are read positionally instead of by column label.
    @Override
    public List<String> listPartitionColumns(Connection connection, String databaseName, String tableName) {
        final String partitionColumnsQuery = "SELECT OWNER, NAME, COLUMN_NAME, COLUMN_POSITION " +
                "FROM ALL_PART_KEY_COLUMNS WHERE OWNER = ? AND NAME = ? ORDER BY COLUMN_POSITION";
        try (PreparedStatement ps = connection.prepareStatement(partitionColumnsQuery)) {
            ps.setString(1, unqualifiedName(databaseName));
            ps.setString(2, unqualifiedName(tableName));
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            final ResultSet rs = ps.executeQuery();
            final ImmutableList.Builder<String> list = ImmutableList.builder();
            while (rs.next()) {
                final String partitionColumn = rs.getString(3);
                if (partitionColumn != null) {
                    list.add(partitionColumn);
                }
            }
            return list.build();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to fetch partition columns: " + e.getMessage(), e);
        }
    }

    @Override
    public List<Partition> getPartitions(Connection connection, Table table) {
        final JDBCTable jdbcTable = (JDBCTable) table;
        final String query = getPartitionQuery(table);
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, unqualifiedName(jdbcTable.getCatalogDBName()));
            ps.setString(2, unqualifiedName(jdbcTable.getCatalogTableName()));
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            final ResultSet rs = ps.executeQuery();
            final ImmutableList.Builder<Partition> list = ImmutableList.builder();
            long createTime = System.currentTimeMillis();
            if (null != rs) {
                while (rs.next()) {
                    final String[] partitionNames = rs.getString(1).
                            replace("'", "").split(",");
                    try {
                        createTime = rs.getTimestamp(2).getTime();
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

    // The catalog table name of a JDBCTable is stored as a quoted qualified name
    // (e.g. "SRTEST"."PART_T") by getTable(); the dictionary views expect the plain
    // unquoted identifier in upper case.
    private static String unqualifiedName(String qualifiedName) {
        if (qualifiedName == null) {
            return null;
        }
        String name = qualifiedName;
        int lastDot = name.lastIndexOf('.');
        if (lastDot >= 0) {
            name = name.substring(lastDot + 1);
        }
        return name.replace("\"", "").trim().toUpperCase();
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

    @Override
    public long getTableRowCount(Connection connection, String dbName, String tableName) throws SQLException {
        // ALL_TABLES.NUM_ROWS is populated by DBMS_STATS; it is NULL before statistics are
        // gathered, in which case the default row count is used.
        String sql = "SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER = ? AND TABLE_NAME = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, unqualifiedName(dbName));
            ps.setString(2, unqualifiedName(tableName));
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long rows = rs.getLong(1);
                    return rs.wasNull() ? -1L : rows;
                }
            }
        }
        return -1L;
    }
}
