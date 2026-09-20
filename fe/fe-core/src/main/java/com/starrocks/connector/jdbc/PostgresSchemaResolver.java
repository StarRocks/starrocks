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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.SchemaConstants;
import com.starrocks.type.ArrayType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.max;

public class PostgresSchemaResolver extends JDBCSchemaResolver {

    public PostgresSchemaResolver() {
        this.defaultTableTypes = new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"};
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
    public List<Column> convertToSRTable(ResultSet columnSet, Map<String, Integer> originalJdbcTypes,
                                         Map<String, String> originalJdbcTypeNames,
                                         Set<String> unboundedNumericColumns) throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            int dataType = columnSet.getInt("DATA_TYPE");
            String columnName = columnSet.getString("COLUMN_NAME");
            String typeName = columnSet.getString("TYPE_NAME");
            if (unboundedNumericColumns != null && isUnboundedNumeric(dataType, columnSet.getInt("COLUMN_SIZE"))) {
                unboundedNumericColumns.add(normalizeColumnName(columnName));
            }
            Type type = convertColumnType(dataType,
                    typeName,
                    columnSet.getInt("COLUMN_SIZE"),
                    columnSet.getInt("DECIMAL_DIGITS"));

            if (originalJdbcTypes != null) {
                originalJdbcTypes.put(columnName.toLowerCase(java.util.Locale.ROOT), dataType);
            }
            if (originalJdbcTypeNames != null && typeName != null) {
                originalJdbcTypeNames.put(normalizeColumnName(columnName), typeName);
            }

            String comment = "";
            // Add try-cache to prevent exceptions when the metadata of some databases does not contain REMARKS
            try {
                if (columnSet.getString("REMARKS") != null) {
                    comment = columnSet.getString("REMARKS");
                }
            } catch (SQLException ignored) { }

            columnName = normalizeColumnName(columnSet.getString("COLUMN_NAME"));
            fullSchema.add(new Column(columnName, type,
                    columnSet.getString("IS_NULLABLE").equals(SchemaConstants.YES), comment));
        }
        return fullSchema;
    }

    @Override
    public List<Column> convertToSRTable(ResultSetMetaData metaData, Map<String, Integer> originalJdbcTypes,
                                         Map<String, String> originalJdbcTypeNames,
                                         Set<String> unboundedNumericColumns) throws SQLException {
        List<Column> columns = super.convertToSRTable(metaData, originalJdbcTypes, originalJdbcTypeNames, null);
        if (unboundedNumericColumns != null) {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (isUnboundedNumeric(metaData.getColumnType(i), metaData.getPrecision(i))) {
                    unboundedNumericColumns.add(columns.get(i - 1).getName());
                }
            }
        }
        return columns;
    }

    private static boolean isUnboundedNumeric(int dataType, int precision) {
        return (dataType == Types.NUMERIC || dataType == Types.DECIMAL) && precision == 0;
    }

    @Override
    protected String normalizeColumnName(String columnName) {
        if (!columnName.equals(columnName.toLowerCase())) {
            return "\"" + columnName + "\"";
        }
        return columnName;
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
            case Types.ARRAY:
                // PostgreSQL reports built-in array names with an underscore prefix.
                // Dimensions and lower bounds belong to individual values, not the column type.
                if ("_text".equalsIgnoreCase(typeName) || "_varchar".equalsIgnoreCase(typeName)) {
                    return new ArrayType(TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength()));
                }
                return TypeFactory.createType(PrimitiveType.UNKNOWN_TYPE);
            case Types.BIT:
                primitiveType = PrimitiveType.BOOLEAN;
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
            case Types.REAL:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case Types.DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                primitiveType = PrimitiveType.DECIMAL32;
                break;
            case Types.CHAR:
                return TypeFactory.createCharType(columnSize);
            case Types.VARCHAR:
                if ("varchar".equalsIgnoreCase(typeName)) {
                    return TypeFactory.createVarcharType(columnSize);
                } else if ("text".equalsIgnoreCase(typeName)) {
                    return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
                }
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
            case Types.BINARY:
            case Types.VARBINARY:
                return TypeFactory.createVarbinary(TypeFactory.CATALOG_MAX_VARCHAR_LENGTH);
            case Types.DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                primitiveType = PrimitiveType.TIME;
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                primitiveType = PrimitiveType.DATETIME;
                break;
            case Types.OTHER:
                if ("json".equalsIgnoreCase(typeName) || "jsonb".equalsIgnoreCase(typeName)) {
                    primitiveType = PrimitiveType.JSON;
                    break;
                } else if ("uuid".equalsIgnoreCase(typeName)) {
                    return TypeFactory.createVarbinary(columnSize);
                } else if ("time".equalsIgnoreCase(typeName)
                        || "time without time zone".equalsIgnoreCase(typeName)) {
                    primitiveType = PrimitiveType.TIME;
                    break;
                } else if (isTimeWithTimezoneTypeName(typeName)) {
                    primitiveType = PrimitiveType.TIME;
                    break;
                } else if (isTimestampWithTimezoneTypeName(typeName)) {
                    primitiveType = PrimitiveType.DATETIME;
                    break;
                }
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }

        if ("uuid".equalsIgnoreCase(typeName)) {
            return TypeFactory.createVarbinary(columnSize);
        }

        if (primitiveType != PrimitiveType.DECIMAL32) {
            return TypeFactory.createType(primitiveType);
        } else {
            int precision = columnSize + max(-digits, 0);
            // Unconstrained PostgreSQL numeric has no column-wide precision or scale.
            // The JDBC reader checks this bounded mapping without rounding each selected value.
            if (columnSize == 0) {
                return TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 18);
            }
            return TypeFactory.createUnifiedDecimalType(precision, max(digits, 0));
        }
    }

    @Override
    public long getTableRowCount(Connection connection, String dbName, String tableName) throws SQLException {
        // pg_class.reltuples is updated by ANALYZE and auto-vacuum; it is an estimate, not exact.
        // The cast to bigint avoids returning a float to Java.
        String sql = "SELECT c.reltuples::bigint FROM pg_class c " +
                     "JOIN pg_namespace n ON c.relnamespace = n.oid " +
                     "WHERE n.nspname = ? AND c.relname = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, dbName);
            ps.setString(2, tableName);
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

    public List<Partition> getPartitions(Connection connection, Table table) {
        return Lists.newArrayList(new Partition(table.getName(), System.currentTimeMillis()));
    }

    private static boolean isTimeWithTimezoneTypeName(String typeName) {
        return "timetz".equalsIgnoreCase(typeName) || "time with time zone".equalsIgnoreCase(typeName);
    }

    private static boolean isTimestampWithTimezoneTypeName(String typeName) {
        return "timestamptz".equalsIgnoreCase(typeName) || "timestamp with time zone".equalsIgnoreCase(typeName);
    }

}
