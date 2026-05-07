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
import com.starrocks.common.util.TimeUtils;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public List<Column> convertToSRTable(ResultSet columnSet, java.util.Map<String, Integer> originalJdbcTypes)
            throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            int dataType = columnSet.getInt("DATA_TYPE");
            String columnName = columnSet.getString("COLUMN_NAME");
            Type type = convertColumnType(dataType,
                    columnSet.getString("TYPE_NAME"),
                    columnSet.getInt("COLUMN_SIZE"),
                    columnSet.getInt("DECIMAL_DIGITS"));

            if (originalJdbcTypes != null) {
                originalJdbcTypes.put(columnName.toLowerCase(java.util.Locale.ROOT), dataType);
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
            // if user not specify numeric precision and scale, the default value is 0,
            // we can't defer the precision and scale, can only deal it as string.
            if (precision == 0) {
                return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
            }
            return TypeFactory.createUnifiedDecimalType(precision, max(digits, 0));
        }
    }

    public List<Partition> getPartitions(Connection connection, Table table) {
        return Lists.newArrayList(new Partition(table.getName(), TimeUtils.getEpochSeconds()));
    }

    private static boolean isTimeWithTimezoneTypeName(String typeName) {
        return "timetz".equalsIgnoreCase(typeName) || "time with time zone".equalsIgnoreCase(typeName);
    }

    private static boolean isTimestampWithTimezoneTypeName(String typeName) {
        return "timestamptz".equalsIgnoreCase(typeName) || "timestamp with time zone".equalsIgnoreCase(typeName);
    }

}
