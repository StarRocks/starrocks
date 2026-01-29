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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.SchemaConstants;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.max;

public class VerticaSchemaResolver extends JDBCSchemaResolver {

    // Vertica internal system schemas to filter out
    private static final Set<String> INTERNAL_SCHEMAS = ImmutableSet.of(
            "v_catalog", "v_monitor", "v_internal", "v_internal_tables", "v_txtindex"
    );

    @Override
    public Collection<String> listSchemas(Connection connection) {
        try {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
                while (resultSet.next()) {
                    String schemaName = resultSet.getString("TABLE_SCHEM");
                    if (schemaName != null && !INTERNAL_SCHEMAS.contains(schemaName.toLowerCase(Locale.ROOT))) {
                        schemaNames.add(schemaName);
                    }
                }
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new StarRocksConnectorException("Failed to list Vertica schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        return connection.getMetaData().getTables(connection.getCatalog(), dbName, null,
                new String[] {"TABLE", "VIEW", "SYSTEM TABLE", "FLEX TABLE"});
    }

    @Override
    public ResultSet getColumns(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(connection.getCatalog(), dbName, tblName, "%");
    }

    @Override
    public List<Column> convertToSRTable(ResultSet columnSet) throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            Type type = convertColumnType(columnSet.getInt("DATA_TYPE"),
                    columnSet.getString("TYPE_NAME"),
                    columnSet.getInt("COLUMN_SIZE"),
                    columnSet.getInt("DECIMAL_DIGITS"));
            String columnName = columnSet.getString("COLUMN_NAME");
            // Quote case-sensitive identifiers (PostgreSQL-style)
            if (!columnName.equals(columnName.toLowerCase(Locale.ROOT))) {
                columnName = "\"" + columnName + "\"";
            }
            fullSchema.add(new Column(columnName, type,
                    columnSet.getString("IS_NULLABLE").equals(SchemaConstants.YES)));
        }
        return fullSchema;
    }

    @Override
    public Table getTable(long id, String name, List<Column> schema, String dbName,
                          String catalogName, Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        // Use Vertica's double-quote identifier style
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "\"" + dbName + "\"" + "." + "\"" + name + "\"");
        return new JDBCTable(id, name, schema, dbName, catalogName, newProp);
    }

    @Override
    public Table getTable(long id, String name, List<Column> schema, List<Column> partitionColumns,
                          String dbName, String catalogName, Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        // Use Vertica's double-quote identifier style
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "\"" + dbName + "\"" + "." + "\"" + name + "\"");
        return new JDBCTable(id, name, schema, partitionColumns, dbName, catalogName, newProp);
    }

    @Override
    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {
        String lowerTypeName = typeName.toLowerCase(Locale.ROOT);
        int parenPos = lowerTypeName.indexOf('(');
        if (parenPos != -1) {
            lowerTypeName = lowerTypeName.substring(0, parenPos).trim();
        }

        switch (lowerTypeName) {
            // Binary types
            case "binary":
            case "varbinary":
            case "long varbinary":
            case "bytea":
            case "raw":
                return ScalarType.createVarbinary(ScalarType.CATALOG_MAX_VARCHAR_LENGTH);

            // Boolean
            case "boolean":
                return ScalarType.createType(PrimitiveType.BOOLEAN);

            // Character types
            case "char":
                return ScalarType.createCharType(columnSize);
            case "varchar":
                return ScalarType.createVarcharType(columnSize);
            case "long varchar":
                return ScalarType.createVarcharType(ScalarType.getOlapMaxVarcharLength());

            // Date/Time types
            case "date":
                return ScalarType.createType(PrimitiveType.DATE);
            case "time":
            case "time with timezone":
            case "timetz":
                return ScalarType.createType(PrimitiveType.TIME);
            case "datetime":
            case "smalldatetime":
            case "timestamp":
            case "timestamptz":
            case "timestamp with timezone":
                return ScalarType.createType(PrimitiveType.DATETIME);
            case "interval":
            case "interval day":
            case "interval day to second":
            case "interval year":
            case "interval year to month":
                return ScalarType.createVarcharType(ScalarType.getOlapMaxVarcharLength());

            // Floating point types
            case "double precision":
            case "float":
            case "float8":
            case "real":
                return ScalarType.createType(PrimitiveType.DOUBLE);

            // Integer types - Vertica's INT is 64-bit
            case "tinyint":
                return ScalarType.createType(PrimitiveType.TINYINT);
            case "smallint":
                return ScalarType.createType(PrimitiveType.SMALLINT);
            case "int":
            case "integer":
            case "bigint":
            case "int8":
                return ScalarType.createType(PrimitiveType.BIGINT);

            // Decimal types
            case "decimal":
            case "numeric":
            case "number":
            case "money":
                return createDecimalType(columnSize, digits);

            // UUID
            case "uuid":
                return ScalarType.createVarcharType(36);

            default:
                break;
        }

        // Fallback: handle by JDBC type code
        PrimitiveType primitiveType;
        switch (dataType) {
            case Types.BIT:
            case Types.BOOLEAN:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case Types.TINYINT:
                primitiveType = PrimitiveType.TINYINT;
                break;
            case Types.SMALLINT:
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case Types.INTEGER:
                primitiveType = PrimitiveType.BIGINT; // Vertica INT is 64-bit
                break;
            case Types.BIGINT:
                primitiveType = PrimitiveType.BIGINT;
                break;
            case Types.REAL:
            case Types.FLOAT:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case Types.DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return createDecimalType(columnSize, digits);
            case Types.CHAR:
                return ScalarType.createCharType(columnSize);
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return ScalarType.createVarcharType(columnSize > 0 ? columnSize : ScalarType.getOlapMaxVarcharLength());
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return ScalarType.createVarbinary(ScalarType.CATALOG_MAX_VARCHAR_LENGTH);
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
            default:
                return ScalarType.createVarcharType(ScalarType.getOlapMaxVarcharLength());
        }
        return ScalarType.createType(primitiveType);
    }

    private Type createDecimalType(int columnSize, int digits) {
        int precision = columnSize + max(-digits, 0);
        if (precision == 0) {
            return ScalarType.createVarcharType(ScalarType.getOlapMaxVarcharLength());
        }
        return ScalarType.createUnifiedDecimalType(precision, max(digits, 0));
    }

    @Override
    public List<Partition> getPartitions(Connection connection, Table table) {
        return Lists.newArrayList(new Partition(table.getName(), TimeUtils.getEpochSeconds()));
    }
}
