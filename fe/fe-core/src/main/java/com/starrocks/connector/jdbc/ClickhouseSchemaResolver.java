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
import com.starrocks.common.SchemaConstants;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.type.AggStateDesc;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClickhouseSchemaResolver extends JDBCSchemaResolver {
    Map<String, String> properties;

    public static final Set<String> SUPPORTED_TABLE_TYPES = new HashSet<>(
            Arrays.asList("LOG TABLE", "MEMORY TABLE", "TEMPORARY TABLE", "VIEW", "DICTIONARY", "SYSTEM TABLE",
                    "REMOTE TABLE", "TABLE"));

    public ClickhouseSchemaResolver(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Collection<String> listSchemas(Connection connection) {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
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
        return connection.getMetaData().getTables(connection.getCatalog(), dbName, null,
                SUPPORTED_TABLE_TYPES.toArray(new String[SUPPORTED_TABLE_TYPES.size()]));

    }

    @Override
    public ResultSet getColumns(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(connection.getCatalog(), dbName, tblName, "%");
    }


    @Override
    public List<Column> convertToSRTable(ResultSet columnSet) throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            String typeName = columnSet.getString("TYPE_NAME");
            Type type = convertColumnType(columnSet.getInt("DATA_TYPE"),
                    typeName,
                    columnSet.getInt("COLUMN_SIZE"),
                    columnSet.getInt("DECIMAL_DIGITS"));

            String comment = "";
            try {
                if (columnSet.getString("REMARKS") != null) {
                    comment = columnSet.getString("REMARKS");
                }
            } catch (SQLException ignored) {
            }

            Column column = new Column(columnSet.getString("COLUMN_NAME"), type,
                    columnSet.getString("IS_NULLABLE").equals(SchemaConstants.YES), comment);

            // Inject AGG_STATE_UNION metadata for AggregateFunction columns to support precise pushdown.
            if (typeName != null && (typeName.startsWith("AggregateFunction(")
                    || typeName.startsWith("SimpleAggregateFunction("))) {
                injectAggStateMetadata(column, typeName);
            }

            fullSchema.add(column);
        }
        return fullSchema;
    }

    private void injectAggStateMetadata(Column column, String typeName) {
        String inner;
        if (typeName.startsWith("AggregateFunction(")) {
            inner = typeName.substring("AggregateFunction(".length(), typeName.length() - 1);
        } else if (typeName.startsWith("SimpleAggregateFunction(")) {
            inner = typeName.substring("SimpleAggregateFunction(".length(), typeName.length() - 1);
        } else {
            return;
        }

        int firstComma = findFirstTopLevelComma(inner);
        if (firstComma > 0) {
            String funcName = inner.substring(0, firstComma).trim();
            if (typeName.startsWith("AggregateFunction(")) {
                int paramParen = funcName.indexOf('(');
                if (paramParen != -1) {
                    funcName = funcName.substring(0, paramParen).trim() + "Merge" + funcName.substring(paramParen);
                } else {
                    funcName = funcName + "Merge";
                }
            }
            String argTypeName = inner.substring(firstComma + 1).trim();
            Type argType = resolveInnerType(argTypeName);

            if (argType.isValid()) {
                AggStateDesc desc = new AggStateDesc(funcName, column.getType(),
                        Lists.newArrayList(argType), true);
                column.setAggregationType(AggregateType.AGG_STATE_UNION, true);
                column.setAggStateDesc(desc);
            }
        }
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
            case Types.NUMERIC:
                primitiveType = PrimitiveType.LARGEINT;
                break;
            case Types.FLOAT:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case Types.DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case Types.BOOLEAN:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case Types.VARCHAR:
                return TypeFactory.createVarcharType(65533);
            case Types.DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case Types.TIMESTAMP:
                primitiveType = PrimitiveType.DATETIME;
                break;
            case Types.DECIMAL:
                // Decimal(9,9), first 9 is precision, second 9 is scale
                if (typeName.startsWith("Nullable")) {
                    typeName = typeName.replace("Nullable", "");
                }
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
                    return TypeFactory.createUnifiedDecimalType(precision, scale);
                }
            case Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE:
                return TypeFactory.createVarcharType(65533);
            case Types.OTHER:
                // ClickHouse aggregate functions are reported as Types.OTHER by the JDBC driver.
                if (typeName != null && (typeName.startsWith("AggregateFunction(")
                        || typeName.startsWith("SimpleAggregateFunction("))) {
                    return resolveInnerType(typeName);
                }
                return TypeFactory.createType(PrimitiveType.UNKNOWN_TYPE);
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }
        return TypeFactory.createType(primitiveType);
    }

    /**
     * Finds the index of the first comma at bracket nesting depth 0 in {@code s}.
     * This correctly handles nested types such as {@code Decimal(9,2)} or
     * function names such as {@code quantile(0.5)}.
     *
     * @return index of the comma, or {@code -1} if not found.
     */
    private int findFirstTopLevelComma(String s) {
        int depth = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Resolves a raw ClickHouse type name string (possibly wrapped in {@code Nullable(...)})
     * directly to a StarRocks {@link Type}.
     * We map them to VARCHAR because the JDBC driver returns them as Strings.
     * TODO: The current implementation always returning VARCHAR is technically an incorrect
     * mapping.
     */
    private Type resolveInnerType(String innerTypeName) {
        return TypeFactory.createVarcharType(65533);
    }

}
