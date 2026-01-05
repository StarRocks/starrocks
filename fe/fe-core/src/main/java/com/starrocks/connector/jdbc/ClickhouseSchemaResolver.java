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
import com.starrocks.connector.exception.StarRocksConnectorException;
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

    /**
     * Extracts the underlying data type from ClickHouse AggregateFunction and SimpleAggregateFunction types.
     * Examples:
     *   "SimpleAggregateFunction(sum, UInt64)" -> "UInt64"
     *   "AggregateFunction(quantile(0.5), Float64)" -> "Float64"
     *   "Nullable(SimpleAggregateFunction(sum, UInt64))" -> "Nullable(UInt64)"
     *   "UInt64" -> "UInt64" (unchanged if not an aggregate function type)
     */
    private String extractUnderlyingType(String typeName) {
        if (typeName == null) {
            return typeName;
        }
        
        // Handle Nullable wrapper first
        boolean isNullable = false;
        String workingTypeName = typeName;
        if (typeName.startsWith("Nullable(") && typeName.endsWith(")")) {
            isNullable = true;
            workingTypeName = typeName.substring("Nullable(".length(), typeName.length() - 1);
        }
        
        // Extract underlying type from SimpleAggregateFunction or AggregateFunction
        if (workingTypeName.startsWith("SimpleAggregateFunction(") && workingTypeName.endsWith(")")) {
            // Format: SimpleAggregateFunction(function_name, underlying_type)
            String content = workingTypeName.substring("SimpleAggregateFunction(".length(), workingTypeName.length() - 1);
            int lastComma = content.lastIndexOf(',');
            if (lastComma != -1) {
                workingTypeName = content.substring(lastComma + 1).trim();
            }
        } else if (workingTypeName.startsWith("AggregateFunction(") && workingTypeName.endsWith(")")) {
            // Format: AggregateFunction(function_name, underlying_type) or AggregateFunction(function_name(params), underlying_type)
            String content = workingTypeName.substring("AggregateFunction(".length(), workingTypeName.length() - 1);
            int lastComma = content.lastIndexOf(',');
            if (lastComma != -1) {
                workingTypeName = content.substring(lastComma + 1).trim();
            }
        }
        
        // Restore Nullable wrapper if needed
        if (isNullable && !workingTypeName.startsWith("Nullable(")) {
            workingTypeName = "Nullable(" + workingTypeName + ")";
        }
        
        return workingTypeName;
    }

    @Override
    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {
        // Handle ClickHouse AggregateFunction and SimpleAggregateFunction types
        // These types wrap the actual data type, e.g., "SimpleAggregateFunction(sum, UInt64)"
        typeName = extractUnderlyingType(typeName);
        
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
            case Types.OTHER:
                // For Types.OTHER, we need to parse the type name to determine the actual type
                // This handles cases where JDBC driver reports aggregate function types as OTHER
                return parseTypeByName(typeName, columnSize, digits);
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }
        return TypeFactory.createType(primitiveType);
    }

    /**
     * Parse ClickHouse type by name when JDBC type code is not helpful (e.g., Types.OTHER).
     * This method handles the extracted underlying types after aggregate function wrappers are removed.
     */
    private Type parseTypeByName(String typeName, int columnSize, int digits) {
        if (typeName == null) {
            return TypeFactory.createType(PrimitiveType.UNKNOWN_TYPE);
        }
        
        // Remove Nullable wrapper for type parsing
        String baseTypeName = typeName;
        if (typeName.startsWith("Nullable(") && typeName.endsWith(")")) {
            baseTypeName = typeName.substring("Nullable(".length(), typeName.length() - 1);
        }
        
        // Handle standard ClickHouse types
        if (baseTypeName.equals("Int8")) {
            return TypeFactory.createType(PrimitiveType.TINYINT);
        } else if (baseTypeName.equals("UInt8") || baseTypeName.equals("Int16")) {
            return TypeFactory.createType(PrimitiveType.SMALLINT);
        } else if (baseTypeName.equals("UInt16") || baseTypeName.equals("Int32")) {
            return TypeFactory.createType(PrimitiveType.INT);
        } else if (baseTypeName.equals("Int64") || baseTypeName.equals("UInt32")) {
            return TypeFactory.createType(PrimitiveType.BIGINT);
        } else if (baseTypeName.equals("UInt64") || baseTypeName.equals("Int128") || 
                   baseTypeName.equals("UInt128") || baseTypeName.equals("Int256") || 
                   baseTypeName.equals("UInt256")) {
            return TypeFactory.createType(PrimitiveType.LARGEINT);
        } else if (baseTypeName.equals("Float32")) {
            return TypeFactory.createType(PrimitiveType.FLOAT);
        } else if (baseTypeName.equals("Float64")) {
            return TypeFactory.createType(PrimitiveType.DOUBLE);
        } else if (baseTypeName.equals("Bool") || baseTypeName.equals("Boolean")) {
            return TypeFactory.createType(PrimitiveType.BOOLEAN);
        } else if (baseTypeName.equals("String")) {
            return TypeFactory.createVarcharType(65533);
        } else if (baseTypeName.equals("Date")) {
            return TypeFactory.createType(PrimitiveType.DATE);
        } else if (baseTypeName.equals("DateTime") || baseTypeName.startsWith("DateTime64")) {
            return TypeFactory.createType(PrimitiveType.DATETIME);
        } else if (baseTypeName.startsWith("Decimal")) {
            // Parse Decimal(precision, scale)
            String[] precisionAndScale = baseTypeName.replace("Decimal", "")
                    .replace("(", "").replace(")", "").replace(" ", "").split(",");
            if (precisionAndScale.length == 2) {
                int precision = Integer.parseInt(precisionAndScale[0]);
                int scale = Integer.parseInt(precisionAndScale[1]);
                return TypeFactory.createUnifiedDecimalType(precision, scale);
            }
        }
        
        // If we can't determine the type, return UNKNOWN_TYPE
        return TypeFactory.createType(PrimitiveType.UNKNOWN_TYPE);
    }


}
