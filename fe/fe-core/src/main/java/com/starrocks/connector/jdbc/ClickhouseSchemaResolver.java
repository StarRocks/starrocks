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
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;

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
                return ScalarType.createVarcharType(65533);
            case Types.DATE:
                primitiveType = PrimitiveType.DATE;
                break;
            case Types.TIMESTAMP:
                primitiveType = PrimitiveType.DATETIME;
                break;
            case Types.DECIMAL:
                // Decimal(9,9), first 9 is precision, second 9 is scale
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
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }
        return ScalarType.createType(primitiveType);
    }


}
