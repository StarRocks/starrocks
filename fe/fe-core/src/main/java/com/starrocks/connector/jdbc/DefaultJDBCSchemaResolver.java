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

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static java.lang.Math.max;

public class DefaultJDBCSchemaResolver extends JDBCSchemaResolver {

    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        return connection.getMetaData().getTables(null, dbName, null,
                new String[] {"TABLE", "VIEW"});
    }

    public ResultSet getColumns(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(null, dbName, tblName, "%");
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
                return ScalarType.createCharType(columnSize);
            case Types.VARCHAR:
                return ScalarType.createVarcharType();
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
            // if user not specify numeric precision and scale, the default value is 0,
            // we can't defer the precision and scale, can only deal it as string.
            if (precision == 0) {
                return ScalarType.createVarcharType(ScalarType.OLAP_MAX_VARCHAR_LENGTH);
            }
            return ScalarType.createUnifiedDecimalType(precision, max(digits, 0));
        }
    }

}
