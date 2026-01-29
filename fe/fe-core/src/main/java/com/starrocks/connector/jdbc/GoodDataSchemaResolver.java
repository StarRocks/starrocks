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
import com.starrocks.catalog.Type;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

/**
 * Schema resolver for GoodData JDBC driver.
 * 
 * GoodData is Vertica-based, so this resolver extends VerticaSchemaResolver to inherit
 * Vertica-specific type conversions, identifier quoting, and other features.
 * 
 * The key difference from standard Vertica is nullability handling:
 * The GoodData JDBC driver incorrectly reports all columns as NOT NULL in metadata,
 * even when the actual data contains NULL values. This causes "Unexpected NULL value"
 * errors when loading data.
 * 
 * To work around this driver issue, this resolver always treats all columns as nullable,
 * ignoring the IS_NULLABLE metadata field from DatabaseMetaData.getColumns().
 */
public class GoodDataSchemaResolver extends VerticaSchemaResolver {

    @Override
    public List<Column> convertToSRTable(ResultSet columnSet) throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            // Inherit Vertica type conversion (handles Vertica-specific types)
            Type type = convertColumnType(columnSet.getInt("DATA_TYPE"),
                    columnSet.getString("TYPE_NAME"),
                    columnSet.getInt("COLUMN_SIZE"),
                    columnSet.getInt("DECIMAL_DIGITS"));
            
            // Inherit Vertica identifier quoting (PostgreSQL-style double quotes)
            String columnName = columnSet.getString("COLUMN_NAME");
            if (!columnName.equals(columnName.toLowerCase(Locale.ROOT))) {
                columnName = "\"" + columnName + "\"";
            }
            
            // Always treat columns as nullable to work around driver metadata bug
            fullSchema.add(new Column(columnName, type, true));
        }
        return fullSchema;
    }
}
