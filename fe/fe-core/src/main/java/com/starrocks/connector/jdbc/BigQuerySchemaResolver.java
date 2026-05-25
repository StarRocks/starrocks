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

/**
 * JDBC schema resolver for Google BigQuery.
 *
 * BigQuery hierarchy: Project (catalog/connection) → Dataset (schema) → Table.
 * Auth lives entirely in the JDBC URL (OAuthType, OAuthPvtKeyPath, etc.) —
 * the user/password catalog properties are unused.
 *
 * Compatible with the Simba/Google BigQuery JDBC driver
 * (com.simba.googlebigquery.jdbc.Driver / com.simba.googlebigquery.jdbc42.Driver).
 *
 * Type mapping notes:
 *   - BIGNUMERIC (precision > 38) → VARCHAR (cannot fit in DECIMAL(38,x))
 *   - STRUCT / RECORD            → VARCHAR (serialised string representation)
 *   - ARRAY                      → VARCHAR (serialised string representation)
 *   - GEOGRAPHY, INTERVAL        → VARCHAR
 *   - JSON                       → JSON
 *   - TIMESTAMP (UTC)            → DATETIME (TZ conversion handled in JDBCScanner)
 *   - DATETIME (civil time)      → DATETIME (no TZ conversion)
 */
public class BigQuerySchemaResolver extends JDBCSchemaResolver {

    public BigQuerySchemaResolver() {
        // BigQuery exposes TABLE, VIEW, and EXTERNAL TABLE via JDBC metadata.
        this.defaultTableTypes = new String[] {"TABLE", "VIEW", "EXTERNAL"};
    }

    // -------------------------------------------------------------------------
    // Schema (dataset) listing
    // -------------------------------------------------------------------------

    @Override
    protected boolean isInternalSchema(String schemaName) {
        // BigQuery's INFORMATION_SCHEMA is a virtual view within each dataset;
        // the JDBC driver may surface it as a schema — filter it out.
        return schemaName.equalsIgnoreCase("INFORMATION_SCHEMA");
    }

    // -------------------------------------------------------------------------
    // Metadata access
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Table object construction — backtick-quoted dataset.table for pushed-down SQL
    // -------------------------------------------------------------------------

    @Override
    public Table getTable(long id, String name, List<Column> schema, String dbName,
                          String catalogName, Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "`" + dbName + "." + name + "`");
        return new JDBCTable(id, name, schema, dbName, catalogName, newProp);
    }

    @Override
    public Table getTable(long id, String name, List<Column> schema, List<Column> partitionColumns,
                          String dbName, String catalogName, Map<String, String> properties) throws DdlException {
        Map<String, String> newProp = new HashMap<>(properties);
        newProp.putIfAbsent(JDBCTable.JDBC_TABLENAME, "`" + dbName + "." + name + "`");
        return new JDBCTable(id, name, schema, partitionColumns, dbName, catalogName, newProp);
    }

    // -------------------------------------------------------------------------
    // Partitions — BigQuery has native table partitioning but it is not exposed
    // via the JDBC metadata API; return the whole table as a single partition.
    // -------------------------------------------------------------------------

    @Override
    public List<Partition> getPartitions(Connection connection, Table table) {
        return Lists.newArrayList(new Partition(table.getName(), TimeUtils.getEpochSeconds()));
    }

    // -------------------------------------------------------------------------
    // Type mapping
    // -------------------------------------------------------------------------

    @Override
    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) {
        // BIGNUMERIC precision exceeds DECIMAL(38,x); fall back to VARCHAR.
        if (isBigNumeric(typeName)) {
            return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
        }

        switch (dataType) {
            // --- Boolean ---
            // Simba driver may return BIT or BOOLEAN for BigQuery BOOL.
            case Types.BIT:
            case Types.BOOLEAN:
                return TypeFactory.createType(PrimitiveType.BOOLEAN);

            // --- Integer ---
            case Types.TINYINT:
                return TypeFactory.createType(PrimitiveType.TINYINT);
            case Types.SMALLINT:
                return TypeFactory.createType(PrimitiveType.SMALLINT);
            case Types.INTEGER:
                return TypeFactory.createType(PrimitiveType.INT);
            case Types.BIGINT:
                // BigQuery INT64 → BIGINT
                return TypeFactory.createType(PrimitiveType.BIGINT);

            // --- Floating point ---
            case Types.FLOAT:
            case Types.REAL:
                return TypeFactory.createType(PrimitiveType.FLOAT);
            case Types.DOUBLE:
                // BigQuery FLOAT64 → DOUBLE
                return TypeFactory.createType(PrimitiveType.DOUBLE);

            // --- Decimal ---
            case Types.NUMERIC:
            case Types.DECIMAL:
                // NUMERIC(38,9) is the BigQuery standard precision.
                // Reject if precision > 38 (driver quirk) or if unspecified (columnSize == 0).
                if (columnSize > 0 && columnSize <= 38) {
                    return TypeFactory.createUnifiedDecimalType(columnSize, Math.max(digits, 0));
                }
                return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());

            // --- String ---
            // BigQuery STRING has no fixed length limit (up to 9 MB); use catalog max.
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
            case Types.CHAR:
            case Types.NCHAR:
                return columnSize > 0
                        ? TypeFactory.createVarcharType(columnSize)
                        : TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());

            // --- Binary ---
            // BigQuery BYTES → VARBINARY
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return TypeFactory.createVarbinary(TypeFactory.CATALOG_MAX_VARCHAR_LENGTH);

            // --- Temporal ---
            case Types.DATE:
                return TypeFactory.createType(PrimitiveType.DATE);
            case Types.TIME:
                return TypeFactory.createType(PrimitiveType.TIME);
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                // Covers both BigQuery TIMESTAMP (UTC) and DATETIME (civil time).
                // TIMESTAMP columns undergo UTC→queryTZ conversion in JDBCScanner.
                return TypeFactory.createType(PrimitiveType.DATETIME);

            // --- Structured / complex types ---
            // Types.STRUCT covers BigQuery STRUCT/RECORD.
            case Types.STRUCT:
            case Types.ARRAY:
                return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());

            // --- Vendor-specific (Types.OTHER) ---
            case Types.OTHER:
                return convertOtherType(typeName);

            default:
                // GEOGRAPHY, INTERVAL, and any future BigQuery types fall here.
                return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
        }
    }

    private Type convertOtherType(String typeName) {
        if (typeName == null) {
            return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
        }
        String normalized = typeName.trim().toUpperCase();
        if (normalized.equals("JSON")) {
            return TypeFactory.createType(PrimitiveType.JSON);
        }
        // GEOGRAPHY, INTERVAL, or anything else → VARCHAR
        return TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
    }

    private boolean isBigNumeric(String typeName) {
        if (typeName == null) {
            return false;
        }
        String upper = typeName.trim().toUpperCase();
        return upper.equals("BIGNUMERIC") || upper.startsWith("BIGNUMERIC(");
    }
}
