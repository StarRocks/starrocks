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
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }
        return TypeFactory.createType(primitiveType);
    }

    @Override
    public long getTableRowCount(Connection connection, String dbName, String tableName) throws SQLException {
        // system.tables.total_rows is maintained by ClickHouse for most table engines (MergeTree, etc.)
        // and is NULL for engines that do not track it (e.g. Distributed over remote shards).
        String sql = "SELECT total_rows FROM system.tables WHERE database = ? AND name = ?";
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

    @Override
    public List<Partition> getPartitions(Connection connection, Table table) {
        // ClickHouse has no engine-agnostic system table for per-partition metadata, so return a single
        // synthetic partition for the whole table, matching MysqlSchemaResolver/PostgresSchemaResolver's
        // handling of non-partitioned tables. The "modified time" used to decide whether an MV built on
        // this table is stale must reflect actual data changes, not just DDL:
        // system.tables.metadata_modification_time only updates on schema changes (e.g. ALTER TABLE), so
        // relying on it alone would make StarRocks silently skip refreshes after plain INSERTs.
        // system.parts.modification_time (MergeTree-family engines only) updates whenever a new part is
        // written (INSERT/merge), so prefer it and fall back to metadata_modification_time for engines
        // without entries there (e.g. Distributed, Memory, Log).
        JDBCTable jdbcTable = (JDBCTable) table;
        long modifiedTime = System.currentTimeMillis();
        boolean gotPartsSignal = false;
        String partsSql = "SELECT MAX(modification_time) FROM system.parts WHERE database = ? AND table = ? AND active = 1";
        try (PreparedStatement ps = connection.prepareStatement(partsSql)) {
            ps.setString(1, jdbcTable.getCatalogDBName());
            ps.setString(2, jdbcTable.getCatalogTableName());
            ps.setQueryTimeout(getQueryTimeoutSeconds());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Timestamp ts = rs.getTimestamp(1);
                    if (ts != null) {
                        modifiedTime = ts.getTime();
                        gotPartsSignal = true;
                    }
                }
            }
        } catch (SQLException e) {
            // ignore: engines without system.parts entries fall through to the metadata-time query below
        }
        if (!gotPartsSignal) {
            String metaSql = "SELECT metadata_modification_time FROM system.tables WHERE database = ? AND name = ?";
            try (PreparedStatement ps = connection.prepareStatement(metaSql)) {
                ps.setString(1, jdbcTable.getCatalogDBName());
                ps.setString(2, jdbcTable.getCatalogTableName());
                ps.setQueryTimeout(getQueryTimeoutSeconds());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        Timestamp ts = rs.getTimestamp(1);
                        if (ts != null) {
                            modifiedTime = ts.getTime();
                        }
                    }
                }
            } catch (SQLException e) {
                throw new StarRocksConnectorException(e.getMessage(), e);
            }
        }
        return Lists.newArrayList(new Partition(table.getName(), modifiedTime));
    }

}
