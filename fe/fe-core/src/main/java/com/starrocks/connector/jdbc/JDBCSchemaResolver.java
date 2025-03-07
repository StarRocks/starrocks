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
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.SchemaConstants;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class JDBCSchemaResolver {

    boolean supportPartitionInformation = false;

    public Collection<String> listSchemas(Connection connection) {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (!schemaName.equalsIgnoreCase("information_schema")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }
    }

    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        return connection.getMetaData().getTables(dbName, null, null,
                new String[] {"TABLE", "VIEW"});
    }

    public ResultSet getColumns(Connection connection, String dbName, String tblName) throws SQLException {
        return connection.getMetaData().getColumns(dbName, null, tblName, "%");
    }

    public Table getTable(long id, String name, List<Column> schema, String dbName,
                          String catalogName, Map<String, String> properties) throws DdlException {
        return new JDBCTable(id, name, schema, dbName, catalogName, properties);
    }

    public Table getTable(long id, String name, List<Column> schema, List<Column> partitionColumns, String dbName,
                          String catalogName, Map<String, String> properties) throws DdlException {
        return new JDBCTable(id, name, schema, partitionColumns, dbName, catalogName, properties);
    }

    public List<String> listPartitionNames(Connection connection, String databaseName, String tableName) {
        return Lists.newArrayList();
    }

    public List<String> listPartitionColumns(Connection connection, String databaseName, String tableName) {
        return Lists.newArrayList();
    }

    public List<Partition> getPartitions(Connection connection, Table table) {
        return Lists.newArrayList();
    }

    public List<Column> convertToSRTable(ResultSet columnSet) throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            Type type = convertColumnType(columnSet.getInt("DATA_TYPE"),
                    columnSet.getString("TYPE_NAME"),
                    columnSet.getInt("COLUMN_SIZE"),
                    columnSet.getInt("DECIMAL_DIGITS"));

            String comment = "";
            // Add try-cache to prevent exceptions when the metadata of some databases does not contain REMARKS
            try {
                if (columnSet.getString("REMARKS") != null) {
                    comment = columnSet.getString("REMARKS");
                }
            } catch (SQLException ignored) { }

            fullSchema.add(new Column(columnSet.getString("COLUMN_NAME"), type,
                    columnSet.getString("IS_NULLABLE").equals(SchemaConstants.YES), comment));
        }
        return fullSchema;
    }

    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) throws SQLException {
        throw new SQLException("should not arrival here");
    }

    public boolean checkAndSetSupportPartitionInformation(Connection connection) {
        return false;

    }

    public boolean isSupportPartitionInformation() {
        return supportPartitionInformation;
    }

}
