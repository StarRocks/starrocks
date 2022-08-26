// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.jdbc;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public abstract class JDBCSchemaResolver {

    public Collection<String> listSchemas(Connection connection) throws DdlException {
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
            throw new DdlException(e.getMessage());
        }
    }

    public ResultSet getTables(Connection connection, String dbName) throws SQLException {
        return connection.getMetaData().getTables(dbName, null, null,
                new String[] {"TABLE", "VIEW"});
    }

    public List<Column> convertToSRTable(ResultSet columnSet) throws SQLException {
        List<Column> fullSchema = Lists.newArrayList();
        while (columnSet.next()) {
            Type type = convertColumnType(columnSet.getInt("DATA_TYPE"),
                    columnSet.getString("TYPE_NAME"),
                    columnSet.getInt("COLUMN_SIZE"),
                    columnSet.getInt("DECIMAL_DIGITS"));
            fullSchema.add(new Column(columnSet.getString("COLUMN_NAME"), type,
                    columnSet.getString("IS_NULLABLE").equals("YES")));
        }
        return fullSchema;
    }

    public Type convertColumnType(int dataType, String typeName, int columnSize, int digits) throws SQLException {
        throw new SQLException("should not arrival here");
    }
}
