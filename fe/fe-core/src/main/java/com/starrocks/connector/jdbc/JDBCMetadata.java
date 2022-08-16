// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.jdbc;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JDBCMetadata implements ConnectorMetadata {

    private static Logger LOG = LogManager.getLogger(JDBCMetadata.class);

    private Map<String, String> properties;
    private JDBCSchemaResolver schemaResolver;

    public JDBCMetadata(Map<String, String> properties) throws DdlException {
        this.properties = properties;
        try {
            Class.forName(properties.get(JDBCResource.DRIVER_CLASS));
        } catch (ClassNotFoundException e) {
            LOG.warn(e.getMessage());
            throw new DdlException("doesn't find class: " + e.getMessage());
        }
        if (properties.get(JDBCResource.DRIVER_CLASS).toLowerCase().contains("mysql")) {
            schemaResolver = new MysqlSchemaResolver();
        } else {
            LOG.warn("{} not support yet", properties.get(JDBCResource.DRIVER_CLASS));
            throw new DdlException(properties.get(JDBCResource.DRIVER_CLASS) + " not support yet");
        }
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(properties.get(JDBCResource.URI),
                properties.get(JDBCResource.USER), properties.get(JDBCResource.PASSWORD));
    }

    @Override
    public List<String> listDbNames() throws DdlException {
        try (Connection connection = getConnection()) {
            return schemaResolver.listSchemas(connection).stream()
                    .map(String::toLowerCase)
                    .collect(Collectors.toList());
        } catch (SQLException e) {
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public Database getDb(String name) {
        try {
            if (listDbNames().contains(name)) {
                return new Database(0, name);
            } else {
                return null;
            }
        } catch (DdlException e) {
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) throws DdlException {
        try (Connection connection = getConnection()) {
            try (ResultSet resultSet = schemaResolver.getTables(connection, dbName)) {
                ImmutableList.Builder<String> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    list.add(tableName.toLowerCase());
                }
                return list.build();
            }
        } catch (SQLException e) {
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try (Connection connection = getConnection()) {
            ResultSet columnSet = connection.getMetaData().getColumns(dbName, null, tblName, "%");
            List<Column> fullSchema = schemaResolver.convertToSRTable(columnSet);
            if (fullSchema.isEmpty()) {
                return null;
            }
            return new JDBCTable(0, tblName, fullSchema, dbName, properties);
        } catch (SQLException | DdlException e) {
            LOG.warn(e.getMessage());
            return null;
        }
    }

}
