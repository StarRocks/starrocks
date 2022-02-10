// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;

import java.util.Map;

/*
* External JDBC resource for JDBC table query
* <p>
* Example:
* CREATE EXTERNAL RESOURCE "jdbc_pg"
* PROPERTIES
* (
* "type" = "jdbc",
* "user" = "user",
* "password" = "password",
* "hosts" = "127.0.0.1:1234,127.0.0.2:1235",
* "driver" = "driver_name",
* "jdbc_type" = "postgresql",
* "connectionTimeout": "10000"
* );
* <p>
* DROP RESOURCE "jdbc_pg";
* */
public class JDBCResource extends Resource {
    private static final String HOSTS = "hosts";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String DRIVER = "driver";

    // @TODO is this necessary?
    private static final String JDBC_TYPE = "jdbc_type";

    @SerializedName(value = "configs")
    private Map<String, String> configs;

    public JDBCResource(String name) {
        super(name, ResourceType.JDBC);
    }

    public JDBCResource(String name, Map<String, String> configs) {
        super(name, ResourceType.JDBC);
        this.configs = configs;
    }

    private void checkProperties(String propertyKey) throws DdlException {
        String value = configs.get(propertyKey);
        if (value == null) {
            throw new DdlException("Missing " + propertyKey + " in properties");
        }
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);

        configs = properties;

        checkProperties(HOSTS);
        checkProperties(USER);
        checkProperties(PASSWORD);
        checkProperties(DRIVER);
        checkProperties(JDBC_TYPE);
    }

    public String getProperty(String propertyKey) {
        return configs.get(propertyKey);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }
}
