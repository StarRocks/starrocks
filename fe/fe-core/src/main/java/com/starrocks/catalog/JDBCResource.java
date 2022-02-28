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
* "jdbc_uri" = "jdbc:postgresql://127.0.0.1:5432/db"
* "driver" = "driver_name",
* );
* <p>
* DROP RESOURCE "jdbc_pg";
* */
public class JDBCResource extends Resource {
    public static final String DRIVER = "driver";
    public static final String URI = "jdbc_uri";
    public static final String USER = "user";
    public static final String PASSWORD = "password";

    // @TODO is this necessary?
    // private static final String JDBC_TYPE = "jdbc_type";

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

        checkProperties(DRIVER);
        checkProperties(URI);
        checkProperties(USER);
        checkProperties(PASSWORD);
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
