// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.jdbcbridge;

public class JDBCScanContext {
    private String driverClassName;
    private String jdbcURL;
    private String user;
    private String password;
    private String sql;

    private int statementFetchSize;

    public JDBCScanContext() {}
    public JDBCScanContext(String driverClassName, String jdbcURL, String user, String password,
                           String sql, int statementFetchSize) {
        this.driverClassName = driverClassName;
        this.jdbcURL = jdbcURL;
        this.user = user;
        this.password = password;
        this.sql = sql;
        this.statementFetchSize = statementFetchSize;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public void setJdbcURL(String jdbcURL) {
        this.jdbcURL = jdbcURL;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public void setStatementFetchSize(int statementFetchSize) {
        this.statementFetchSize = statementFetchSize;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getJdbcURL() {
        return jdbcURL;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getSql() {
        return sql;
    }

    public int getStatementFetchSize() {
        return statementFetchSize;
    }


}
