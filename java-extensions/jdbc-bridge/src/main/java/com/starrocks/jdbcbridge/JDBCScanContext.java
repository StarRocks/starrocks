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

package com.starrocks.jdbcbridge;

public class JDBCScanContext {
    private String driverClassName;
    private String jdbcURL;
    private String user;
    private String password;
    private String sql;

    private int statementFetchSize;
    private int connectionPoolSize;
    private int minimumIdleConnections;
    private int connectionIdleTimeoutMs;

    public JDBCScanContext() {}
    public JDBCScanContext(String driverClassName, String jdbcURL, String user, String password,
                           String sql, int statementFetchSize, int connectionPoolSize,
                           int minimumIdleConnections, int connectionIdleTimeoutMs) {
        this.driverClassName = driverClassName;
        this.jdbcURL = jdbcURL;
        this.user = user;
        this.password = password;
        this.sql = sql;
        this.statementFetchSize = statementFetchSize;
        this.connectionPoolSize = connectionPoolSize;
        this.minimumIdleConnections = minimumIdleConnections;
        this.connectionIdleTimeoutMs = connectionIdleTimeoutMs;
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

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    public int getMinimumIdleConnections() {
        return minimumIdleConnections;
    }

    public int getConnectionIdleTimeoutMs() {
        return connectionIdleTimeoutMs;
    }

}
