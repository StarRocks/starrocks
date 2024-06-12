// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe.events;

import com.starrocks.sql.ast.StatementBase;

/**
 * Stmt Event with query info and Statement
 * using for stmt event listeners
 */
public class StmtEvent {
    private String queryId;
    private Long timestamp;
    private String clientIp;
    private String user;
    private StatementBase statementBase;

    public StmtEvent queryId(String queryId) {
        this.queryId = queryId;
        return this;
    }

    public StmtEvent timestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public StmtEvent clientIp(String clientIp) {
        this.clientIp = clientIp;
        return this;
    }

    public StmtEvent user(String user) {
        this.user = user;
        return this;
    }

    public StmtEvent statementBase(StatementBase statementBase) {
        this.statementBase = statementBase;
        return this;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public StatementBase getStatementBase() {
        return statementBase;
    }

    public void setStatementBase(StatementBase statementBase) {
        this.statementBase = statementBase;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
