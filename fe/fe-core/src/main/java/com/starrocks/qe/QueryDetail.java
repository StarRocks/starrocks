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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/QueryDetail.java

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

package com.starrocks.qe;


import java.io.Serializable;

public class QueryDetail implements Serializable {
    public enum QueryMemState {
        RUNNING,
        FINISHED,
        FAILED,
        CANCELLED
    }

    // When query received, FE will construct a QueryDetail
    // object. This object will set queryId, startTime, sql
    // fields. As well state is be set as RUNNING. 
    // After query finished, endTime and latency will
    // be set and state will be updated to be FINISHED/FAILED/CANCELLED
    // according to the query execution results.
    // So, one query will be inserted into as a item and 
    // be udpated upon finished. To indicate the two event,
    // a extra field named eventTime is added.
    private long eventTime;
    private String queryId;
    private boolean isQuery;
    private String remoteIP;
    private int connId;
    private long startTime;
    // endTime and latency are update upon query finished.
    // default value will set to be minus one(-1).
    private long endTime;
    private long latency;
    private QueryMemState state;
    private String database;
    private String sql;
    private String user;
    private String errorMessage;
    private String explain;
    private String profile;
    private String resourceGroupName;

    public QueryDetail() {
    }

    public QueryDetail(String queryId, boolean isQuery, int connId, String remoteIP,
                       long startTime, long endTime, long latency, QueryMemState state,
                       String database, String sql, String user, String resourceGroupName) {
        this.queryId = queryId;
        this.isQuery = isQuery;
        this.connId = connId;
        this.remoteIP = remoteIP;
        this.startTime = startTime;
        this.endTime = endTime;
        this.latency = latency;
        this.state = state;
        if (database.equals("")) {
            this.database = "";
        } else {
            String[] stringPieces = database.split(":", -1);
            if (stringPieces.length == 1) {
                this.database = stringPieces[0];
            } else {
                this.database = stringPieces[1]; // eliminate cluster name
            }
        }
        this.sql = sql;
        this.user = user;
    }

    public QueryDetail copy() {
        QueryDetail queryDetail = new QueryDetail();
        queryDetail.eventTime = this.eventTime;
        queryDetail.queryId = this.queryId;
        queryDetail.isQuery = this.isQuery;
        queryDetail.connId = this.connId;
        queryDetail.remoteIP = this.remoteIP;
        queryDetail.startTime = this.startTime;
        queryDetail.endTime = this.endTime;
        queryDetail.latency = this.latency;
        queryDetail.state = this.state;
        queryDetail.database = this.database;
        queryDetail.sql = this.sql;
        queryDetail.user = this.user;
        queryDetail.errorMessage = this.errorMessage;
        queryDetail.explain = this.explain;
        queryDetail.profile = this.profile;
        return queryDetail;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQuery(boolean isQuery) {
        this.isQuery = isQuery;
    }

    public boolean isQuery() {
        return isQuery;
    }

    public String getRemoteIP() {
        return remoteIP;
    }

    public void setRemoteIP(String remoteIP) {
        this.remoteIP = remoteIP;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setLatency(long latency) {
        this.latency = latency;
    }

    public long getLatency() {
        return latency;
    }

    public void setState(QueryMemState state) {
        this.state = state;
    }

    public QueryMemState getState() {
        return state;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getDatabase() {
        return database;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUser() {
        return user;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getExplain() {
        return explain;
    }

    public void setExplain(String explain) {
        this.explain = explain;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public String getResourceGroupName() {
        return resourceGroupName;
    }

    public void setResourceGroupName(String workGroupName) {
        this.resourceGroupName = workGroupName;
    }
}
