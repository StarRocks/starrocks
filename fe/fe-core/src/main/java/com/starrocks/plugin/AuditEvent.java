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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/plugin/AuditEvent.java

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

package com.starrocks.plugin;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/*
 * AuditEvent contains all information about audit log info.
 * It should be created by AuditEventBuilder. For example:
 *
 *      AuditEvent event = new AuditEventBuilder()
 *          .setEventType(AFTER_QUERY)
 *          .setClientIp(xxx)
 *          ...
 *          .build();
 */
public class AuditEvent {
    public enum EventType {
        CONNECTION,
        DISCONNECTION,
        BEFORE_QUERY,
        AFTER_QUERY
    }

    @Retention(RetentionPolicy.RUNTIME)
    public @interface AuditField {
        String value() default "";

        boolean ignore_zero() default false;
    }

    public EventType type;

    // all fields which is about to be audit should be annotated by "@AuditField"
    // make them all "public" so that easy to visit.
    @AuditField(value = "Timestamp")
    public long timestamp = -1;
    @AuditField(value = "Client")
    public String clientIp = "";
    // The original login user
    @AuditField(value = "User")
    public String user = "";
    // The user used to authorize
    // `User` could be different from `AuthorizedUser` if impersonated
    @AuditField(value = "AuthorizedUser")
    public String authorizedUser = "";
    @AuditField(value = "ResourceGroup")
    public String resourceGroup = "";
    @AuditField(value = "Catalog")
    public String catalog = "";
    @AuditField(value = "Db")
    public String db = "";
    @AuditField(value = "State")
    public String state = "";
    @AuditField(value = "ErrorCode")
    public String errorCode = "";
    @AuditField(value = "Time")
    public long queryTime = -1;
    @AuditField(value = "ScanBytes")
    public long scanBytes = -1;
    @AuditField(value = "ScanRows")
    public long scanRows = -1;
    @AuditField(value = "ReturnRows")
    public long returnRows = -1;
    @AuditField(value = "CpuCostNs", ignore_zero = true)
    public long cpuCostNs = -1;
    @AuditField(value = "MemCostBytes", ignore_zero = true)
    public long memCostBytes = -1;
    @AuditField(value = "StmtId")
    public long stmtId = -1;
    @AuditField(value = "QueryId")
    public String queryId = "";
    @AuditField(value = "IsQuery")
    public boolean isQuery = false;
    @AuditField(value = "feIp")
    public String feIp = "";
    @AuditField(value = "Stmt")
    public String stmt = "";
    @AuditField(value = "Digest")
    public String digest = "";
    @AuditField(value = "PlanCpuCost")
    public double planCpuCosts = -1;
    @AuditField(value = "PlanMemCost")
    public double planMemCosts = -1;
    @AuditField(value = "PendingTimeMs")
    public long pendingTimeMs = -1;
    @AuditField(value = "BigQueryLogCPUSecondThreshold")
    public long bigQueryLogCPUSecondThreshold = -1;
    @AuditField(value = "BigQueryLogScanBytesThreshold")
    public long bigQueryLogScanBytesThreshold = -1;
    @AuditField(value = "BigQueryLogScanRowsThreshold")
    public long bigQueryLogScanRowsThreshold = -1;

    public static class AuditEventBuilder {

        private AuditEvent auditEvent = new AuditEvent();

        public AuditEventBuilder() {
        }

        public void reset() {
            auditEvent = new AuditEvent();
        }

        public AuditEventBuilder setEventType(EventType eventType) {
            auditEvent.type = eventType;
            return this;
        }

        public AuditEventBuilder setTimestamp(long timestamp) {
            auditEvent.timestamp = timestamp;
            return this;
        }

        public AuditEventBuilder setClientIp(String clientIp) {
            auditEvent.clientIp = clientIp;
            return this;
        }

        public AuditEventBuilder setUser(String user) {
            auditEvent.user = user;
            return this;
        }

        public AuditEventBuilder setAuthorizedUser(String authorizedUser) {
            auditEvent.authorizedUser = authorizedUser;
            return this;
        }

        public AuditEventBuilder setResourceGroup(String resourceGroup) {
            auditEvent.resourceGroup = resourceGroup;
            return this;
        }

        public AuditEventBuilder setCatalog(String catalog) {
            auditEvent.catalog = catalog;
            return this;
        }

        public AuditEventBuilder setDb(String db) {
            auditEvent.db = db;
            return this;
        }

        public AuditEventBuilder setState(String state) {
            auditEvent.state = state;
            return this;
        }

        public AuditEventBuilder setErrorCode(String errorCode) {
            auditEvent.errorCode = errorCode;
            return this;
        }

        public AuditEventBuilder setQueryTime(long queryTime) {
            auditEvent.queryTime = queryTime;
            return this;
        }

        public AuditEventBuilder setScanBytes(long scanBytes) {
            auditEvent.scanBytes = scanBytes;
            return this;
        }

        public AuditEventBuilder setScanRows(long scanRows) {
            auditEvent.scanRows = scanRows;
            return this;
        }

        public AuditEventBuilder setReturnRows(long returnRows) {
            auditEvent.returnRows = returnRows;
            return this;
        }

        /**
         * Cpu cost in nanoseconds
         */
        public AuditEventBuilder setCpuCostNs(long cpuNs) {
            auditEvent.cpuCostNs = cpuNs;
            return this;
        }

        public AuditEventBuilder setMemCostBytes(long memCostBytes) {
            auditEvent.memCostBytes = memCostBytes;
            return this;
        }

        public AuditEventBuilder setStmtId(long stmtId) {
            auditEvent.stmtId = stmtId;
            return this;
        }

        public AuditEventBuilder setQueryId(String queryId) {
            auditEvent.queryId = queryId;
            return this;
        }

        public AuditEventBuilder setIsQuery(boolean isQuery) {
            auditEvent.isQuery = isQuery;
            return this;
        }

        public AuditEventBuilder setFeIp(String feIp) {
            auditEvent.feIp = feIp;
            return this;
        }

        public AuditEventBuilder setStmt(String stmt) {
            auditEvent.stmt = stmt;
            return this;
        }

        public AuditEventBuilder setDigest(String digest) {
            auditEvent.digest = digest;
            return this;
        }

        public AuditEventBuilder setPlanCpuCosts(double cpuCosts) {
            auditEvent.planCpuCosts = cpuCosts;
            return this;
        }

        public AuditEventBuilder setPlanMemCosts(double memCosts) {
            auditEvent.planMemCosts = memCosts;
            return this;
        }

        public AuditEventBuilder setPendingTimeMs(long pendingTimeMs) {
            auditEvent.pendingTimeMs = pendingTimeMs;
            return this;
        }

        public AuditEventBuilder setBigQueryLogCPUSecondThreshold(long bigQueryLogCPUSecondThreshold) {
            auditEvent.bigQueryLogCPUSecondThreshold = bigQueryLogCPUSecondThreshold;
            return this;
        }

        public AuditEventBuilder setBigQueryLogScanBytesThreshold(long bigQueryLogScanBytesThreshold) {
            auditEvent.bigQueryLogScanBytesThreshold = bigQueryLogScanBytesThreshold;
            return this;
        }

        public AuditEventBuilder setBigQueryLogScanRowsThreshold(long bigQueryLogScanRowsThreshold) {
            auditEvent.bigQueryLogScanRowsThreshold = bigQueryLogScanRowsThreshold;
            return this;
        }

        public AuditEvent build() {
            return this.auditEvent;
        }
    }
}
