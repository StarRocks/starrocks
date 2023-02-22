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


package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

public class AlterDatabaseQuotaStmt extends DdlStmt {
    private String catalog;
    private String dbName;
    private final QuotaType quotaType;
    private final String quotaValue;
    private long quota;

    public enum QuotaType {
        NONE,
        DATA,
        REPLICA
    }

    public AlterDatabaseQuotaStmt(String dbName, QuotaType quotaType, String quotaValue) {
        this(dbName, quotaType, quotaValue, NodePosition.ZERO);
    }

    public AlterDatabaseQuotaStmt(String dbName, QuotaType quotaType, String quotaValue, NodePosition pos) {
        super(pos);
        this.dbName = dbName;
        this.quotaType = quotaType;
        this.quotaValue = quotaValue;
    }

    public String getCatalogName() {
        return this.catalog;
    }

    public void setCatalogName(String catalogName) {
        this.catalog = catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public long getQuota() {
        return quota;
    }

    public String getQuotaValue() {
        return quotaValue;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setQuota(long quota) {
        this.quota = quota;
    }

    public QuotaType getQuotaType() {
        return quotaType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterDatabaseQuotaStatement(this, context);
    }
}
