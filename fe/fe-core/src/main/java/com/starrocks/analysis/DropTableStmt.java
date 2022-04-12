// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DropTableStmt.java

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

package com.starrocks.analysis;

import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AstVisitor;

// DROP TABLE
public class DropTableStmt extends DdlStmt {
    private boolean ifExists;
    private final TableName tableName;
    private final boolean isView;
    private boolean forceDrop;

    public DropTableStmt(boolean ifExists, TableName tableName, boolean forceDrop) {
        this.ifExists = ifExists;
        this.tableName = tableName;
        this.isView = false;
        this.forceDrop = forceDrop;
    }

    public DropTableStmt(boolean ifExists, TableName tableName, boolean isView, boolean forceDrop) {
        this.ifExists = ifExists;
        this.tableName = tableName;
        this.isView = isView;
        this.forceDrop = forceDrop;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public TableName getTableNameObject() {
        return tableName;
    }

    public boolean isView() {
        return isView;
    }

    public boolean isForceDrop() {
        return this.forceDrop;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        if (isView()) {
            stringBuilder.append("DROP VIEW ");
        } else {
            stringBuilder.append("DROP TABLE ");
        }
        if (isSetIfExists()) {
            stringBuilder.append("IF EXISTS ");
        }
        stringBuilder.append(tableName.toSql());
        if (isForceDrop()) {
            stringBuilder.append(" FORCE");
        }
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropTableStmt(this, context);
    }
}
