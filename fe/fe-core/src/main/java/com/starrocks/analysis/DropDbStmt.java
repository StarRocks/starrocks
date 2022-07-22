// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DropDbStmt.java

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

import com.starrocks.sql.ast.AstVisitor;

// DROP DB Statement
public class DropDbStmt extends DdlStmt {
    private boolean ifExists;
    private String dbName;
    private boolean forceDrop;

    public DropDbStmt(boolean ifExists, String dbName, boolean forceDrop) {
        this.ifExists = ifExists;
        this.dbName = dbName;
        this.forceDrop = forceDrop;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public String getDbName() {
        return this.dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public boolean isForceDrop() {
        return this.forceDrop;
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP DATABASE ").append("`").append(dbName).append("`");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropDbStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

}
