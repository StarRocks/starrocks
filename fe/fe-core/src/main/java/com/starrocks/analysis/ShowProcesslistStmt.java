// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowProcesslistStmt.java

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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;

// SHOW PROCESSLIST statement.
// Used to show connection belong to this user.
public class ShowProcesslistStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Id", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("User", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Host", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Db", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Command", ScalarType.createVarchar(16)))
                    .addColumn(new Column("ConnectionStartTime", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Time", ScalarType.createType(PrimitiveType.INT)))
                    .addColumn(new Column("State", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Info", ScalarType.createVarchar(32 * 1024)))
                    .build();
    private boolean isShowFull = false;

    public ShowProcesslistStmt(boolean isShowFull) {
        this.isShowFull = isShowFull;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowProcesslistStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public String toSql() {
        String full = isShowFull ? "FULL " : "";
        return String.format("SHOW %sPROCESSLIST", full);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    public boolean showFull() {
        return isShowFull;
    }
}
