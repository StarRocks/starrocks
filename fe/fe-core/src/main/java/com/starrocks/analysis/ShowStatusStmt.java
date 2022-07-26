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
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

/**
 * Show Status statement
 * Acceptable syntax:
 * SHOW [GLOBAL | LOCAL | SESSION] STATUS [LIKE 'pattern' | WHERE expr]
 */
public class ShowStatusStmt extends ShowStmt {

    private static final String NAME_COL = "Variable_name";
    private static final String VALUE_COL = "Value";
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(NAME_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(VALUE_COL, ScalarType.createVarchar(20)))
                    .build();

    private SetType type;
    private String pattern;
    private Expr where;

    public ShowStatusStmt() {
        this.type = SetType.DEFAULT;
    }

    public ShowStatusStmt(SetType type, String pattern, Expr where) {
        this.type = type;
        this.pattern = pattern;
        this.where = where;
    }

    public SetType getType() {
        return type;
    }

    public String getPattern() {
        return pattern;
    }

    public Expr getWhere() {
        return where;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW ");
        sb.append(type.toString()).append(" STATUS");
        if (pattern != null) {
            sb.append(" LIKE '").append(pattern).append("'");
        }
        if (where != null) {
            sb.append(" WHERE ").append(where.toSql());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
