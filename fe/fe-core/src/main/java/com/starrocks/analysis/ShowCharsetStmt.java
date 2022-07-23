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
 * Show charset statement
 * Acceptable syntax:
 * SHOW {CHAR SET | CHARSET}
 *     [LIKE 'pattern' | WHERE expr]
 */
public class ShowCharsetStmt extends ShowStmt {
    private static final String CHAR_SET_COL = "Charset";
    private static final String DESC_COL = "Description";
    private static final String DEFAULT_COLLATION_COL = "Default collation";
    private static final String MAX_LEN_COL = "Maxlen";

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(CHAR_SET_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(DESC_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(DEFAULT_COLLATION_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(MAX_LEN_COL, ScalarType.createVarchar(20)))
                    .build();

    private String pattern;
    private Expr where;

    public ShowCharsetStmt() {
    }

    public ShowCharsetStmt(String pattern) {
        this.pattern = pattern;
    }

    public ShowCharsetStmt(String pattern, Expr where) {
        this.pattern = pattern;
        this.where = where;
    }

    public String getPattern() {
        return pattern;
    }

    public Expr getWhere() {
        return where;
    }

    @Override
    public void analyze(Analyzer analyzer) {

    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW CHARSET");
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
