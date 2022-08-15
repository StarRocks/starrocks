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

/**
 * Show collation statement:
 * Acceptable syntax:
 *  SHOW COLLATION
 *     [LIKE 'pattern' | WHERE expr]
 */
public class ShowCollationStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Collation", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Charset", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Id", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Compiled", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Sortlen", ScalarType.createType(PrimitiveType.BIGINT)))
                    .build();

    private String pattern;
    private Expr where;

    public ShowCollationStmt() {

    }

    public ShowCollationStmt(String pattern) {
        this.pattern = pattern;
    }

    public ShowCollationStmt(String pattern, Expr where) {
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
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW COLLATION");
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
