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
import com.starrocks.sql.ast.AstVisitor;
import com.google.common.base.Strings;

// SHOW OPEN TABLES
public class ShowOpenTableStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Database", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(10)))
                    .addColumn(new Column("In_use", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Name_locked", ScalarType.createVarchar(64)))
                    .build();

    private String  dbName;
    private String  pattern;
    private Expr  where;
    public String getDbName() {return dbName;}
    public String getPattern() {return pattern;}
    public Expr getWhere() {return where;}
    public void setDbName(String dbName) {this.dbName = dbName;}
    public void setPattern(String pattern) {this.pattern = pattern;}
    public void setWhere(Expr where) {this.where = where;}
    public ShowOpenTableStmt () {}
    public ShowOpenTableStmt (String dbName, String pattern, Expr where) {
        setDbName( dbName);
        setPattern( pattern);
        setWhere( where);
    }
    public boolean isSupportNewPlanner() {
        return true;
    }
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowOpenTablesStmt(this, context);
    }
    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW OPEN TABLES");
        if (!Strings.isNullOrEmpty(dbName)){
            sb.append(" FROM ").append(dbName);
        }
        if (pattern != null) {
            sb.append(" LIKE '").append(pattern).append("'");
        }
        if (where != null) {
            sb.append(" WHERE '").append(where).append("'");
        }
        return sb.toString();
    }
    public String toString() {
        return toSql();
    }
    @Override
    public void analyze(Analyzer analyzer) {
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}