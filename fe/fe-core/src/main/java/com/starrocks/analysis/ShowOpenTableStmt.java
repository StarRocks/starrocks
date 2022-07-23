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

import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;

// SHOW OPEN TABLES
public class ShowOpenTableStmt extends ShowStmt {
    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "tables");
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Database", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(10)))
                    .addColumn(new Column("In_use", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Name_locked", ScalarType.createVarchar(64)))
                    .build();

    private String dbName;
    private String pattern;
    private Expr where;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public Expr getWhere() {
        return where;
    }

    public void setWhere(Expr where) {
        this.where = where;
    }

    public ShowOpenTableStmt() {}
    public ShowOpenTableStmt(String dbName, String pattern, Expr where) {
        setDbName(dbName);
        setPattern(pattern);
        setWhere(where);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW OPEN TABLES");
        if (!Strings.isNullOrEmpty(dbName)){
            sb.append(" From ").append(dbName);
        }
        if (pattern != null) {
            sb.append(" LIKE '").append(pattern).append("'");
        }
        if (where != null) {
            sb.append(" WHERE '").append(where).append("'");
        }
        return sb.toString();
    }

    public QueryStatement toSelectStmt() {
        if (where == null) {
            return null;
        }

        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        // Database
        SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, "DATABASE"), "Database");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Database"), item.getExpr().clone(null));
        // Table
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE"), "Table");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Table"), item.getExpr().clone(null));
        // In_use
        item = new SelectListItem(new SlotRef(TABLE_NAME, "IN_USE"), "In_use");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "In_use"), item.getExpr().clone(null));
        // Name_Locked
        item = new SelectListItem(new SlotRef(TABLE_NAME, "NAME_LOCKED"), "Name_Locked");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Name_Locked"), item.getExpr().clone(null));

        where = where.substitute(aliasMap);
        return new QueryStatement(new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                where, null, null));
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowOpenTablesStmt(this, context);
    }

    public boolean isSupportNewPlanner() {
        return true;
    }
    @Override
    public void analyze(Analyzer analyzer) {
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
