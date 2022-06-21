// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowMaterializedViewStmt.java

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
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;

// Show rollup statement, used to show rollup information of one table.
//
// Syntax:
//      SHOW MATERIALIZED VIEW { FROM | IN } db
public class ShowMaterializedViewStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("id", ScalarType.createVarchar(50)))
                    .addColumn(new Column("name", ScalarType.createVarchar(50)))
                    .addColumn(new Column("database_name", ScalarType.createVarchar(20)))
                    .addColumn(new Column("text", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("rows", ScalarType.createVarchar(50)))
                    .build();

    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "materialized_views");

    private String db;

    private final String pattern;

    private Expr where;

    public ShowMaterializedViewStmt(String db) {
        this.db = db;
        this.pattern = null;
        this.where = null;
    }

    public ShowMaterializedViewStmt(String db, String pattern) {
        this.db = db;
        this.pattern = pattern;
        this.where = null;
    }

    public ShowMaterializedViewStmt(String db, Expr where) {
        this.db = db;
        this.pattern = null;
        this.where = where;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    public QueryStatement toSelectStmt() throws AnalysisException {
        if (where == null) {
            return null;
        }
        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        // id
        SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, "MATERIALIZED_VIEW_ID"), "id");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "id"), item.getExpr().clone(null));
        // name
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_NAME"), "name");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "name"), item.getExpr().clone(null));
        // database_name
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_SCHEMA"), "database_name");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "database_name"), item.getExpr().clone(null));
        // text
        item = new SelectListItem(new SlotRef(TABLE_NAME, "MATERIALIZED_VIEW_DEFINITION"), "text");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "text"), item.getExpr().clone(null));
        // rows
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_ROWS"), "rows");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "rows"), item.getExpr().clone(null));
        where = where.substitute(aliasMap);
        return new QueryStatement(new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                where, null, null));
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW MATERIALIZED VIEW");
        if (!Strings.isNullOrEmpty(db)) {
            sb.append(" FROM ").append(db);
        }
        if (pattern != null) {
            sb.append(" LIKE '").append(pattern).append("'");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowMaterializedViewStmt(this, context);
    }
}
