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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

// Show rollup statement, used to show rollup information of one table.
//
// Syntax:
//      SHOW MATERIALIZED VIEWS { FROM | IN } db
public class ShowMaterializedViewsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("id", ScalarType.createVarchar(50)))
                    .addColumn(new Column("name", ScalarType.createVarchar(50)))
                    .addColumn(new Column("database_name", ScalarType.createVarchar(20)))
                    .addColumn(new Column("refresh_type", ScalarType.createVarchar(10)))
                    .addColumn(new Column("is_active", ScalarType.createVarchar(10)))
                    .addColumn(new Column("last_refresh_start_time", ScalarType.createVarchar(20)))
                    .addColumn(new Column("last_refresh_finished_time", ScalarType.createVarchar(20)))
                    .addColumn(new Column("last_refresh_duration", ScalarType.createVarchar(20)))
                    .addColumn(new Column("last_refresh_state", ScalarType.createVarchar(20)))
                    .addColumn(new Column("inactive_code", ScalarType.createVarchar(20)))
                    .addColumn(new Column("inactive_reason", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("text", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("rows", ScalarType.createVarchar(50)))
                    .build();

    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "materialized_views");

    private String db;

    private final String pattern;

    private Expr where;

    public ShowMaterializedViewsStmt(String db) {
        this(db, null, null, NodePosition.ZERO);
    }

    public ShowMaterializedViewsStmt(String db, String pattern) {
        this(db, pattern, null, NodePosition.ZERO);
    }

    public ShowMaterializedViewsStmt(String db, Expr where) {
        this(db, null, where, NodePosition.ZERO);
    }

    public ShowMaterializedViewsStmt(String db, String pattern, Expr where, NodePosition pos) {
        super(pos);
        this.db = db;
        this.pattern = pattern;
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
        // REFRESH_TYPE
        item = new SelectListItem(new SlotRef(TABLE_NAME, "REFRESH_TYPE"), "refresh_type");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "REFRESH_TYPE"), item.getExpr().clone(null));
        // IS_ACTIVE
        item = new SelectListItem(new SlotRef(TABLE_NAME, "IS_ACTIVE"), "is_active");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "IS_ACTIVE"), item.getExpr().clone(null));
        // LAST_REFRESH_START_TIME
        item = new SelectListItem(new SlotRef(TABLE_NAME, "LAST_REFRESH_START_TIME"), "last_refresh_start_time");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "LAST_REFRESH_START_TIME"), item.getExpr().clone(null));
        // LAST_REFRESH_FINISHED_TIME
        item = new SelectListItem(new SlotRef(TABLE_NAME, "LAST_REFRESH_FINISHED_TIME"), "last_refresh_finished_time");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "LAST_REFRESH_FINISHED_TIME"), item.getExpr().clone(null));
        // LAST_REFRESH_DURATION
        item = new SelectListItem(new SlotRef(TABLE_NAME, "LAST_REFRESH_DURATION"), "last_refresh_duration");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "LAST_REFRESH_DURATION"), item.getExpr().clone(null));
        // LAST_REFRESH_STATE
        item = new SelectListItem(new SlotRef(TABLE_NAME, "LAST_REFRESH_STATE"), "last_refresh_state");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "LAST_REFRESH_STATE"), item.getExpr().clone(null));
        // INACTIVE_CODE
        item = new SelectListItem(new SlotRef(TABLE_NAME, "INACTIVE_CODE"), "inactive_code");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "INACTIVE_CODE"), item.getExpr().clone(null));
        // INACTIVE_REASON
        item = new SelectListItem(new SlotRef(TABLE_NAME, "INACTIVE_REASON"), "inactive_reason");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "INACTIVE_REASON"), item.getExpr().clone(null));
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
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowMaterializedViewStatement(this, context);
    }
}
