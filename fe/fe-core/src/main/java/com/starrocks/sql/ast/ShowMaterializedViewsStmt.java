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

import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

// Show rollup statement, used to show rollup information of one table.
//
// Syntax:
//      SHOW MATERIALIZED VIEWS { FROM | IN } db
public class ShowMaterializedViewsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .column("id", ScalarType.createVarchar(50))
                    .column("database_name", ScalarType.createVarchar(20))
                    .column("name", ScalarType.createVarchar(50))
                    .column("refresh_type", ScalarType.createVarchar(10))
                    .column("is_active", ScalarType.createVarchar(10))
                    .column("inactive_reason", ScalarType.createVarcharType(64))
                    .column("partition_type", ScalarType.createVarchar(16))
                    .column("task_id", ScalarType.createVarchar(20))
                    .column("task_name", ScalarType.createVarchar(50))
                    .column("last_refresh_start_time", ScalarType.createVarchar(20))
                    .column("last_refresh_finished_time", ScalarType.createVarchar(20))
                    .column("last_refresh_duration", ScalarType.createVarchar(20))
                    .column("last_refresh_state", ScalarType.createVarchar(20))
                    .column("last_refresh_force_refresh", ScalarType.createVarchar(8))
                    .column("last_refresh_start_partition", ScalarType.createVarchar(1024))
                    .column("last_refresh_end_partition", ScalarType.createVarchar(1024))
                    .column("last_refresh_base_refresh_partitions", ScalarType.createVarchar(1024))
                    .column("last_refresh_mv_refresh_partitions", ScalarType.createVarchar(1024))
                    .column("last_refresh_error_code", ScalarType.createVarchar(20))
                    .column("last_refresh_error_message", ScalarType.createVarchar(1024))
                    .column("rows", ScalarType.createVarchar(50))
                    .column("text", ScalarType.createVarchar(1024))
                    .build();

    private static final Map<String, String> ALIAS_MAP = ImmutableMap.of(
            "id", "MATERIALIZED_VIEW_ID",
            "database_name", "TABLE_SCHEMA",
            "name", "TABLE_NAME",
            "text", "MATERIALIZED_VIEW_DEFINITION",
            "rows", "TABLE_ROWS"
    );

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
        for (Column column : META_DATA.getColumns()) {
            if (ALIAS_MAP.containsKey(column.getName())) {
                SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, ALIAS_MAP.get(column.getName())),
                        column.getName());
                selectList.addItem(item);
                aliasMap.put(new SlotRef(null, column.getName()), item.getExpr().clone(null));
            } else {
                SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, column.getName()), column.getName());
                selectList.addItem(item);
                aliasMap.put(new SlotRef(null, column.getName()), item.getExpr().clone(null));
            }
        }
        where = where.substitute(aliasMap);

        // where databases_name = currentdb
        Expr whereDbEQ = new BinaryPredicate(
                BinaryType.EQ,
                new SlotRef(TABLE_NAME, "TABLE_SCHEMA"),
                new StringLiteral(db));
        // old where + and + db where
        Expr finalWhere = new CompoundPredicate(
                CompoundPredicate.Operator.AND,
                whereDbEQ,
                where);
        return new QueryStatement(new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                finalWhere, null, null), this.getOrigStmt());
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
