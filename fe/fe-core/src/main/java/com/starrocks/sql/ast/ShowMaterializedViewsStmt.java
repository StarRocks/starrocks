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
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.util.Util.normalizeName;

// Show rollup statement, used to show rollup information of one table.
//
// Syntax:
//      SHOW MATERIALIZED VIEWS { FROM | IN } db
public class ShowMaterializedViewsStmt extends EnhancedShowStmt {
    private static final List<String> META_DATA = Arrays.asList(
            "id",
            "database_name",
            "name",
            "refresh_type",
            "is_active",
            "inactive_reason",
            "partition_type",
            "task_id",
            "task_name",
            "last_refresh_start_time",
            "last_refresh_finished_time",
            "last_refresh_duration",
            "last_refresh_state",
            "last_refresh_force_refresh",
            "last_refresh_start_partition",
            "last_refresh_end_partition",
            "last_refresh_base_refresh_partitions",
            "last_refresh_mv_refresh_partitions",
            "last_refresh_error_code",
            "last_refresh_error_message",
            "rows",
            "text",
            "extra_message",
            "query_rewrite_status",
            "creator",
            "last_refresh_process_time",
            "last_refresh_job_id"
    );

    private static final Map<String, String> ALIAS_MAP = ImmutableMap.of(
            "id", "MATERIALIZED_VIEW_ID",
            "database_name", "TABLE_SCHEMA",
            "name", "TABLE_NAME",
            "text", "MATERIALIZED_VIEW_DEFINITION",
            "rows", "TABLE_ROWS"
    );

    private static final TableName TABLE_NAME = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
            InfoSchemaDb.DATABASE_NAME, "materialized_views");

    private String db;

    private String catalogName;

    private final String pattern;

    private Expr where;

    public ShowMaterializedViewsStmt(String catalogName, String db) {
        this(catalogName, db, null, null, NodePosition.ZERO);
    }

    public ShowMaterializedViewsStmt(String catalogName, String db, String pattern) {
        this(catalogName, db, pattern, null, NodePosition.ZERO);
    }

    public ShowMaterializedViewsStmt(String catalogName, String db, Expr where) {
        this(catalogName, db, null, where, NodePosition.ZERO);
    }

    public ShowMaterializedViewsStmt(String catalogName, String db, String pattern, Expr where, NodePosition pos) {
        super(pos);
        this.catalogName = normalizeName(catalogName);
        this.db = normalizeName(db);
        this.pattern = pattern;
        this.where = where;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = normalizeName(db);
    }

    public String getPattern() {
        return pattern;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public QueryStatement toSelectStmt() throws AnalysisException {
        if (where == null) {
            return null;
        }
        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        for (String column : META_DATA) {
            if (ALIAS_MAP.containsKey(column)) {
                SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, ALIAS_MAP.get(column)),
                        column);
                selectList.addItem(item);
                aliasMap.put(new SlotRef(null, column), item.getExpr().clone(null));
            } else {
                SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, column), column);
                selectList.addItem(item);
                aliasMap.put(new SlotRef(null, column), item.getExpr().clone(null));
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
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowMaterializedViewStatement(this, context);
    }
}
