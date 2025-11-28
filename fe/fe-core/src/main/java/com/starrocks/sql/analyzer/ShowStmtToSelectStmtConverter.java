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

package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.ShowColumnStmt;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.ShowMaterializedViewsStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowTableStatusStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.ExprSubstitutionVisitor;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

// Utility class to convert ShowStmt instances into equivalent SELECT statements.
public final class ShowStmtToSelectStmtConverter {
    private static final String NAME_COL_PREFIX = "Tables_in_";
    private static final String TYPE_COL = "Table_type";
    private static final TableName SHOW_TABLES_TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "tables");

    private static final TableName SHOW_DB_TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "schemata");
    private static final String DB_COL = "Database";

    private static final TableName SHOW_COLUMNS_TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "COLUMNS");

    private static final TableName SHOW_MATERIALIZED_VIEWS_TABLE_NAME =
            new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    InfoSchemaDb.DATABASE_NAME, "materialized_views");

    private static final List<String> MATERIALIZED_VIEW_META_COLUMNS = Arrays.asList(
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

    private static final Map<String, String> MATERIALIZED_VIEW_ALIAS_MAP = ImmutableMap.of(
            "id", "MATERIALIZED_VIEW_ID",
            "database_name", "TABLE_SCHEMA",
            "name", "TABLE_NAME",
            "text", "MATERIALIZED_VIEW_DEFINITION",
            "rows", "TABLE_ROWS"
    );

    private ShowStmtToSelectStmtConverter() {
    }

    public static QueryStatement toSelectStmt(ShowStmt stmt) throws AnalysisException {
        if (stmt instanceof ShowTableStmt) {
            return buildShowTableQuery((ShowTableStmt) stmt);
        } else if (stmt instanceof ShowDbStmt) {
            return buildShowDbQuery((ShowDbStmt) stmt);
        } else if (stmt instanceof ShowColumnStmt) {
            return buildShowColumnQuery((ShowColumnStmt) stmt);
        } else if (stmt instanceof ShowTableStatusStmt) {
            return buildShowTableStatusQuery((ShowTableStatusStmt) stmt);
        } else if (stmt instanceof ShowMaterializedViewsStmt) {
            return buildShowMaterializedViewsQuery((ShowMaterializedViewsStmt) stmt);
        } else if (stmt instanceof ShowVariablesStmt) {
            return buildShowVariablesQuery((ShowVariablesStmt) stmt);
        }
        return null;
    }

    private static QueryStatement buildShowDbQuery(ShowDbStmt stmt) {
        Expr where = stmt.getWhereClause();
        if (where == null) {
            return null;
        }

        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap();
        SelectListItem item = new SelectListItem(new SlotRef(SHOW_DB_TABLE_NAME, "SCHEMA_NAME"), DB_COL);
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, DB_COL), item.getExpr().clone());
        Expr rewrittenWhere = ExprSubstitutionVisitor.rewrite(where, aliasMap);

        return new QueryStatement(new SelectRelation(selectList, new TableRelation(SHOW_DB_TABLE_NAME),
                rewrittenWhere, null, null), stmt.getOrigStmt());
    }

    private static QueryStatement buildShowTableQuery(ShowTableStmt stmt) {
        Expr where = stmt.getWhereClause();
        if (where == null) {
            return null;
        }

        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap();
        SelectListItem item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "TABLE_NAME"),
                NAME_COL_PREFIX + stmt.getDb());
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, NAME_COL_PREFIX + stmt.getDb()),
                item.getExpr().clone());
        if (stmt.isVerbose()) {
            item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "TABLE_TYPE"), TYPE_COL);
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, TYPE_COL), item.getExpr().clone());
        }
        Expr rewrittenWhere = ExprSubstitutionVisitor.rewrite(where, aliasMap);
        Expr whereDbEQ = new BinaryPredicate(
                BinaryType.EQ,
                new SlotRef(SHOW_TABLES_TABLE_NAME, "TABLE_SCHEMA"),
                new StringLiteral(stmt.getDb()));
        Expr finalWhere = new CompoundPredicate(
                CompoundPredicate.Operator.AND,
                whereDbEQ,
                rewrittenWhere);
        return new QueryStatement(new SelectRelation(selectList, new TableRelation(SHOW_TABLES_TABLE_NAME),
                finalWhere, null, null), stmt.getOrigStmt());
    }

    private static QueryStatement buildShowColumnQuery(ShowColumnStmt stmt) throws AnalysisException {
        Expr where = stmt.getWhereClause();
        if (where == null) {
            return null;
        }

        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap();
        SelectListItem item = new SelectListItem(new SlotRef(SHOW_COLUMNS_TABLE_NAME, "COLUMN_NAME"), "Field");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Field"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_COLUMNS_TABLE_NAME, "DATA_TYPE"), "Type");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Type"), item.getExpr().clone());

        if (stmt.isVerbose()) {
            item = new SelectListItem(new SlotRef(SHOW_COLUMNS_TABLE_NAME, "COLLATION_NAME"), "Collation");
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, "Collation"), item.getExpr().clone());
        }

        item = new SelectListItem(new SlotRef(SHOW_COLUMNS_TABLE_NAME, "IS_NULLABLE"), "Null");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Null"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_COLUMNS_TABLE_NAME, "COLUMN_KEY"), "Key");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Key"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_COLUMNS_TABLE_NAME, "COLUMN_DEFAULT"), "Default");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Default"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_COLUMNS_TABLE_NAME, "EXTRA"), "Extra");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Extra"), item.getExpr().clone());

        if (stmt.isVerbose()) {
            item = new SelectListItem(new SlotRef(SHOW_COLUMNS_TABLE_NAME, "PRIVILEGES"), "Privileges");
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, "Privileges"), item.getExpr().clone());

            item = new SelectListItem(new SlotRef(SHOW_COLUMNS_TABLE_NAME, "COLUMN_COMMENT"), "Comment");
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, "Comment"), item.getExpr().clone());
        }

        Expr rewrittenWhere = ExprSubstitutionVisitor.rewrite(where, aliasMap);
        Expr finalWhere = new CompoundPredicate(CompoundPredicate.Operator.AND, rewrittenWhere,
                new CompoundPredicate(CompoundPredicate.Operator.AND,
                        new BinaryPredicate(BinaryType.EQ, new SlotRef(SHOW_COLUMNS_TABLE_NAME, "TABLE_NAME"),
                                new StringLiteral(stmt.getTableName().getTbl())),
                        new BinaryPredicate(BinaryType.EQ, new SlotRef(SHOW_COLUMNS_TABLE_NAME, "TABLE_SCHEMA"),
                                new StringLiteral(stmt.getTableName().getDb()))));
        return new QueryStatement(new SelectRelation(selectList, new TableRelation(SHOW_COLUMNS_TABLE_NAME),
                finalWhere, null, null), stmt.getOrigStmt());
    }

    private static QueryStatement buildShowTableStatusQuery(ShowTableStatusStmt stmt) {
        Expr where = stmt.getWhereClause();
        if (where == null) {
            return null;
        }

        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap();
        SelectListItem item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "TABLE_NAME"), "Name");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Name"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "ENGINE"), "Engine");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Engine"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "VERSION"), "Version");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Version"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "ROW_FORMAT"), "Row_format");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Row_format"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "TABLE_ROWS"), "Rows");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Rows"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "AVG_ROW_LENGTH"), "Avg_row_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Avg_row_length"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "DATA_LENGTH"), "Data_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Data_length"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "MAX_DATA_LENGTH"), "Max_data_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Max_data_length"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "INDEX_LENGTH"), "Index_length");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Index_length"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "DATA_FREE"), "Data_free");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Data_free"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "AUTO_INCREMENT"), "Auto_increment");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Auto_increment"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "CREATE_TIME"), "Create_time");
        selectList.addItem(item);

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "UPDATE_TIME"), "Update_time");
        selectList.addItem(item);

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "CHECK_TIME"), "Check_time");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Check_time"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "TABLE_COLLATION"), "Collation");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Collation"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "CHECKSUM"), "Checksum");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Checksum"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "CREATE_OPTIONS"), "Create_options");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Create_options"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(SHOW_TABLES_TABLE_NAME, "TABLE_COMMENT"), "Comment");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Comment"), item.getExpr().clone());

        Expr rewrittenWhere = ExprSubstitutionVisitor.rewrite(where, aliasMap);

        return new QueryStatement(new SelectRelation(selectList, new TableRelation(SHOW_TABLES_TABLE_NAME),
                rewrittenWhere, null, null), stmt.getOrigStmt());
    }

    private static QueryStatement buildShowMaterializedViewsQuery(ShowMaterializedViewsStmt stmt) {
        Expr where = stmt.getWhereClause();
        if (where == null) {
            return null;
        }

        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap();
        for (String column : MATERIALIZED_VIEW_META_COLUMNS) {
            if (MATERIALIZED_VIEW_ALIAS_MAP.containsKey(column)) {
                SelectListItem item = new SelectListItem(
                        new SlotRef(SHOW_MATERIALIZED_VIEWS_TABLE_NAME, MATERIALIZED_VIEW_ALIAS_MAP.get(column)),
                        column);
                selectList.addItem(item);
                aliasMap.put(new SlotRef(null, column), item.getExpr().clone());
            } else {
                SelectListItem item = new SelectListItem(new SlotRef(SHOW_MATERIALIZED_VIEWS_TABLE_NAME, column), column);
                selectList.addItem(item);
                aliasMap.put(new SlotRef(null, column), item.getExpr().clone());
            }
        }
        Expr rewrittenWhere = ExprSubstitutionVisitor.rewrite(where, aliasMap);

        Expr whereDbEQ = new BinaryPredicate(
                BinaryType.EQ,
                new SlotRef(SHOW_MATERIALIZED_VIEWS_TABLE_NAME, "TABLE_SCHEMA"),
                new StringLiteral(stmt.getDb()));
        Expr finalWhere = new CompoundPredicate(
                CompoundPredicate.Operator.AND,
                whereDbEQ,
                rewrittenWhere);
        return new QueryStatement(new SelectRelation(selectList, new TableRelation(SHOW_MATERIALIZED_VIEWS_TABLE_NAME),
                finalWhere, null, null), stmt.getOrigStmt());
    }

    private static QueryStatement buildShowVariablesQuery(ShowVariablesStmt stmt) {
        Expr where = stmt.getWhereClause();
        if (where == null) {
            return null;
        }
        SetType type = stmt.getType();
        if (type == null) {
            type = SetType.SESSION;
            stmt.setType(type);
        }
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap();
        TableName tableName;
        if (type == SetType.GLOBAL) {
            tableName = new TableName(InfoSchemaDb.DATABASE_NAME, "GLOBAL_VARIABLES");
        } else if (type == SetType.VERBOSE) {
            tableName = new TableName(InfoSchemaDb.DATABASE_NAME, "VERBOSE_SESSION_VARIABLES");
        } else {
            tableName = new TableName(InfoSchemaDb.DATABASE_NAME, "SESSION_VARIABLES");
        }

        SelectListItem item = new SelectListItem(new SlotRef(tableName, "VARIABLE_NAME"), "Variable_name");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Variable_name"), item.getExpr().clone());

        item = new SelectListItem(new SlotRef(tableName, "VARIABLE_VALUE"), "Value");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "Value"), item.getExpr().clone());

        if (type == SetType.VERBOSE) {
            item = new SelectListItem(new SlotRef(tableName, "Default_value"), "Default_value");
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, "Default_value"), item.getExpr().clone());

            item = new SelectListItem(new SlotRef(tableName, "Is_changed"), "Is_changed");
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, "Is_changed"), item.getExpr().clone());
        }

        Expr rewrittenWhere = ExprSubstitutionVisitor.rewrite(where, aliasMap);

        return new QueryStatement(new SelectRelation(selectList, new TableRelation(tableName),
                rewrittenWhere, null, null), stmt.getOrigStmt());
    }
}
