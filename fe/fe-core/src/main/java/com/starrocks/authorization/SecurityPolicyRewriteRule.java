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

package com.starrocks.authorization;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SecurityPolicyRewriteRule {
    /**
     * Marks all relations in one executable statement for policy lookup during analysis.
     * Deferred statements such as PREPARE deliberately stay opaque; their executable inner
     * statement is marked explicitly at the PREPARE metadata and EXECUTE boundaries.
     */
    public static void markRelationsForRewrite(ParseNode statement) {
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitRelation(Relation relation, Void context) {
                relation.setNeedRewrittenByPolicy(true);
                return null;
            }
        }.visit(statement);
    }

    /**
     * Checks the current authorization context without mutating the analyzed relation. This is
     * used before reusing a cached prepared point-query plan: active policies require a fresh AST
     * and a normal policy rewrite.
     */
    public static boolean hasPolicy(ConnectContext context, Relation relation) {
        if (relation instanceof TableRelation && ((TableRelation) relation).isSyncMVQuery()) {
            return false;
        }

        List<Column> columns;
        TableName tableName;
        if (relation instanceof ViewRelation) {
            ViewRelation viewRelation = (ViewRelation) relation;
            columns = viewRelation.getView().getBaseSchema();
            tableName = viewRelation.getName();
        } else if (relation instanceof TableRelation) {
            TableRelation tableRelation = (TableRelation) relation;
            if (tableRelation.getTable() == null) {
                return true;
            }
            columns = tableRelation.getTable().getBaseSchema();
            tableName = tableRelation.getName();
        } else {
            return true;
        }

        List<Column> validColumns = columns.stream().filter(c -> !c.getType().isUnknown()).collect(Collectors.toList());
        Map<String, Expr> maskingExprMap = Authorizer.getColumnMaskingPolicy(context, tableName, validColumns);
        Expr rowAccessExpr = Authorizer.getRowAccessPolicy(context, tableName);
        return (maskingExprMap != null && !maskingExprMap.isEmpty()) || rowAccessExpr != null;
    }

    /**
     * Checks every physical table or view reachable from an analyzed statement. A point query may
     * still contain scalar subqueries in its select list, so checking only its outer relation is
     * not sufficient before reusing a prepared plan.
     */
    public static boolean hasPolicy(ConnectContext context, ParseNode statement) {
        boolean[] policyFound = {false};
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitTable(TableRelation node, Void ignored) {
                if (!policyFound[0] && SecurityPolicyRewriteRule.hasPolicy(context, node)) {
                    policyFound[0] = true;
                }
                return null;
            }

            @Override
            public Void visitView(ViewRelation node, Void ignored) {
                if (!policyFound[0] && SecurityPolicyRewriteRule.hasPolicy(context, node)) {
                    policyFound[0] = true;
                }
                // A cached plan scans the expanded view definition. Policies newly added to a
                // base table are therefore just as relevant as policies attached to the view.
                return super.visitView(node, ignored);
            }
        }.visit(statement);
        return policyFound[0];
    }

    public static QueryStatement buildView(ConnectContext context, Relation relation, TableName tableName) {
        if (relation instanceof TableRelation && ((TableRelation) relation).isSyncMVQuery()) {
            return null;
        }

        List<Column> columns;
        if (relation instanceof ViewRelation) {
            ViewRelation viewRelation = (ViewRelation) relation;
            columns = viewRelation.getView().getBaseSchema();
        } else if (relation instanceof TableRelation) {
            TableRelation tableRelation = (TableRelation) relation;
            columns = tableRelation.getTable().getBaseSchema();
        } else {
            return null;
        }

        List<Column> validColumns = columns.stream().filter(c -> !c.getType().isUnknown()).collect(Collectors.toList());
        Map<String, Expr> maskingExprMap = Authorizer.getColumnMaskingPolicy(context, tableName, validColumns);
        Expr rowAccessExpr = Authorizer.getRowAccessPolicy(context, tableName);
        if ((maskingExprMap == null || maskingExprMap.isEmpty()) && rowAccessExpr == null) {
            return null;
        }

        List<SelectListItem> selectListItemList = new ArrayList<>();
        for (Column column : validColumns) {
            String columnName = column.getName();
            if (maskingExprMap != null && maskingExprMap.containsKey(columnName)) {
                Expr maskingExpr = maskingExprMap.get(columnName);
                selectListItemList.add(new SelectListItem(maskingExpr, columnName, NodePosition.ZERO));
                Map<TableName, Relation> allTablesRelations = AnalyzerUtils.collectAllTableAndViewRelations(maskingExpr);
                allTablesRelations.values().forEach(r -> r.setCreateByPolicyRewritten(true));
            } else {
                selectListItemList.add(new SelectListItem(new SlotRef(tableName, columnName), columnName, NodePosition.ZERO));
            }
        }

        if (rowAccessExpr != null) {
            Map<TableName, Relation> allTablesRelations = AnalyzerUtils.collectAllTableAndViewRelations(rowAccessExpr);
            allTablesRelations.values().forEach(r -> r.setCreateByPolicyRewritten(true));
        }

        // Eliminate the effects of aliases
        // `select v1 from tbl t` is rewritten as `select t.v1 from (select tbl.v1 from tbl) t`
        // If the influence of alias is not eliminated, it will cause tbl.v1 resolve error.
        if (relation.getAlias() != null) {
            relation.setAlias(null);
        }

        SelectRelation selectRelation = new SelectRelation(new SelectList(selectListItemList, false),
                relation, rowAccessExpr, null, null);
        selectRelation.setOrderBy(Collections.emptyList());
        return new QueryStatement(selectRelation);
    }
}
