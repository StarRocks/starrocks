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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.GroupByClause;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FieldReference;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Validates where and how the built-in {@code search()} function may be used. */
final class SearchFunctionValidator {
    private SearchFunctionValidator() {
    }

    /**
     * Validates one query block before its search predicates are lowered.
     *
     * @return the resolved source context, or {@code null} when the block has no built-in search predicate
     */
    static RelationContext validateQueryBlock(SelectRelation select) {
        Expr where = select.getWhereClause();
        if (where == null || !containsSearch(where)) {
            return null;
        }
        RelationContext context = RelationContext.resolve(select.getRelation());
        if (context == null) {
            throw new SemanticException("search() currently supports WHERE on one OLAP table only, "
                            + "directly or through pass-through subqueries",
                    where.getPos());
        }
        validateWherePredicate(where);
        return context;
    }

    static void validatePlacement(SelectRelation select) {
        rejectSearchOutsideWhere(select);
    }

    /** Maps fields visible in one query block to direct columns of its single underlying OLAP table. */
    static final class RelationContext {
        private final OlapTable table;
        private final String visibleRelationName;
        private final Set<String> visibleFields;
        private final Map<String, Column> directColumns;

        private RelationContext(OlapTable table, String visibleRelationName,
                                Set<String> visibleFields, Map<String, Column> directColumns) {
            this.table = table;
            this.visibleRelationName = visibleRelationName;
            this.visibleFields = visibleFields;
            this.directColumns = directColumns;
        }

        static RelationContext resolve(Relation relation) {
            if (relation instanceof TableRelation) {
                TableRelation tableRelation = (TableRelation) relation;
                if (!(tableRelation.getTable() instanceof OlapTable)) {
                    return null;
                }
                OlapTable table = (OlapTable) tableRelation.getTable();
                Set<String> visibleFields = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                Map<String, Column> directColumns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (Field field : relation.getRelationFields().getAllVisibleFields()) {
                    visibleFields.add(field.getName());
                    Column column = table.getColumn(field.getName());
                    if (column != null) {
                        directColumns.put(field.getName(), column);
                    }
                }
                return new RelationContext(table, visibleName(relation), visibleFields, directColumns);
            }
            if (!(relation instanceof SubqueryRelation)) {
                return null;
            }

            QueryRelation query = ((SubqueryRelation) relation).getQueryStatement().getQueryRelation();
            if (!(query instanceof SelectRelation)) {
                return null;
            }
            SelectRelation select = (SelectRelation) query;
            if (select.hasAggregation() || select.hasAnalyticInfo() || select.hasOrderByClause() || select.hasLimit()
                    || select.getHavingClause() != null || select.hasWithClause()) {
                return null;
            }
            RelationContext source = resolve(select.getRelation());
            if (source == null) {
                return null;
            }

            List<Field> outputFields = relation.getRelationFields().getAllVisibleFields();
            List<Expr> outputExpressions = select.getOutputExpression();
            if (outputFields.size() != outputExpressions.size()) {
                return null;
            }
            Set<String> visibleFields = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Map<String, Column> directColumns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            List<Field> sourceFields = select.getRelation().getRelationFields().getAllFields();
            for (int i = 0; i < outputFields.size(); i++) {
                Field outputField = outputFields.get(i);
                visibleFields.add(outputField.getName());
                String sourceField = directSourceField(outputExpressions.get(i), sourceFields);
                if (sourceField != null) {
                    Column column = source.directColumns.get(sourceField);
                    if (column != null) {
                        directColumns.put(outputField.getName(), column);
                    }
                }
            }
            return new RelationContext(source.table, visibleName(relation), visibleFields, directColumns);
        }

        private static String directSourceField(Expr expression, List<Field> sourceFields) {
            if (expression instanceof SlotRef) {
                return ((SlotRef) expression).getColumnName();
            }
            if (expression instanceof FieldReference) {
                int fieldIndex = ((FieldReference) expression).getFieldIndex();
                return fieldIndex >= 0 && fieldIndex < sourceFields.size()
                        ? sourceFields.get(fieldIndex).getName() : null;
            }
            return null;
        }

        private static String visibleName(Relation relation) {
            TableName name = relation.getResolveTableName();
            return name == null ? null : name.getTbl();
        }

        OlapTable getTable() {
            return table;
        }

        String getVisibleRelationName() {
            return visibleRelationName;
        }

        Column resolveColumn(String visibleColumn, String fieldName, NodePosition position) {
            if (!visibleFields.contains(visibleColumn)) {
                throw new SemanticException("unknown search() field '" + fieldName + "'", position);
            }
            Column column = directColumns.get(visibleColumn);
            if (column == null) {
                throw new SemanticException("search() field '" + fieldName
                        + "' is not a direct column reference and cannot be pushed down", position);
            }
            return column;
        }
    }

    static void rejectMaterializedView(QueryStatement statement) {
        if (containsSearchInQueryTree(statement.getQueryRelation())) {
            throw new SemanticException("search() is not supported in materialized view definitions; "
                    + "use the equivalent MATCH predicates so the persisted definition has stable semantics",
                    statement.getPos());
        }
    }

    static void rejectPreparedStatement(QueryStatement statement) {
        if (containsSearchInQueryTree(statement.getQueryRelation())) {
            throw new SemanticException("search() is not supported in prepared statements", statement.getPos());
        }
    }

    private static void validateWherePredicate(Expr expression) {
        if (expression instanceof FunctionCallExpr
                && SearchFunctionResolver.isBuiltinSearchInvocation((FunctionCallExpr) expression)) {
            validateInvocation((FunctionCallExpr) expression);
            return;
        }
        if (expression instanceof CompoundPredicate) {
            for (Expr child : expression.getChildren()) {
                validateWherePredicate(child);
            }
            return;
        }
        if (containsSearch(expression)) {
            throw new SemanticException("search() must be a boolean leaf combined only with AND, OR, NOT, "
                    + "and parentheses", expression.getPos());
        }
    }

    // search() is rewritten before normal function analysis, so validate its call shape here before
    // the rewriter assumes one or two StringLiteral children.
    private static void validateInvocation(FunctionCallExpr call) {
        FunctionParams params = call.getParams();
        if (params.isStar() || params.isDistinct()
                || (params.getExprsNames() != null && !params.getExprsNames().isEmpty())
                || (params.getOrderByElements() != null && !params.getOrderByElements().isEmpty())) {
            throw new SemanticException("search() only supports plain positional arguments", call.getPos());
        }
        if (call.getChildren().size() < 1 || call.getChildren().size() > 2) {
            throw new SemanticException("search() expects one DSL string and an optional options string",
                    call.getPos());
        }
        if (call.getChildren().stream().anyMatch(child -> !(child instanceof StringLiteral))) {
            throw new SemanticException("search() arguments must be constant strings", call.getPos());
        }
    }

    private static void rejectSearchOutsideWhere(SelectRelation select) {
        for (SelectListItem item : select.getSelectList().getItems()) {
            if (!item.isStar()) {
                rejectIfContainsSearch(item.getExpr(), "SELECT list");
            }
        }
        rejectIfContainsSearch(select.getHavingClause(), "HAVING");
        if (select.getOrderBy() != null) {
            for (OrderByElement orderBy : select.getOrderBy()) {
                rejectIfContainsSearch(orderBy.getExpr(), "ORDER BY");
            }
        }

        GroupByClause groupBy = select.getGroupByClause();
        if (groupBy != null) {
            rejectSearchInExpressions(groupBy.getOriGroupingExprs(), "GROUP BY");
            if (groupBy.getGroupingSetList() != null) {
                for (List<Expr> groupingSet : groupBy.getGroupingSetList()) {
                    rejectSearchInExpressions(groupingSet, "GROUP BY");
                }
            }
        }
        rejectSearchInJoinPredicates(select.getRelation());
    }

    private static void rejectSearchInJoinPredicates(Relation relation) {
        if (!(relation instanceof JoinRelation)) {
            return;
        }
        JoinRelation join = (JoinRelation) relation;
        rejectIfContainsSearch(join.getOnPredicate(), "JOIN ON");
        rejectSearchInJoinPredicates(join.getLeft());
        rejectSearchInJoinPredicates(join.getRight());
    }

    private static void rejectSearchInExpressions(List<Expr> expressions, String clause) {
        if (expressions == null) {
            return;
        }
        for (Expr expression : expressions) {
            rejectIfContainsSearch(expression, clause);
        }
    }

    private static void rejectIfContainsSearch(Expr expression, String clause) {
        if (expression != null && containsSearch(expression)) {
            throw new SemanticException("search() can only be used as a WHERE predicate, not in " + clause,
                    expression.getPos());
        }
    }

    private static boolean containsSearch(Expr expression) {
        if (expression == null) {
            return false;
        }
        if (expression instanceof Subquery) {
            // A nested query is a new query block. Its own QueryAnalyzer invocation decides
            // whether search() occurs in a supported clause and table scope.
            return false;
        }
        if (expression instanceof FunctionCallExpr
                && SearchFunctionResolver.isBuiltinSearchInvocation((FunctionCallExpr) expression)) {
            return true;
        }
        for (Expr child : expression.getChildren()) {
            if (containsSearch(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsSearchInQueryTree(QueryRelation query) {
        for (CTERelation cte : query.getCteRelations()) {
            if (containsSearchInQueryTree(cte.getCteQueryStatement().getQueryRelation())) {
                return true;
            }
        }
        if (query.getOrderBy() != null && query.getOrderBy().stream()
                .anyMatch(element -> containsSearchDeep(element.getExpr()))) {
            return true;
        }
        if (query instanceof SelectRelation) {
            SelectRelation select = (SelectRelation) query;
            for (SelectListItem item : select.getSelectList().getItems()) {
                if (!item.isStar() && containsSearchDeep(item.getExpr())) {
                    return true;
                }
            }
            GroupByClause groupBy = select.getGroupByClause();
            return containsSearchDeep(select.getWhereClause())
                    || containsSearchDeep(select.getHavingClause())
                    || (groupBy != null && containsSearchDeep(groupBy.getOriGroupingExprs()))
                    || (groupBy != null && groupBy.getGroupingSetList() != null
                        && groupBy.getGroupingSetList().stream()
                        .anyMatch(SearchFunctionValidator::containsSearchDeep))
                    || containsSearchInRelationTree(select.getRelation());
        }
        if (query instanceof SetOperationRelation) {
            return ((SetOperationRelation) query).getRelations().stream()
                    .anyMatch(SearchFunctionValidator::containsSearchInQueryTree);
        }
        if (query instanceof SubqueryRelation) {
            return containsSearchInQueryTree(((SubqueryRelation) query).getQueryStatement().getQueryRelation());
        }
        if (query instanceof ValuesRelation) {
            return ((ValuesRelation) query).getRows().stream()
                    .anyMatch(SearchFunctionValidator::containsSearchDeep);
        }
        return false;
    }

    private static boolean containsSearchInRelationTree(Relation relation) {
        if (relation instanceof SubqueryRelation) {
            return containsSearchInQueryTree(((SubqueryRelation) relation).getQueryStatement().getQueryRelation());
        }
        if (relation instanceof JoinRelation) {
            JoinRelation join = (JoinRelation) relation;
            return containsSearchDeep(join.getOnPredicate())
                    || containsSearchInRelationTree(join.getLeft())
                    || containsSearchInRelationTree(join.getRight());
        }
        return false;
    }

    private static boolean containsSearchDeep(Expr expression) {
        if (expression == null) {
            return false;
        }
        if (containsSearch(expression)) {
            return true;
        }

        List<Subquery> subqueries = new ArrayList<>();
        expression.collect(Subquery.class, subqueries);
        return subqueries.stream().anyMatch(subquery ->
                containsSearchInQueryTree(subquery.getQueryStatement().getQueryRelation()));
    }

    private static boolean containsSearchDeep(List<Expr> expressions) {
        return expressions != null
                && expressions.stream().anyMatch(SearchFunctionValidator::containsSearchDeep);
    }
}
