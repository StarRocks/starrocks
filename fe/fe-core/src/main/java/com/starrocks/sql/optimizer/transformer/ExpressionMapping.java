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

package com.starrocks.sql.optimizer.transformer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExpressionMapping {

    /**
     * This structure is responsible for the translation map from Expr to operator
     */
    private Map<Expr, ColumnRefOperator> expressionToColumns = new HashMap<>();
    /**
     * The purpose of below property is to hold the current plan built so far,
     * and the mapping to indicate how the fields (by position) in the relation map to
     * the outputs of the plan.
     * <p>
     * fieldMappings are resolved within scopes hierarchy.
     * Indexes of resolved parent scope fields start from "total number of child scope fields".
     * For instance if a child scope has n fields, then first parent scope field
     * will have index n.
     */
    private final Scope scope;
    private ColumnRefOperator[] fieldMappings;
    private RelationId outerScopeRelationId;

    public ExpressionMapping(Scope scope, List<ColumnRefOperator> fieldMappings) {
        this.scope = scope;
        this.fieldMappings = new ColumnRefOperator[fieldMappings.size()];
        fieldMappings.toArray(this.fieldMappings);
    }

    public ExpressionMapping(Scope scope, List<ColumnRefOperator> fieldMappings, ExpressionMapping outer) {
        this.scope = scope;
        List<ColumnRefOperator> fieldsList = new ArrayList<>(fieldMappings);
        if (outer != null) {
            this.scope.setParent(outer.getScope());
            fieldsList.addAll(outer.getFieldMappings());
            this.outerScopeRelationId = outer.getScope().getRelationId();
            if (scope.isLambdaScope()) { // lambda can use outer scope's expression, like agg.
                this.expressionToColumns = outer.expressionToColumns;
            }
        }
        this.fieldMappings = new ColumnRefOperator[fieldsList.size()];
        fieldsList.toArray(this.fieldMappings);
    }

    public ExpressionMapping(Scope scope) {
        this.scope = scope;

        int fieldMappingSize = 0;
        while (scope != null) {
            fieldMappingSize += scope.getRelationFields().getAllFields().size();
            scope = scope.getParent();
        }
        this.fieldMappings = new ColumnRefOperator[fieldMappingSize];
    }

    public Scope getScope() {
        return scope;
    }

    public List<ColumnRefOperator> getFieldMappings() {
        return Arrays.asList(fieldMappings);
    }

    public ColumnRefOperator getColumnRefWithIndex(int fieldIndex) {
        if (fieldIndex > fieldMappings.length) {
            throw new StarRocksPlannerException(
                    String.format("Get columnRef with index %d out fieldMappings length", fieldIndex),
                    ErrorType.INTERNAL_ERROR);
        }
        return fieldMappings[fieldIndex];
    }

    public RelationId getOuterScopeRelationId() {
        return outerScopeRelationId;
    }

    public void setFieldMappings(List<ColumnRefOperator> fieldMappings) {
        this.fieldMappings = new ColumnRefOperator[fieldMappings.size()];
        fieldMappings.toArray(this.fieldMappings);
    }

    public void put(Expr expression, ColumnRefOperator columnRefOperator) {
        if (expression instanceof FieldReference) {
            fieldMappings[((FieldReference) expression).getFieldIndex()] = columnRefOperator;
        }
        // slotRef with struct origin type is subfieldExpr, can not put into fieldMappings.
        if (expression instanceof SlotRef && !expression.getTrueOriginType().isStructType()) {
            scope.tryResolveField((SlotRef) expression)
                    .ifPresent(field -> fieldMappings[field.getRelationFieldIndex()] = columnRefOperator);
        }

        expressionToColumns.put(expression, columnRefOperator);
    }

    public void putWithSymbol(Expr expression, Expr resolveExpr, ColumnRefOperator columnRefOperator) {
        if (resolveExpr instanceof SlotRef) {
            if (expression instanceof SlotRef
                    && ((SlotRef) expression).getColumnName().equals(((SlotRef) resolveExpr).getColumnName())) {
                // There is no alias, and it is an expression of SlotRef,
                // which is resolved according to the original expression
                scope.tryResolveField((SlotRef) expression)
                        .ifPresent(field -> fieldMappings[field.getRelationFieldIndex()] = columnRefOperator);
            } else {
                scope.tryResolveField((SlotRef) resolveExpr)
                        .ifPresent(field -> fieldMappings[field.getRelationFieldIndex()] = columnRefOperator);
            }
        }

        expressionToColumns.put(expression, columnRefOperator);
    }

    public boolean hasExpression(Expr expr) {
        return expressionToColumns.containsKey(expr);
    }

    public ColumnRefOperator get(Expr expression) {
        return expressionToColumns.get(expression);
    }

    public List<Expr> getAllExpressions() {
        return Lists.newArrayList(expressionToColumns.keySet());
    }

    public Map<Expr, ColumnRefOperator> getExpressionToColumns() {
        return expressionToColumns;
    }

    public void addExpressionToColumns(Map<Expr, ColumnRefOperator> expressionToColumns) {
        this.expressionToColumns.putAll(expressionToColumns);
    }
}
