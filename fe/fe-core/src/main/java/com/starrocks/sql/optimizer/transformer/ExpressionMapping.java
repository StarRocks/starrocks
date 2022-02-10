// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.transformer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FieldReference;
import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExpressionMapping {
    /**
     * This structure is responsible for the translation map from Expr to operator
     */
    private final Map<Expr, ColumnRefOperator> expressionToColumns = new HashMap<>();

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

    public ExpressionMapping(Scope scope, List<ColumnRefOperator> fieldMappings) {
        this.scope = scope;
        this.fieldMappings = new ColumnRefOperator[fieldMappings.size()];
        fieldMappings.toArray(this.fieldMappings);
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

    public void setFieldMappings(List<ColumnRefOperator> fieldMappings) {
        this.fieldMappings = new ColumnRefOperator[fieldMappings.size()];
        fieldMappings.toArray(this.fieldMappings);
    }

    public void put(Expr expression, ColumnRefOperator variable) {
        if (expression instanceof FieldReference) {
            fieldMappings[((FieldReference) expression).getFieldIndex()] = variable;
        }

        if (expression instanceof SlotRef) {
            scope.tryResolveFeild((SlotRef) expression)
                    .ifPresent(field -> fieldMappings[field.getRelationFieldIndex()] = variable);
        }
        expressionToColumns.put(expression, variable);
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
}
