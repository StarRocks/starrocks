// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.transformer;

import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * OptExprBuilder is used to build OptExpression tree
 */
public class OptExprBuilder {
    private final Operator root;
    private final List<OptExprBuilder> inputs;
    private ExpressionMapping expressionMapping;

    public OptExprBuilder(Operator root, List<OptExprBuilder> inputs, ExpressionMapping expressionMapping) {
        this.root = root;
        this.inputs = inputs;
        this.expressionMapping = expressionMapping;
    }

    public Scope getScope() {
        return expressionMapping.getScope();
    }

    public List<ColumnRefOperator> getFieldMappings() {
        return expressionMapping.getFieldMappings();
    }

    public ExpressionMapping getExpressionMapping() {
        return expressionMapping;
    }

    public void setExpressionMapping(ExpressionMapping expressionMapping) {
        this.expressionMapping = expressionMapping;
    }

    public OptExpression getRoot() {
        if (inputs.size() > 0) {
            return OptExpression
                    .create(root, inputs.stream().map(OptExprBuilder::getRoot).collect(Collectors.toList()));
        } else {
            return new OptExpression(root);
        }
    }

    public List<OptExprBuilder> getInputs() {
        return inputs;
    }

    public void addChild(OptExprBuilder builder) {
        inputs.add(builder);
    }

    public OptExprBuilder withNewRoot(Operator operator) {
        return new OptExprBuilder(operator, Collections.singletonList(this), expressionMapping);
    }
}