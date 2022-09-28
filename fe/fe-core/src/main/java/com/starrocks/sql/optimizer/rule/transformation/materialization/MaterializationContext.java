// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;

import java.util.List;

public class MaterializationContext {
    private MaterializedView mv;
    // scan materialized view operator
    private Operator scanMvOperator;
    // Logical OptExpression for query of materialized view
    private OptExpression mvExpression;

    // for column -> relationId mapping, column -> table mapping
    private ColumnRefFactory mvColumnRefFactory;

    public MaterializationContext(MaterializedView mv,
                                  Operator scanMvOperator,
                                  OptExpression mvExpression,
                                  ColumnRefFactory columnRefFactory) {
        this.mv = mv;
        this.scanMvOperator = scanMvOperator;
        this.mvExpression = mvExpression;
        this.mvColumnRefFactory = columnRefFactory;
    }

    public MaterializedView getMv() {
        return mv;
    }

    public Operator getScanMvOperator() {
        return scanMvOperator;
    }

    public OptExpression getMvExpression() {
        return mvExpression;
    }

    public ColumnRefFactory getMvColumnRefFactory() {
        return mvColumnRefFactory;
    }
}
