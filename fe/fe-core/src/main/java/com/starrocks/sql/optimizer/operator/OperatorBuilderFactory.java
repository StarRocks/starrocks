// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;

public class OperatorBuilderFactory {
    public static Operator.Builder<?, ?> build(Operator operator) {
        if (operator instanceof LogicalJoinOperator) {
            return new LogicalJoinOperator.Builder();
        } else if (operator instanceof LogicalAggregationOperator) {
            return new LogicalAggregationOperator.Builder();
        } else if (operator instanceof LogicalTopNOperator) {
            return new LogicalTopNOperator.Builder();
        } else if (operator instanceof LogicalOlapScanOperator) {
            return new LogicalOlapScanOperator.Builder();
        } else if (operator instanceof LogicalHiveScanOperator) {
            return new LogicalHiveScanOperator.Builder();
        } else if (operator instanceof LogicalEsScanOperator) {
            return new LogicalEsScanOperator.Builder();
        } else if (operator instanceof LogicalMysqlScanOperator) {
            return new LogicalMysqlScanOperator.Builder();
        } else if (operator instanceof LogicalSchemaScanOperator) {
            return new LogicalSchemaScanOperator.Builder();
        } else if (operator instanceof LogicalMetaScanOperator) {
            return new LogicalMetaScanOperator.Builder();
        } else if (operator instanceof LogicalValuesOperator) {
            return new LogicalValuesOperator.Builder();
        } else if (operator instanceof LogicalTableFunctionOperator) {
            return new LogicalTableFunctionOperator.Builder();
        } else if (operator instanceof LogicalWindowOperator) {
            return new LogicalWindowOperator.Builder();
        } else if (operator instanceof LogicalUnionOperator) {
            return new LogicalUnionOperator.Builder();
        } else if (operator instanceof LogicalExceptOperator) {
            return new LogicalExceptOperator.Builder();
        } else if (operator instanceof LogicalIntersectOperator) {
            return new LogicalIntersectOperator.Builder();
        } else if (operator instanceof LogicalFilterOperator) {
            return new LogicalFilterOperator.Builder();
        } else if (operator instanceof LogicalAssertOneRowOperator) {
            return new LogicalAssertOneRowOperator.Builder();
        } else if (operator instanceof LogicalRepeatOperator) {
            return new LogicalRepeatOperator.Builder();
        } else {
            throw new StarRocksPlannerException("not implement builder", ErrorType.INTERNAL_ERROR);
        }
    }
}