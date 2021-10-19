// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;

/**
 * OperatorVisitor is used to traverse Operator
 * R represents the return value of function visitXXX, C represents the global context
 */
public abstract class OperatorVisitor<R, C> {
    /**
     * The default behavior to perform when visiting a Operator
     */
    public abstract R visitOperator(Operator node, C context);

    /**
     * Logical operator visitor
     */
    public R visitLogicalTableScan(LogicalScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalSchemaScan(LogicalSchemaScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalOlapScan(LogicalOlapScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalHiveScan(LogicalHiveScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalMysqlScan(LogicalMysqlScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalEsScan(LogicalEsScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalProject(LogicalProjectOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalJoin(LogicalJoinOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalAggregation(LogicalAggregationOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalTopN(LogicalTopNOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalAssertOneRow(LogicalAssertOneRowOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalAnalytic(LogicalWindowOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalUnion(LogicalUnionOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalExcept(LogicalExceptOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalIntersect(LogicalIntersectOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalValues(LogicalValuesOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalRepeat(LogicalRepeatOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalFilter(LogicalFilterOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalTableFunction(LogicalTableFunctionOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalLimit(LogicalLimitOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitMockOperator(MockOperator node, C context) {
        return visitOperator(node, context);
    }

    /**
     * Physical operator visitor
     */
    public R visitPhysicalDistribution(PhysicalDistributionOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalProject(PhysicalProjectOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalHashAggregate(PhysicalHashAggregateOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalHashJoin(PhysicalHashJoinOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalOlapScan(PhysicalOlapScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalHiveScan(PhysicalHiveScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalSchemaScan(PhysicalSchemaScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalMysqlScan(PhysicalMysqlScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalEsScan(PhysicalEsScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalTopN(PhysicalTopNOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalAnalytic(PhysicalWindowOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalUnion(PhysicalUnionOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalExcept(PhysicalExceptOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalIntersect(PhysicalIntersectOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalValues(PhysicalValuesOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalRepeat(PhysicalRepeatOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalFilter(PhysicalFilterOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalTableFunction(PhysicalTableFunctionOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalLimit(PhysicalLimitOperator node, C context) {
        return visitOperator(node, context);
    }
}
