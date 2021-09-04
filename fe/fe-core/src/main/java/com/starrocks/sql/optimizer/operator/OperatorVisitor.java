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
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRow;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistribution;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScan;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExcept;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilter;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregate;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoin;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScan;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersect;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScan;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScan;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProject;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeat;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScan;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunction;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopN;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnion;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValues;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindow;

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

    public R visitMockOperator(MockOperator node, C context) {
        return visitOperator(node, context);
    }

    /**
     * Physical operator visitor
     */
    public R visitPhysicalDistribution(PhysicalDistribution node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalProject(PhysicalProject node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalHashAggregate(PhysicalHashAggregate node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalHashJoin(PhysicalHashJoin node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalOlapScan(PhysicalOlapScan node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalHiveScan(PhysicalHiveScan node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalSchemaScan(PhysicalSchemaScan node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalMysqlScan(PhysicalMysqlScan node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalEsScan(PhysicalEsScan node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalTopN(PhysicalTopN node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalAssertOneRow(PhysicalAssertOneRow node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalAnalytic(PhysicalWindow node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalUnion(PhysicalUnion node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalExcept(PhysicalExcept node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalIntersect(PhysicalIntersect node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalValues(PhysicalValues node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalRepeat(PhysicalRepeat node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalFilter(PhysicalFilter node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalTableFunction(PhysicalTableFunction node, C context) {
        return visitOperator(node, context);
    }
}
