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

package com.starrocks.sql.optimizer;

/**
 * OptExpressionVisitor is used to visit operator tree by OptExpression
 * The visitX function can ensure that the root operator of the optExpression must be X.
 * The user can use optExpression to traverse the children node and use optExpression.getOp()
 * to get the current operator node
 */
public abstract class OptExpressionVisitor<R, C> {
    /**
     * The default behavior to perform when visiting a Operator
     */
    public R visit(OptExpression optExpression, C context) {
        return optExpression.getOp().accept(this, optExpression, context);
    }

    /**
     * Logical operator visitor
     */
    public R visitLogicalTableScan(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalProject(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalFilter(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalLimit(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalAggregate(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalTopN(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalJoin(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalApply(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalAssertOneRow(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalWindow(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalUnion(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalExcept(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalIntersect(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalValues(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalRepeat(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalTableFunction(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalTreeAnchor(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalCTEAnchor(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalCTEProduce(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitLogicalCTEConsume(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    /**
     * Physical operator visitor
     */
    public R visitPhysicalScan(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalOlapScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalHiveScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalFileScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalIcebergScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalHudiScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalDeltaLakeScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalPaimonScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalSchemaScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalMysqlScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalEsScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalMetaScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalJDBCScan(OptExpression optExpression, C context) {
        return visitPhysicalScan(optExpression, context);
    }

    public R visitPhysicalProject(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalHashAggregate(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalTopN(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalDistribution(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalHashJoin(OptExpression optExpression, C context) {
        return visitPhysicalJoin(optExpression, context);
    }

    public R visitPhysicalMergeJoin(OptExpression optExpression, C context) {
        return visitPhysicalJoin(optExpression, context);
    }

    public R visitPhysicalJoin(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalNestLoopJoin(OptExpression optExpression, C context) {
        return visitPhysicalJoin(optExpression, context);
    }

    public R visitPhysicalAssertOneRow(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalAnalytic(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalUnion(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalExcept(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalIntersect(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalValues(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalRepeat(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalFilter(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalTableFunction(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalDecode(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalLimit(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalCTEAnchor(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalCTEConsume(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalCTEProduce(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalNoCTE(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalStreamScan(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalStreamJoin(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalStreamAgg(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }

    public R visitPhysicalTableFunctionTableScan(OptExpression optExpression, C context) {
        return visit(optExpression, context);
    }
}