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

package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFileScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOdpsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionTableScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFileScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOdpsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionTableScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.stream.LogicalBinlogScanOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamJoinOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamScanOperator;

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

    public R visitLogicalBinlogScan(LogicalBinlogScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
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

    public R visitLogicalFileScan(LogicalFileScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalTableFunctionTableScan(LogicalTableFunctionTableScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalIcebergScan(LogicalIcebergScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalDeltaLakeScan(LogicalDeltaLakeScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalPaimonScan(LogicalPaimonScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalOdpsScan(LogicalOdpsScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalHudiScan(LogicalHudiScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalMysqlScan(LogicalMysqlScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalMetaScan(LogicalMetaScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalEsScan(LogicalEsScanOperator node, C context) {
        return visitLogicalTableScan(node, context);
    }

    public R visitLogicalJDBCScan(LogicalJDBCScanOperator node, C context) {
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

    public R visitLogicalTreeAnchor(LogicalTreeAnchorOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalCTEAnchor(LogicalCTEAnchorOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalCTEConsume(LogicalCTEConsumeOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitLogicalCTEProduce(LogicalCTEProduceOperator node, C context) {
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

    public R visitPhysicalMergeJoin(PhysicalMergeJoinOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalNestLoopJoin(PhysicalNestLoopJoinOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalOlapScan(PhysicalOlapScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalHiveScan(PhysicalHiveScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalFileScan(PhysicalFileScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalIcebergScan(PhysicalIcebergScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalHudiScan(PhysicalHudiScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalDeltaLakeScan(PhysicalDeltaLakeScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalPaimonScan(PhysicalPaimonScanOperator node, C context) {
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

    public R visitPhysicalMetaScan(PhysicalMetaScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalJDBCScan(PhysicalJDBCScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalOdpsScan(PhysicalOdpsScanOperator node, C context) {
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

    public R visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalCTEProduce(PhysicalCTEProduceOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalCTEConsume(PhysicalCTEConsumeOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalNoCTE(PhysicalNoCTEOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalStreamScan(PhysicalStreamScanOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalStreamJoin(PhysicalStreamJoinOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalStreamAgg(PhysicalStreamAggOperator node, C context) {
        return visitOperator(node, context);
    }

    public R visitPhysicalTableFunctionTableScan(PhysicalTableFunctionTableScanOperator node, C context) {
        return visitOperator(node, context);
    }
}
