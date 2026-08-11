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

package com.starrocks.sql.optimizer.rule.ivm;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalVersionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Enterprise-only tests for {@link IvmDeltaRetractableAggregateRule}: the retractable-PK recompute plan,
 * the mixed-join rejection, and the mutual exclusion with the append-only {@link IvmDeltaAggregateRule}.
 * Kept separate from {@link IvmAggregateRuleTest} (the shared append-only test) to match the production
 * split and keep the community sync surface small.
 */
public class IvmDeltaRetractableAggregateRuleTest {

    @Test
    public void testSubtreeHasRetractablePkScanDetectsCloudNativePk(@Mocked OlapTable table) {
        mockOlapPk(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        Map<ColumnRefOperator, Column> colRefMap = Maps.newHashMap();
        colRefMap.put(factory.create("id", IntegerType.INT, false), new Column("id", IntegerType.INT, false));
        OptExpression scan = newOlapPkScan(table, colRefMap);
        Assertions.assertTrue(IvmDeltaRetractableAggregateRule.subtreeHasRetractablePkScan(scan));
        Assertions.assertTrue(IvmDeltaRetractableAggregateRule.allLeafScansAreRetractablePk(scan));
    }

    @Test
    public void testSubtreeHasRetractablePkScanRejectsIceberg(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptExpression scan = newIcebergScan(factory, table);
        Assertions.assertFalse(IvmDeltaRetractableAggregateRule.subtreeHasRetractablePkScan(scan));
    }

    @Test
    public void testTransformProducesSingleSnapshotPlan(@Mocked OlapTable table) {
        mockOlapPk(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        OptExpression aggExpr = buildDeltaAggregateOverPk(factory, table);
        deriveLogicalProperty(aggExpr);

        List<OptExpression> result = new IvmDeltaRetractableAggregateRule().transform(aggExpr, context);
        Assertions.assertEquals(1, result.size());
        OptExpression root = result.get(0);
        Assertions.assertTrue(root.getOp() instanceof LogicalCTEAnchorOperator);
        Assertions.assertTrue(root.inputAt(1).getOp() instanceof LogicalProjectOperator);
        LogicalJoinOperator join = root.inputAt(1).inputAt(0).getOp().cast();
        Assertions.assertEquals(JoinOperator.LEFT_OUTER_JOIN, join.getJoinType());
        // The point of the plan: the FROM snapshot is gone, so the base is read once, not twice.
        Assertions.assertEquals(1, countVersions(root, LogicalVersionOperator.VersionRefType.TO_VERSION));
        Assertions.assertEquals(0, countVersions(root, LogicalVersionOperator.VersionRefType.FROM_VERSION));
    }

    @Test
    public void testTransformFallsBackWhenEmptyGroupValueIsUnnameable(@Mocked OlapTable table) {
        mockOlapPk(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        // A non-nullable MAX state: an emptied group has no value this rule can name for it, unlike a count,
        // so it must keep rebuilding the old aggregate from the FROM snapshot instead of guessing.
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);
        ColumnRefOperator gRef = factory.create("g", IntegerType.INT, false);
        ColumnRefOperator vRef = factory.create("v", IntegerType.BIGINT, true);
        Map<ColumnRefOperator, Column> colRefMap = Maps.newHashMap();
        colRefMap.put(gRef, new Column("g", IntegerType.INT, false));
        colRefMap.put(vRef, new Column("v", IntegerType.BIGINT, true));
        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(factory.create("max_combine", IntegerType.BIGINT, false),
                new CallOperator("max_combine", IntegerType.BIGINT, List.of(vRef)));
        OptExpression aggExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL, List.of(gRef), aggMap),
                        newOlapPkScan(table, colRefMap)));
        deriveLogicalProperty(aggExpr);

        OptExpression root = new IvmDeltaRetractableAggregateRule().transform(aggExpr, context).get(0);
        Assertions.assertTrue(root.getOp() instanceof LogicalCTEAnchorOperator);
        Assertions.assertTrue(root.inputAt(1).getOp() instanceof LogicalAggregationOperator);
        Assertions.assertTrue(root.inputAt(1).inputAt(0).getOp() instanceof LogicalUnionOperator);
        Assertions.assertEquals(1, countVersions(root, LogicalVersionOperator.VersionRefType.TO_VERSION));
        Assertions.assertEquals(1, countVersions(root, LogicalVersionOperator.VersionRefType.FROM_VERSION));
    }

    @Test
    public void testTransformFallsBackWhenAggregateCarriesLimit(@Mocked OlapTable table) {
        mockOlapPk(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        // A dropped LIMIT would silently give the MV more rows than its definition asks for.
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);
        ColumnRefOperator gRef = factory.create("g", IntegerType.INT, false);
        ColumnRefOperator vRef = factory.create("v", IntegerType.BIGINT, true);
        Map<ColumnRefOperator, Column> colRefMap = Maps.newHashMap();
        colRefMap.put(gRef, new Column("g", IntegerType.INT, false));
        colRefMap.put(vRef, new Column("v", IntegerType.BIGINT, true));
        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(factory.create("sum_combine", IntegerType.BIGINT, true),
                new CallOperator("sum_combine", IntegerType.BIGINT, List.of(vRef)));
        LogicalAggregationOperator agg = LogicalAggregationOperator.builder()
                .withOperator(new LogicalAggregationOperator(AggType.GLOBAL, List.of(gRef), aggMap))
                .setLimit(10).build();
        OptExpression aggExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(agg, newOlapPkScan(table, colRefMap)));
        deriveLogicalProperty(aggExpr);

        OptExpression root = new IvmDeltaRetractableAggregateRule().transform(aggExpr, context).get(0);
        Assertions.assertTrue(root.inputAt(1).getOp() instanceof LogicalAggregationOperator);
        Assertions.assertEquals(1, countVersions(root, LogicalVersionOperator.VersionRefType.FROM_VERSION));
    }

    private static int countVersions(OptExpression root, LogicalVersionOperator.VersionRefType type) {
        int count = root.getOp() instanceof LogicalVersionOperator version
                && version.getVersionRefType() == type ? 1 : 0;
        for (OptExpression input : root.getInputs()) {
            count += countVersions(input, type);
        }
        return count;
    }

    @Test
    public void testTransformRejectsMixedJoin(@Mocked OlapTable pk, @Mocked IcebergTable ice) {
        mockOlapPk(pk);
        mockIcebergTable(ice);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);

        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);
        ColumnRefOperator gRef = factory.create("g", IntegerType.INT, false);
        ColumnRefOperator vRef = factory.create("v", IntegerType.BIGINT, true);
        Map<ColumnRefOperator, Column> pkColRefMap = Maps.newHashMap();
        pkColRefMap.put(gRef, new Column("g", IntegerType.INT, false));
        pkColRefMap.put(vRef, new Column("v", IntegerType.BIGINT, true));
        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(factory.create("sum_combine", IntegerType.BIGINT, true),
                new CallOperator("sum_combine", IntegerType.BIGINT, List.of(vRef)));
        LogicalAggregationOperator agg = new LogicalAggregationOperator(AggType.GLOBAL, List.of(gRef), aggMap);
        OptExpression join = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, null),
                newOlapPkScan(pk, pkColRefMap), newIcebergScan(factory, ice));
        OptExpression aggExpr = OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(agg, join));
        deriveLogicalProperty(aggExpr);

        // a PK base joined to a non-PK (iceberg) base cannot honor the whole-child FROM/TO snapshot recompute
        Assertions.assertThrows(SemanticException.class,
                () -> new IvmDeltaRetractableAggregateRule().transform(aggExpr, context));
    }

    @Test
    public void testMutuallyExclusiveWithAppendOnlyOnPkBase(@Mocked OlapTable table) {
        mockOlapPk(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        OptExpression aggExpr = buildDeltaAggregateOverPk(factory, table);
        deriveLogicalProperty(aggExpr);
        // over a retractable PK base only the retractable rule fires; the append-only rule declines
        Assertions.assertTrue(new IvmDeltaRetractableAggregateRule().check(aggExpr, context));
        Assertions.assertFalse(new IvmDeltaAggregateRule().check(aggExpr, context));
    }

    @Test
    public void testDeclinesAppendOnlyBase(@Mocked IcebergTable table) {
        mockIcebergTable(table);
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        OptExpression aggExpr = buildDeltaAggregate(factory, table, true);
        deriveLogicalProperty(aggExpr);
        // over an append-only (iceberg) base the retractable rule declines; the append-only rule handles it
        Assertions.assertFalse(new IvmDeltaRetractableAggregateRule().check(aggExpr, context));
    }

    // ==================== Helpers ====================

    private void mockOlapPk(OlapTable table) {
        new Expectations() {
            {
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;
                table.getKeysType();
                result = KeysType.PRIMARY_KEYS;
                minTimes = 0;
            }
        };
    }

    private OptExpression newOlapPkScan(OlapTable table, Map<ColumnRefOperator, Column> colRefMap) {
        return OptExpression.create(LogicalOlapScanOperator.builder()
                .setTable(table)
                .setColRefToColumnMetaMap(colRefMap)
                .setColumnMetaToColRefMap(Maps.newHashMap())
                .setTableVersionRange(TvrTableDelta.of(1L, 2L))
                .build());
    }

    private OptExpression buildDeltaAggregateOverPk(ColumnRefFactory factory, OlapTable table) {
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);
        ColumnRefOperator gRef = factory.create("g", IntegerType.INT, false);
        ColumnRefOperator vRef = factory.create("v", IntegerType.BIGINT, true);
        Map<ColumnRefOperator, Column> colRefMap = Maps.newHashMap();
        colRefMap.put(gRef, new Column("g", IntegerType.INT, false));
        colRefMap.put(vRef, new Column("v", IntegerType.BIGINT, true));
        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(factory.create("sum_combine", IntegerType.BIGINT, true),
                new CallOperator("sum_combine", IntegerType.BIGINT, List.of(vRef)));
        LogicalAggregationOperator agg = new LogicalAggregationOperator(AggType.GLOBAL, List.of(gRef), aggMap);
        return OptExpression.create(new LogicalDeltaOperator(true, actionRef),
                OptExpression.create(agg, newOlapPkScan(table, colRefMap)));
    }

    private void mockIcebergTable(IcebergTable table) {
        new Expectations() {
            {
                table.getType();
                result = com.starrocks.catalog.Table.TableType.ICEBERG;
                minTimes = 0;
            }
        };
    }

    private OptExpression buildDeltaAggregate(ColumnRefFactory factory, IcebergTable table, boolean isRootDelta) {
        ColumnRefOperator actionRef = factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IntegerType.TINYINT, false);
        ColumnRefOperator dateRef = factory.create("date", IntegerType.INT, false);
        ColumnRefOperator sumRef = factory.create("sum_combine", IntegerType.BIGINT, true);
        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(sumRef, new CallOperator("sum_combine", IntegerType.BIGINT, List.of(dateRef)));
        LogicalAggregationOperator agg = new LogicalAggregationOperator(AggType.GLOBAL, List.of(dateRef), aggMap);
        OptExpression scanExpr = newIcebergScan(factory, table);
        return OptExpression.create(new LogicalDeltaOperator(isRootDelta, actionRef),
                OptExpression.create(agg, scanExpr));
    }

    private OptExpression newIcebergScan(ColumnRefFactory factory, IcebergTable table) {
        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator dataRef = factory.create("data", StringType.STRING, true);
        Column idCol = new Column("id", IntegerType.INT, false);
        Column dataCol = new Column("data", StringType.STRING, true);
        Map<ColumnRefOperator, Column> colRefMap = Maps.newHashMap();
        colRefMap.put(idRef, idCol);
        colRefMap.put(dataRef, dataCol);
        LogicalIcebergScanOperator scan = new LogicalIcebergScanOperator.Builder()
                .setTable(table)
                .setColRefToColumnMetaMap(colRefMap)
                .setTableVersionRange(TvrTableDelta.of(TvrVersion.of(100L), TvrVersion.of(200L)))
                .build();
        return OptExpression.create(scan);
    }

    private static void deriveLogicalProperty(OptExpression expression) {
        for (OptExpression child : expression.getInputs()) {
            deriveLogicalProperty(child);
        }
        ExpressionContext ctx = new ExpressionContext(expression);
        ctx.deriveLogicalProperty();
        expression.setLogicalProperty(ctx.getRootProperty());
    }
}
