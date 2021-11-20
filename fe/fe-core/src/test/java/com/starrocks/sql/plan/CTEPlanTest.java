// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.planner.PlannerContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.thrift.TExplainLevel;
import org.junit.Test;

import java.util.Map;

public class CTEPlanTest extends PlanTestBase {

    @Test
    public void testCTE2() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator v1 = factory.create("v1", Type.BIGINT, true);
        ColumnRefOperator v2 = factory.create("v2", Type.BIGINT, true);
        ColumnRefOperator v3 = factory.create("v3", Type.BIGINT, true);

        ColumnRefOperator x1 = factory.create("x1", Type.BIGINT, true);
        ColumnRefOperator x2 = factory.create("x2", Type.BIGINT, true);
        ColumnRefOperator x3 = factory.create("x3", Type.BIGINT, true);

        ColumnRefOperator x4 = factory.create("x4", Type.BIGINT, true);
        ColumnRefOperator x5 = factory.create("x5", Type.BIGINT, true);
        ColumnRefOperator x6 = factory.create("x6", Type.BIGINT, true);

        OlapTable table = (OlapTable) Catalog.getCurrentCatalog().getDb("default_cluster:test").getTable("t0");
        Map<ColumnRefOperator, Column> crc = ImmutableMap.<ColumnRefOperator, Column>builder()
                .put(v1, table.getBaseColumn("v1"))
                .put(v2, table.getBaseColumn("v2"))
                .put(v3, table.getBaseColumn("v3"))
                .build();

        Map<Column, ColumnRefOperator> ccr = ImmutableMap.<Column, ColumnRefOperator>builder()
                .put(table.getBaseColumn("v1"), v1)
                .put(table.getBaseColumn("v2"), v2)
                .put(table.getBaseColumn("v3"), v3)
                .build();

        OptExpression scan = OptExpression
                .create(new LogicalOlapScanOperator(table, crc, ccr, new HashDistributionSpec(new HashDistributionDesc(
                        Lists.newArrayList(v1.getId()), HashDistributionDesc.SourceType.LOCAL)), -1, null));

        OptExpression produce = OptExpression.create(new LogicalCTEProduceOperator("cte1"), scan);

        Map<ColumnRefOperator, ColumnRefOperator> csm1 = ImmutableMap.<ColumnRefOperator, ColumnRefOperator>builder()
                .put(x1, v1)
                .put(x2, v2)
                .put(x3, v3)
                .build();
        Map<ColumnRefOperator, ColumnRefOperator> csm2 = ImmutableMap.<ColumnRefOperator, ColumnRefOperator>builder()
                .put(x4, v1)
                .put(x5, v2)
                .put(x6, v3)
                .build();
        OptExpression consume1 = OptExpression.create(new LogicalCTEConsumeOperator(-1, null, null, "cte1", csm1));
        OptExpression consume2 = OptExpression.create(new LogicalCTEConsumeOperator(-1, null, null, "cte1", csm2));

        OptExpression join = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, x1, x4)), consume1, consume2);

        OptExpression anchor = OptExpression.create(new LogicalCTEAnchorOperator("cte1"), produce, join);

        Optimizer optimizer = new Optimizer();

        ColumnRefSet ref = new ColumnRefSet();
        ref.union(x1);
        ref.union(x4);
        ref.union(x5);

        OptExpression tree = optimizer.optimize(connectContext, anchor, new PhysicalPropertySet(), ref, factory);

        PlannerContext
                plannerContext = new PlannerContext(null, null, connectContext.getSessionVariable().toThrift(), null);
        ExecPlan plan = new PlanFragmentBuilder().createPhysicalPlan(
                tree, plannerContext, connectContext, Lists.newArrayList(v1, v2, v3), factory,
                Lists.newArrayList("x1", "x4", "x5"));

        System.out.println(plan.getExplainString(TExplainLevel.NORMAL));
    }
}
