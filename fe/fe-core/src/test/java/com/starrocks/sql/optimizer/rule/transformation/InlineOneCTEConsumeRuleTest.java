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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.optimizer.CTEUtils;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TaskScheduler;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InlineOneCTEConsumeRuleTest {

    /**
     * cte1 (a) is read by cte3 (c), and both are inlined in the same pass. After predicate and projection
     * push-down, the inlined body of one CONSUME(c) is directly CONSUME(a). The rewriter visits only the
     * children of a replacement, so the nested CONSUME(a) used to be skipped while the same pass pruned
     * the anchors of cte1 and cte3. Nothing triggered another pass, and the orphan consume made the root
     * CTE property unsatisfiable: "no executable plan for this sql ... required property: ... EMPTY CTE".
     */
    @Test
    public void testInlinePassLeavesNoOrphanConsumeForDirectlyNestedConsumes() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator produced = factory.create("v", Type.BIGINT, true);
        ColumnRefOperator reused = factory.create("w", Type.BIGINT, true);
        ColumnRefOperator reusedLeft = factory.create("w_left", Type.BIGINT, true);
        ColumnRefOperator reusedRight = factory.create("w_right", Type.BIGINT, true);
        ColumnRefOperator consumedByC = factory.create("c_v", Type.BIGINT, true);
        ColumnRefOperator consumedByQuery = factory.create("q_v", Type.BIGINT, true);

        // Query:
        // WITH a AS (VALUES v), b AS (VALUES w), c AS (SELECT * FROM a)
        // SELECT * FROM (SELECT q_v FROM c GROUP BY q_v), b, b
        //
        // The interesting part of the physical plan tree:
        // ANCHOR 3
        // ├─ PRODUCE 3
        // │    └─ CONSUME 1 {c_v → v}
        // │         └─ VALUES (c_v)
        // └─ AGG
        //     └─ CONSUME 3 {q_v → c_v}
        //          └─ CONSUME 1 {q_v → v}
        //               └─ VALUES (q_v)
        //
        // The inlined body of CONSUME(c) is directly CONSUME(a), with no node in between.
        OptExpression produceA = OptExpression.create(new LogicalCTEProduceOperator(1), values(produced));
        // b stays reused: its consumers are forced (no inlined body), so it keeps cte1 and cte3 apart,
        // which lets the same pass prune both of their anchors.
        OptExpression produceB = OptExpression.create(new LogicalCTEProduceOperator(2), values(reused));
        OptExpression produceC = OptExpression.create(new LogicalCTEProduceOperator(3),
                consume(1, consumedByC, produced, values(consumedByC)) /* CONSUME(a) */);
        // CONSUME(c)
        OptExpression consumeC = consume(3, consumedByQuery, consumedByC,
                consume(1, consumedByQuery, produced, values(consumedByQuery)) /* CONSUME(a) */);
        OptExpression aggregate = OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL,
                new ArrayList<>(ImmutableList.of(consumedByQuery)), Maps.newHashMap()), consumeC);
        OptExpression query = crossJoin(crossJoin(aggregate, forcedConsume(2, reusedLeft, reused)),
                forcedConsume(2, reusedRight, reused));
        OptExpression anchorC = OptExpression.create(new LogicalCTEAnchorOperator(3), produceC, query);
        OptExpression anchorB = OptExpression.create(new LogicalCTEAnchorOperator(2), produceB, anchorC);
        OptExpression anchorA = OptExpression.create(new LogicalCTEAnchorOperator(1), produceA, anchorB);
        OptExpression tree = OptExpression.create(new LogicalTreeAnchorOperator(), anchorA);
        deriveLogicalProperty(tree);

        OptimizerContext context = OptimizerFactory.mockContext(new ConnectContext(), factory);
        TaskContext taskContext = new TaskContext(context, new PhysicalPropertySet(), new ColumnRefSet(),
                Double.MAX_VALUE);
        TaskScheduler scheduler = new TaskScheduler();

        // Same loop as QueryOptimizer#logicalRuleRewrite: both CTEs have one consumer, so both are inlined.
        CTEUtils.collectCteOperators(tree, context);
        Assertions.assertTrue(context.getCteContext().needInline(1));
        Assertions.assertFalse(context.getCteContext().needInline(2));
        Assertions.assertTrue(context.getCteContext().needInline(3));
        while (context.getCteContext().hasInlineCTE()) {
            scheduler.rewriteOnce(tree, taskContext, RuleSet.INLINE_CTE_RULES);
            CTEUtils.collectCteOperators(tree, context);
        }

        // Only the reused cte2 may remain: its anchor, its produce and its two forced consumers.
        List<String> cteOperators = new ArrayList<>();
        collectCteOperators(tree, cteOperators);
        Assertions.assertEquals(List.of("LOGICAL_CTE_ANCHOR 2", "LOGICAL_CTE_PRODUCE 2",
                "LOGICAL_CTE_CONSUME 2", "LOGICAL_CTE_CONSUME 2"), cteOperators);
    }

    @Test
    public void testTransformUnwrapsDirectlyNestedInlinedConsume() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator produced = factory.create("v", Type.BIGINT, true);
        ColumnRefOperator consumedByC = factory.create("c_v", Type.BIGINT, true);
        ColumnRefOperator consumedByQuery = factory.create("q_v", Type.BIGINT, true);

        OptExpression body = values(consumedByQuery);
        OptExpression consumeC = consume(3, consumedByQuery, consumedByC,
                consume(1, consumedByQuery, produced, body));

        OptimizerContext context = OptimizerFactory.mockContext(new ConnectContext(), factory);
        // No consumer of either CTE is registered, so both count as inlined.
        Assertions.assertTrue(context.getCteContext().needInline(1));
        Assertions.assertTrue(context.getCteContext().needInline(3));

        InlineOneCTEConsumeRule rule = new InlineOneCTEConsumeRule();
        Assertions.assertTrue(rule.check(consumeC, context));
        List<OptExpression> result = rule.transform(consumeC, context);
        Assertions.assertEquals(1, result.size());
        Assertions.assertSame(body, result.get(0));
    }

    private static OptExpression values(ColumnRefOperator column) {
        return OptExpression.create(new LogicalValuesOperator(ImmutableList.of(column)));
    }

    private static OptExpression consume(int cteId, ColumnRefOperator output, ColumnRefOperator produced,
                                         OptExpression inlinedBody) {
        Map<ColumnRefOperator, ColumnRefOperator> outputMap = Maps.newHashMap();
        outputMap.put(output, produced);
        return OptExpression.create(new LogicalCTEConsumeOperator(cteId, outputMap), inlinedBody);
    }

    private static OptExpression forcedConsume(int cteId, ColumnRefOperator output, ColumnRefOperator produced) {
        Map<ColumnRefOperator, ColumnRefOperator> outputMap = Maps.newHashMap();
        outputMap.put(output, produced);
        return OptExpression.create(new LogicalCTEConsumeOperator(cteId, outputMap));
    }

    private static OptExpression crossJoin(OptExpression left, OptExpression right) {
        return OptExpression.create(new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null), left, right);
    }

    private static void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }
        ExpressionContext context = new ExpressionContext(root);
        context.deriveLogicalProperty();
        root.setLogicalProperty(context.getRootProperty());
    }

    private static void collectCteOperators(OptExpression root, List<String> operators) {
        if (root.getOp() instanceof LogicalCTEAnchorOperator anchor) {
            operators.add(OperatorType.LOGICAL_CTE_ANCHOR + " " + anchor.getCteId());
        } else if (root.getOp() instanceof LogicalCTEProduceOperator produce) {
            operators.add(OperatorType.LOGICAL_CTE_PRODUCE + " " + produce.getCteId());
        } else if (root.getOp() instanceof LogicalCTEConsumeOperator consume) {
            operators.add(OperatorType.LOGICAL_CTE_CONSUME + " " + consume.getCteId());
        }
        root.getInputs().forEach(child -> collectCteOperators(child, operators));
    }
}
