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

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.transformation.ApplyExceptionRule;
import com.starrocks.sql.optimizer.rule.transformation.EliminateAggRule;
import com.starrocks.sql.optimizer.rule.transformation.ForceCTEReuseRule;
import com.starrocks.sql.optimizer.rule.transformation.IcebergPartitionsTableRewriteRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinOnExpressionToChildProject;
import com.starrocks.sql.optimizer.rule.transformation.PushDownProjectLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.SchemaTableEvaluateRule;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TaskScheduler;

public class ColumnPrivilegeOptimizer extends Optimizer {
    private final TaskScheduler scheduler = new TaskScheduler();

    ColumnPrivilegeOptimizer(OptimizerContext context) {
        super(context);
        context.setTaskScheduler(scheduler);
    }

    @Override
    public OptExpression optimize(OptExpression tree, PhysicalPropertySet requiredProperty,
                                  ColumnRefSet requiredColumns) {
        TaskContext rootTaskContext =
                new TaskContext(context, requiredProperty, requiredColumns.clone(), Double.MAX_VALUE);

        tree = OptExpression.create(new LogicalTreeAnchorOperator(), tree);

        deriveLogicalProperty(tree);

        CTEContext cteContext = context.getCteContext();
        CTEUtils.collectCteOperators(tree, context);
        cteContext.setForceInline();

        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.INLINE_CTE_RULES);
            CTEUtils.collectCteOperators(tree, context);
        }

        scheduler.rewriteOnce(tree, rootTaskContext, new IcebergPartitionsTableRewriteRule());
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.AGGREGATE_REWRITE_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.PUSH_DOWN_SUBQUERY_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.SUBQUERY_REWRITE_COMMON_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.SUBQUERY_REWRITE_TO_JOIN_RULES);
        scheduler.rewriteOnce(tree, rootTaskContext, new ApplyExceptionRule());
        CTEUtils.collectCteOperators(tree, context);

        // Note: PUSH_DOWN_PREDICATE tasks should be executed before MERGE_LIMIT tasks
        // because of the Filter node needs to be merged first to avoid the Limit node
        // cannot merge
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.PUSH_DOWN_PREDICATE_RULES);
        scheduler.rewriteOnce(tree, rootTaskContext, SchemaTableEvaluateRule.getInstance());

        scheduler.rewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.ELIMINATE_OP_WITH_CONSTANT_RULES);

        scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.PRUNE_COLUMNS_RULES);
        // Put EliminateAggRule after PRUNE_COLUMNS to give a chance to prune group bys before eliminate aggregations.
        scheduler.rewriteOnce(tree, rootTaskContext, EliminateAggRule.getInstance());
        deriveLogicalProperty(tree);

        scheduler.rewriteOnce(tree, rootTaskContext, new PushDownJoinOnExpressionToChildProject());
        scheduler.rewriteIterative(tree, rootTaskContext, new PruneEmptyWindowRule());
        scheduler.rewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());

        // Limit push must be after the column prune,
        // otherwise the Node containing limit may be prune
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.MERGE_LIMIT_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, new PushDownProjectLimitRule());

        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.PRUNE_PROJECT_RULES);

        CTEUtils.collectCteOperators(tree, context);
        if (cteContext.needOptimizeCTE()) {
            cteContext.reset();
            scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.COLLECT_CTE_RULES);
            rootTaskContext.setRequiredColumns(requiredColumns.clone());
            scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.PRUNE_COLUMNS_RULES);
            scheduler.rewriteOnce(tree, rootTaskContext, new ForceCTEReuseRule());
        }
        Preconditions.checkState(!cteContext.needOptimizeCTE());

        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.PUSH_DOWN_PREDICATE_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.PRUNE_PROJECT_RULES);

        CTEUtils.collectCteOperators(tree, context);
        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.INLINE_CTE_RULES);
            CTEUtils.collectCteOperators(tree, context);
        }

        scheduler.rewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());

        // After this rule, we shouldn't generate logical project operator
        scheduler.rewriteIterative(tree, rootTaskContext, new MergeProjectWithChildRule());
        scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.VECTOR_REWRITE_RULES);
        return tree.getInputs().get(0);
    }
}
