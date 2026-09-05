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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

/**
 * Base interface to implement materialized view rewrite rule.
 */
public interface IMaterializedViewRewriter {

    /**
     * Do rewrite query's plan based on materialized view's plan.
     */
    OptExpression doViewBasedRewrite(RewriteContext rewriteContext,
                                     OptExpression mvScanOptExpression);

    /**
     * Do rewrite mv's defined query plan based on query's plan.
     */
    OptExpression doQueryBasedRewrite(RewriteContext rewriteContext,
                                      ScalarOperator compensationPredicates,
                                      OptExpression queryExpression);
    /**
     * Union Rewrite by viewBasedRewrite and queryBasedRewrite.
     * NOTE: Ensure plan's output column refs are unique even for the same query/mv plans.
     * NOTE: viewInput's column ref's uniqueness is ensured by viewBasedRewrite.
     */
    OptExpression doUnionRewrite(OptExpression queryInput, OptExpression viewInput,
                                 RewriteContext rewriteContext);

    /**
     * Rewrite the query with the given materialized view context.
     * @param mvContext: materialized view context of query and associated mv.
     * @return: the rewritten query opt expression if rewrite success, otherwise null.
     */
    OptExpression doRewrite(MvRewriteContext mvContext);

    /**
     * After plan is rewritten by MV, still do some actions for new MV's plan.
     * 1. column prune
     * 2. partition prune
     * 3. bucket prune
     */
    OptExpression postRewrite(OptimizerContext optimizerContext,
                              MvRewriteContext mvRewriteContext,
                              OptExpression candidate);
}
