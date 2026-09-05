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

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.IMaterializedViewRewriter;

import java.util.List;

/**
 * Base interface to implement materialized view rewrite rule.
 */
public interface IMaterializedViewRewriteRule {
    /**
     * Prune the materialized view candidates.
     * @param queryExpression: query opt expression.
     * @param context: optimizer context.
     * @param mvCandidateContexts: materialized view candidates prepared in the mv preprocessor.
     * @return: the pruned materialized view candidates.
     */
    List<MaterializationContext> doPrune(OptExpression queryExpression,
                                         OptimizerContext context,
                                         List<MaterializationContext> mvCandidateContexts);
    /**
     * Create a materialized view rewriter to do the mv rewrite for the specific query opt expression.
     * @param mvContext: materialized view context of query and associated mv.
     * @return: the specific rewriter for the mv rewrite.
     */
    IMaterializedViewRewriter createRewriter(OptimizerContext optimizerContext,
                                             MvRewriteContext mvContext);
}
