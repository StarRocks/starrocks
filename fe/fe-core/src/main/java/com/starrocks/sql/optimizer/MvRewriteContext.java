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

import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.PredicateSplit;

import java.util.List;

// the difference from MaterializationContext is that
// MaterializationContext has static properties for mv,
// while MvRewriteContext has properties for each rewrite process.
public class MvRewriteContext {
    private final MaterializationContext materializationContext;
    private final List<Table> queryTables;
    // logical OptExpression for query
    private final OptExpression queryExpression;
    private final ReplaceColumnRefRewriter queryColumnRefRewriter;
    private final PredicateSplit queryPredicateSplit;

    public MvRewriteContext(
            MaterializationContext materializationContext,
            List<Table> queryTables,
            OptExpression queryExpression,
            ReplaceColumnRefRewriter queryColumnRefRewriter,
            PredicateSplit queryPredicateSplit) {
        this.materializationContext = materializationContext;
        this.queryTables = queryTables;
        this.queryExpression = queryExpression;
        this.queryColumnRefRewriter = queryColumnRefRewriter;
        this.queryPredicateSplit = queryPredicateSplit;
    }

    public MaterializationContext getMaterializationContext() {
        return materializationContext;
    }

    public List<Table> getQueryTables() {
        return queryTables;
    }

    public OptExpression getQueryExpression() {
        return queryExpression;
    }

    public ReplaceColumnRefRewriter getQueryColumnRefRewriter() {
        return queryColumnRefRewriter;
    }

    public PredicateSplit getQueryPredicateSplit() {
        return queryPredicateSplit;
    }
}
