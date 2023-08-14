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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.mv.JoinDeriveContext;
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

    // mv's partition and distribution related conjunct predicate,
    // used to prune partitions and buckets of scan mv operator after rewrite
    private ScalarOperator mvPruneConjunct;

    private final List<ScalarOperator> onPredicates;
    private final Rule rule;
    private List<ColumnRefOperator> enforcedColumns;

    private List<JoinDeriveContext> joinDeriveContexts;

    public MvRewriteContext(
            MaterializationContext materializationContext,
            List<Table> queryTables,
            OptExpression queryExpression,
            ReplaceColumnRefRewriter queryColumnRefRewriter,
            PredicateSplit queryPredicateSplit,
            List<ScalarOperator> onPredicates, Rule rule) {
        this.materializationContext = materializationContext;
        this.queryTables = queryTables;
        this.queryExpression = queryExpression;
        this.queryColumnRefRewriter = queryColumnRefRewriter;
        this.queryPredicateSplit = queryPredicateSplit;
        this.onPredicates = onPredicates;
        this.rule = rule;
        this.joinDeriveContexts = Lists.newArrayList();
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

    public ScalarOperator getMvPruneConjunct() {
        return mvPruneConjunct;
    }

    public void setMvPruneConjunct(ScalarOperator mvPruneConjunct) {
        this.mvPruneConjunct = mvPruneConjunct;
    }

    public List<ScalarOperator> getOnPredicates() {
        return onPredicates;
    }

    public Rule getRule() {
        return rule;
    }

    public void addJoinDeriveContext(JoinDeriveContext joinDeriveContext) {
        joinDeriveContexts.add(joinDeriveContext);
    }

    public List<JoinDeriveContext> getJoinDeriveContexts() {
        return joinDeriveContexts;
    }

    public List<ColumnRefOperator> getEnforcedColumns() {
        return enforcedColumns;
    }

    public void setEnforcedColumns(List<ColumnRefOperator> enforcedColumns) {
        this.enforcedColumns = enforcedColumns;
    }
}
