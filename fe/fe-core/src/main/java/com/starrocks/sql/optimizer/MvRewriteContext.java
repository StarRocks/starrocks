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

<<<<<<< HEAD
import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.mv.JoinDeriveContext;
=======
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
>>>>>>> branch-2.5-mrs
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

<<<<<<< HEAD
=======
    private List<ScalarOperator> queryJoinOnPredicates;

    private List<ScalarOperator> mvJoinOnPredicates;

>>>>>>> branch-2.5-mrs
    // mv's partition and distribution related conjunct predicate,
    // used to prune partitions and buckets of scan mv operator after rewrite
    private ScalarOperator mvPruneConjunct;

<<<<<<< HEAD
    private final List<ScalarOperator> onPredicates;
    private List<ColumnRefOperator> enforcedColumns;

    private List<JoinDeriveContext> joinDeriveContexts;

    private final Rule rule;

    public MvRewriteContext(MaterializationContext materializationContext,
                            List<Table> queryTables,
                            OptExpression queryExpression,
                            ReplaceColumnRefRewriter queryColumnRefRewriter,
                            PredicateSplit queryPredicateSplit,
                            List<ScalarOperator> onPredicates,
                            Rule rule) {
=======
    public MvRewriteContext(
            MaterializationContext materializationContext,
            List<Table> queryTables,
            OptExpression queryExpression,
            ReplaceColumnRefRewriter queryColumnRefRewriter,
            PredicateSplit queryPredicateSplit) {
>>>>>>> branch-2.5-mrs
        this.materializationContext = materializationContext;
        this.queryTables = queryTables;
        this.queryExpression = queryExpression;
        this.queryColumnRefRewriter = queryColumnRefRewriter;
        this.queryPredicateSplit = queryPredicateSplit;
<<<<<<< HEAD
        this.onPredicates = onPredicates;
        this.joinDeriveContexts = Lists.newArrayList();
        this.rule = rule;
=======
>>>>>>> branch-2.5-mrs
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

<<<<<<< HEAD
    public void setMvPruneConjunct(ScalarOperator mvPruneConjunct) {
        this.mvPruneConjunct = mvPruneConjunct;
    }

    public List<ScalarOperator> getOnPredicates() {
        return onPredicates;
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

    public Rule getRule() {
        return rule;
=======
    public List<ScalarOperator> getQueryJoinOnPredicates() {
        if (queryJoinOnPredicates == null) {
            queryJoinOnPredicates = MvUtils.getJoinOnPredicates(queryExpression);
        }
        return queryJoinOnPredicates;
    }

    public List<ScalarOperator> getMvJoinOnPredicates() {
        if (mvJoinOnPredicates == null) {
            mvJoinOnPredicates = MvUtils.getJoinOnPredicates(materializationContext.getMvExpression());

        }
        return mvJoinOnPredicates;
    }

    public void setMvPruneConjunct(ScalarOperator mvPruneConjunct) {
        this.mvPruneConjunct = mvPruneConjunct;
>>>>>>> branch-2.5-mrs
    }
}
