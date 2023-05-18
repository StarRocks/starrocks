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

import com.google.common.collect.BiMap;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.Map;
import java.util.Set;

public class RewriteContext {
    private final OptExpression queryExpression;
    private final PredicateSplit queryPredicateSplit;
    private EquivalenceClasses queryEquivalenceClasses;
    // key is table relation id
    private final Map<Integer, Map<String, ColumnRefOperator>> queryRelationIdToColumns;
    private final ColumnRefFactory queryRefFactory;
    private final ReplaceColumnRefRewriter queryColumnRefRewriter;

    private final OptExpression mvExpression;
    private final PredicateSplit mvPredicateSplit;
    private final Map<Integer, Map<String, ColumnRefOperator>> mvRelationIdToColumns;
    private final ColumnRefFactory mvRefFactory;
    private EquivalenceClasses queryBasedViewEquivalenceClasses;
    private final ReplaceColumnRefRewriter mvColumnRefRewriter;

    private final Map<ColumnRefOperator, ColumnRefOperator> outputMapping;
    private final Set<ColumnRefOperator> queryColumnSet;
    private BiMap<Integer, Integer> queryToMvRelationIdMapping;

    public RewriteContext(OptExpression queryExpression,
                          PredicateSplit queryPredicateSplit,
                          EquivalenceClasses queryEquivalenceClasses,
                          Map<Integer, Map<String, ColumnRefOperator>> queryRelationIdToColumns,
                          ColumnRefFactory queryRefFactory,
                          ReplaceColumnRefRewriter queryColumnRefRewriter,
                          OptExpression mvExpression,
                          PredicateSplit mvPredicateSplit,
                          Map<Integer, Map<String, ColumnRefOperator>> mvRelationIdToColumns,
                          ColumnRefFactory mvRefFactory,
                          ReplaceColumnRefRewriter mvColumnRefRewriter,
                          Map<ColumnRefOperator, ColumnRefOperator> outputMapping,
                          Set<ColumnRefOperator> queryColumnSet) {
        this.queryExpression = queryExpression;
        this.queryPredicateSplit = queryPredicateSplit;
        this.queryEquivalenceClasses = queryEquivalenceClasses;
        this.queryRelationIdToColumns = queryRelationIdToColumns;
        this.queryRefFactory = queryRefFactory;
        this.queryColumnRefRewriter = queryColumnRefRewriter;
        this.mvExpression = mvExpression;
        this.mvPredicateSplit = mvPredicateSplit;
        this.mvRelationIdToColumns = mvRelationIdToColumns;
        this.mvRefFactory = mvRefFactory;
        this.mvColumnRefRewriter = mvColumnRefRewriter;
        this.outputMapping = outputMapping;
        this.queryColumnSet = queryColumnSet;
    }

    public BiMap<Integer, Integer> getQueryToMvRelationIdMapping() {
        return queryToMvRelationIdMapping;
    }

    public void setQueryToMvRelationIdMapping(BiMap<Integer, Integer> queryToMvRelationIdMapping) {
        this.queryToMvRelationIdMapping = queryToMvRelationIdMapping;
    }

    public OptExpression getQueryExpression() {
        return queryExpression;
    }

    public PredicateSplit getQueryPredicateSplit() {
        return queryPredicateSplit;
    }

    public EquivalenceClasses getQueryEquivalenceClasses() {
        return queryEquivalenceClasses;
    }

    public void setQueryEquivalenceClasses(EquivalenceClasses queryEquivalenceClasses) {
        this.queryEquivalenceClasses = queryEquivalenceClasses;
    }

    public Map<Integer, Map<String, ColumnRefOperator>> getQueryRelationIdToColumns() {
        return queryRelationIdToColumns;
    }

    public OptExpression getMvExpression() {
        return mvExpression;
    }

    public PredicateSplit getMvPredicateSplit() {
        return mvPredicateSplit;
    }

    public Map<Integer, Map<String, ColumnRefOperator>> getMvRelationIdToColumns() {
        return mvRelationIdToColumns;
    }

    public ColumnRefFactory getQueryRefFactory() {
        return queryRefFactory;
    }

    public ColumnRefFactory getMvRefFactory() {
        return mvRefFactory;
    }

    public EquivalenceClasses getQueryBasedViewEquivalenceClasses() {
        return queryBasedViewEquivalenceClasses;
    }

    public void setQueryBasedViewEquivalenceClasses(EquivalenceClasses queryBasedViewEquivalenceClasses) {
        this.queryBasedViewEquivalenceClasses = queryBasedViewEquivalenceClasses;
    }

    public ReplaceColumnRefRewriter getQueryColumnRefRewriter() {
        return queryColumnRefRewriter;
    }

    public ReplaceColumnRefRewriter getMvColumnRefRewriter() {
        return mvColumnRefRewriter;
    }

    public Map<ColumnRefOperator, ColumnRefOperator> getOutputMapping() {
        return outputMapping;
    }

    public Set<ColumnRefOperator> getQueryColumnSet() {
        return queryColumnSet;
    }

    public Map<ColumnRefOperator, ScalarOperator> getMVColumnRefToScalarOp() {
        return MvUtils.getColumnRefMap(getMvExpression(), getMvRefFactory());
    }
}
