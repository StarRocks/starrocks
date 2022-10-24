// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.BiMap;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RewriteContext {
    private final OptExpression queryExpression;
    private final LogicalProjectOperator queryProjection;
    private final PredicateSplit queryPredicateSplit;
    private final EquivalenceClasses queryEc;
    // key is table relation id
    private final Map<Integer, List<ColumnRefOperator>> queryRelationIdToColumns;
    private final ColumnRefFactory queryRefFactory;
    private final ReplaceColumnRefRewriter queryColumnRefRewriter;

    private final OptExpression mvExpression;
    private final LogicalProjectOperator mvProjection;
    private final PredicateSplit mvPredicateSplit;
    private final Map<Integer, List<ColumnRefOperator>> mvRelationIdToColumns;
    private final ColumnRefFactory mvRefFactory;
    private EquivalenceClasses queryBasedViewEc;
    private final ReplaceColumnRefRewriter mvColumnRefRewriter;

    private final Map<ColumnRefOperator, ColumnRefOperator> outputMapping;
    private final Set<ColumnRefOperator> queryColumnSet;
    private BiMap<Integer, Integer> queryToMvRelationIdMapping;

    /*
    public RewriteContext(BiMap<Integer, Integer> relationIdMap,
                          Map<Integer, List<ColumnRefOperator>> queryRelationIdToColumns,
                          ColumnRefFactory queryRefFactory,
                          Map<Integer, List<ColumnRefOperator>> mvRelationIdToColumns,
                          ColumnRefFactory mvRefFactory) {
        this.queryToMvRelationIdMapping = relationIdMap;
        this.queryRelationIdToColumns = queryRelationIdToColumns;
        this.queryRefFactory = queryRefFactory;
        this.mvRelationIdToColumns = mvRelationIdToColumns;
        this.mvRefFactory = mvRefFactory;
    }

     */

    public RewriteContext(OptExpression queryExpression,
                          LogicalProjectOperator queryProjection,
                          PredicateSplit queryPredicateSplit,
                          EquivalenceClasses queryEc,
                          Map<Integer, List<ColumnRefOperator>> queryRelationIdToColumns,
                          ColumnRefFactory queryRefFactory,
                          ReplaceColumnRefRewriter queryColumnRefRewriter,
                          OptExpression mvExpression,
                          LogicalProjectOperator mvProjection,
                          PredicateSplit mvPredicateSplit,
                          Map<Integer, List<ColumnRefOperator>> mvRelationIdToColumns,
                          ColumnRefFactory mvRefFactory,
                          ReplaceColumnRefRewriter mvColumnRefRewriter,
                          Map<ColumnRefOperator, ColumnRefOperator> outputMapping,
                          Set<ColumnRefOperator> queryColumnSet) {
        this.queryExpression = queryExpression;
        this.queryProjection = queryProjection;
        this.queryPredicateSplit = queryPredicateSplit;
        this.queryEc = queryEc;
        this.queryRelationIdToColumns = queryRelationIdToColumns;
        this.queryRefFactory = queryRefFactory;
        this.queryColumnRefRewriter = queryColumnRefRewriter;
        this.mvExpression = mvExpression;
        this.mvProjection = mvProjection;
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

    public LogicalProjectOperator getQueryProjection() {
        return queryProjection;
    }

    public PredicateSplit getQueryPredicateSplit() {
        return queryPredicateSplit;
    }

    public EquivalenceClasses getQueryEc() {
        return queryEc;
    }

    public Map<Integer, List<ColumnRefOperator>> getQueryRelationIdToColumns() {
        return queryRelationIdToColumns;
    }

    public OptExpression getMvExpression() {
        return mvExpression;
    }

    public LogicalProjectOperator getMvProjection() {
        return mvProjection;
    }

    public PredicateSplit getMvPredicateSplit() {
        return mvPredicateSplit;
    }

    public Map<Integer, List<ColumnRefOperator>> getMvRelationIdToColumns() {
        return mvRelationIdToColumns;
    }

    public ColumnRefFactory getQueryRefFactory() {
        return queryRefFactory;
    }

    public ColumnRefFactory getMvRefFactory() {
        return mvRefFactory;
    }

    public EquivalenceClasses getQueryBasedViewEc() {
        return queryBasedViewEc;
    }

    public void setQueryBasedViewEc(EquivalenceClasses queryBasedViewEc) {
        this.queryBasedViewEc = queryBasedViewEc;
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
}
