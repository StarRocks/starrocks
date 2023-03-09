// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;

import java.util.Map;
import java.util.Set;

public class ColumnRewriter {
    private final RewriteContext rewriteContext;

    public ColumnRewriter(RewriteContext rewriteContext) {
        this.rewriteContext = rewriteContext;
    }

    public ScalarOperator rewriteByQueryEc(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, false, false, true, true);
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteByViewEc(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, false, false, true, false);
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteViewToQuery(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, true, true, false, false);
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteViewToQueryWithQueryEc(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, true, true, true, true);
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteViewToQueryWithViewEc(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, true, true, true, false);
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteQueryToView(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, true, false, false, false);
        return predicate.accept(visitor, null);
    }

    private static class ColumnRewriteVisitor extends BaseScalarOperatorShuttle {
        private final boolean enableRelationRewrite;
        private final boolean enableEquivalenceClassesRewrite;

        private Map<Integer, Integer> srcToDstRelationIdMapping;
        private ColumnRefFactory srcRefFactory;
        private Map<Integer, Map<String, ColumnRefOperator>> dstRelationIdToColumns;
        private EquivalenceClasses equivalenceClasses;

        public ColumnRewriteVisitor(RewriteContext rewriteContext, boolean enableRelationRewrite, boolean viewToQuery,
                                    boolean enableEquivalenceClassesRewrite, boolean useQueryEquivalenceClasses) {
            this.enableRelationRewrite = enableRelationRewrite;
            this.enableEquivalenceClassesRewrite = enableEquivalenceClassesRewrite;

            if (enableRelationRewrite) {
                srcToDstRelationIdMapping = viewToQuery ? rewriteContext.getQueryToMvRelationIdMapping().inverse()
                        : rewriteContext.getQueryToMvRelationIdMapping();
                srcRefFactory = viewToQuery ? rewriteContext.getMvRefFactory() : rewriteContext.getQueryRefFactory();
                dstRelationIdToColumns = viewToQuery ? rewriteContext.getQueryRelationIdToColumns()
                        : rewriteContext.getMvRelationIdToColumns();
            }

            if (enableEquivalenceClassesRewrite) {
                equivalenceClasses = useQueryEquivalenceClasses ?
                        rewriteContext.getQueryEquivalenceClasses() : rewriteContext.getQueryBasedViewEquivalenceClasses();
            }
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator columnRef, Void context) {
            ColumnRefOperator result = columnRef;
            if (enableRelationRewrite && srcToDstRelationIdMapping != null) {
                Integer srcRelationId = srcRefFactory.getRelationId(columnRef.getId());
                if (srcRelationId < 0) {
                    return null;
                }
                Integer targetRelationId = srcToDstRelationIdMapping.get(srcRelationId);
                Map<String, ColumnRefOperator> relationColumns = dstRelationIdToColumns.get(targetRelationId);
                if (relationColumns == null) {
                    return null;
                }
                result = relationColumns.getOrDefault(columnRef.getName(), columnRef);
            }
            if (enableEquivalenceClassesRewrite && equivalenceClasses != null) {
                Set<ColumnRefOperator> equalities = equivalenceClasses.getEquivalenceClass(result);
                if (equalities != null) {
                    // equalities can not be empty.
                    // and for every item in equalities, the equalities is the same.
                    // so this will convert each equality column ref to the first one in the equalities.
                    result = equalities.iterator().next();
                }
            }
            return result;
        }
    }
}
