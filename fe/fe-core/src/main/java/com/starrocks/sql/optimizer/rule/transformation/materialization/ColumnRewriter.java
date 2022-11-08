// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnRewriter {
    private static final Logger LOG = LogManager.getLogger(ColumnRewriter.class);
    private RewriteContext rewriteContext;

    public ColumnRewriter(RewriteContext rewriteContext) {
        this.rewriteContext = rewriteContext;
    }

    public ScalarOperator rewriteByQueryEc(ScalarOperator predicate) {
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, false, false, true, true);
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteByViewEc(ScalarOperator predicate) {
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, false, false, true, false);
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteViewToQuery(ScalarOperator predicate) {
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, true, true, false, false);
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteViewToQueryWithQueryEc(ScalarOperator predicate) {
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, true, true, true, true);
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteViewToQueryWithViewEc(ScalarOperator predicate) {
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, true, true, true, false);
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteQueryToView(ScalarOperator predicate) {
        ColumnRewriteVisitor visitor = new ColumnRewriteVisitor(rewriteContext, true, false, false, false);
        return predicate.accept(visitor, null);
    }

    private class ColumnRewriteVisitor extends ScalarOperatorVisitor<ScalarOperator, Void> {
        private boolean enableRelationRewrite;
        private boolean enableEcRewrite;

        private Map<Integer, Integer> srcToDstRelationIdMapping;
        private ColumnRefFactory srcRefFactory;
        private ColumnRefFactory dstRefFactory;
        private Map<Integer, List<ColumnRefOperator>> srcRelationIdToColumns;
        private Map<Integer, List<ColumnRefOperator>> dstRelationIdToColumns;
        private EquivalenceClasses ec;

        public ColumnRewriteVisitor(RewriteContext rewriteContext, boolean enableRelationRewrite, boolean viewToQuery,
                                    boolean enableEcRewrite, boolean useQueryEc) {
            this.enableRelationRewrite = enableRelationRewrite;
            this.enableEcRewrite = enableEcRewrite;

            if (enableRelationRewrite) {
                srcToDstRelationIdMapping = viewToQuery ? rewriteContext.getQueryToMvRelationIdMapping().inverse()
                        : rewriteContext.getQueryToMvRelationIdMapping();
                srcRefFactory = viewToQuery ? rewriteContext.getMvRefFactory() : rewriteContext.getQueryRefFactory();
                dstRefFactory = viewToQuery ? rewriteContext.getQueryRefFactory() : rewriteContext.getMvRefFactory();
                srcRelationIdToColumns = viewToQuery ? rewriteContext.getMvRelationIdToColumns()
                        : rewriteContext.getQueryRelationIdToColumns();
                dstRelationIdToColumns = viewToQuery ? rewriteContext.getQueryRelationIdToColumns()
                        : rewriteContext.getMvRelationIdToColumns();
            }

            if (enableEcRewrite) {
                ec = useQueryEc ? rewriteContext.getQueryEc() : rewriteContext.getQueryBasedViewEc();
            }
        }

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
            List<ScalarOperator> children = Lists.newArrayList(scalarOperator.getChildren());
            for (int i = 0; i < children.size(); ++i) {
                ScalarOperator child = scalarOperator.getChild(i).accept(this, context);
                if (child == null) {
                    return null;
                }
                scalarOperator.setChild(i, child);
            }
            return scalarOperator;
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator columnRef, Void context) {
            ColumnRefOperator result = columnRef;
            if (enableRelationRewrite && srcToDstRelationIdMapping != null) {
                Integer srcRelationId = srcRefFactory.getRelationId(columnRef.getId());
                if (srcRelationId < 0) {
                    LOG.warn("invalid columnRef:%s", columnRef);
                    return null;
                }
                Integer targetRelationId = srcToDstRelationIdMapping.get(srcRelationId);
                List<ColumnRefOperator> relationColumns = dstRelationIdToColumns.get(targetRelationId);
                if (relationColumns == null) {
                    LOG.warn("no columns for relation id:%d", targetRelationId);
                    return null;
                }
                boolean found = false;
                for (ColumnRefOperator dstColumnRef : relationColumns) {
                    if (columnRef.getName().equals(dstColumnRef.getName())) {
                        result = dstColumnRef;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.warn("can not find column ref:%s in target relation:%d", columnRef, targetRelationId);
                }
            }
            if (enableEcRewrite && ec != null) {
                Set<ColumnRefOperator> equalities = ec.getEquivalenceClass(result);
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
