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
    private final RewriteContext rewriteContext;

    public ColumnRewriter(RewriteContext rewriteContext) {
        this.rewriteContext = rewriteContext;
    }

    public ScalarOperator rewriteByQueryEc(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ColumnRewriteVisitor visitor =
                new ColumnWriterBuilder()
                        .withRewriteContext(rewriteContext)
                        .withEnableEquivalenceClassesRewrite(true)
                        .withUseQueryEquivalenceClasses(true)
                        .build();
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteByViewEc(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ColumnRewriteVisitor visitor =
                new ColumnWriterBuilder()
                        .withRewriteContext(rewriteContext)
                        .withEnableEquivalenceClassesRewrite(true)
                        .build();
        return predicate.accept(visitor, null);
    }

    public ColumnRefOperator rewriteViewToQuery(ColumnRefOperator colRef) {
        if (colRef == null) {
            return null;
        }
        ColumnRewriteVisitor visitor =
                new ColumnWriterBuilder()
                        .withRewriteContext(rewriteContext)
                        .withEnableRelationRewrite(true)
                        .withViewToQuery(true)
                        .build();
        ScalarOperator target = colRef.accept(visitor, null);
        if (target == null || target == colRef) {
            return null;
        }
        return (ColumnRefOperator) target;
    }

    public ScalarOperator rewriteViewToQueryWithQueryEc(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ColumnRewriteVisitor visitor =
                new ColumnWriterBuilder()
                        .withRewriteContext(rewriteContext)
                        .withEnableRelationRewrite(true)
                        .withViewToQuery(true)
                        .withEnableEquivalenceClassesRewrite(true)
                        .withUseQueryEquivalenceClasses(true)
                        .build();
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteViewToQueryWithViewEc(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ColumnRewriteVisitor visitor =
                new ColumnWriterBuilder()
                        .withRewriteContext(rewriteContext)
                        .withEnableRelationRewrite(true)
                        .withViewToQuery(true)
                        .withEnableEquivalenceClassesRewrite(true)
                        .build();
        return predicate.accept(visitor, null);
    }

    public ScalarOperator rewriteQueryToView(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        ColumnRewriteVisitor visitor = new ColumnWriterBuilder()
                .withRewriteContext(rewriteContext)
                .withEnableRelationRewrite(true)
                .build();
        return predicate.accept(visitor, null);
    }

<<<<<<< HEAD
    private static class ColumnRewriteVisitor extends ScalarOperatorVisitor<ScalarOperator, Void> {
=======
    public class ColumnWriterBuilder {
        private RewriteContext rewriteContext;
        private boolean enableRelationRewrite;
        private boolean viewToQuery;
        private boolean enableEquivalenceClassesRewrite;
        private boolean useQueryEquivalenceClasses;

        ColumnWriterBuilder withRewriteContext(RewriteContext rewriteContext) {
            this.rewriteContext = rewriteContext;
            return this;
        }

        ColumnWriterBuilder withEnableRelationRewrite(boolean enableRelationRewrite) {
            this.enableRelationRewrite = enableRelationRewrite;
            return this;
        }

        ColumnWriterBuilder withViewToQuery(boolean viewToQuery) {
            this.viewToQuery = viewToQuery;
            return this;
        }
        ColumnWriterBuilder withEnableEquivalenceClassesRewrite(boolean enableEquivalenceClassesRewrite) {
            this.enableEquivalenceClassesRewrite = enableEquivalenceClassesRewrite;
            return this;
        }

        ColumnWriterBuilder withUseQueryEquivalenceClasses(boolean useQueryEquivalenceClasses) {
            this.useQueryEquivalenceClasses = useQueryEquivalenceClasses;
            return this;
        }

        ColumnRewriteVisitor build() {
            return new ColumnRewriteVisitor(this.rewriteContext,
                    this.enableRelationRewrite,
                    this.viewToQuery,
                    this.enableEquivalenceClassesRewrite,
                    this.useQueryEquivalenceClasses);
        }
    }

    private static class ColumnRewriteVisitor extends BaseScalarOperatorShuttle {
>>>>>>> f241c36fa ([Enhancement] Support TPCH Benchmark for MV (#18506))
        private final boolean enableRelationRewrite;
        private final boolean enableEquivalenceClassesRewrite;
        private Map<Integer, Integer> srcToDstRelationIdMapping;
        private ColumnRefFactory srcRefFactory;
        private Map<Integer, List<ColumnRefOperator>> dstRelationIdToColumns;
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
                    LOG.warn("invalid columnRef: {}", columnRef);
                    return null;
                }
                Integer targetRelationId = srcToDstRelationIdMapping.get(srcRelationId);
                List<ColumnRefOperator> relationColumns = dstRelationIdToColumns.get(targetRelationId);
                if (relationColumns == null) {
                    LOG.warn("no columns for relation id:{}", targetRelationId);
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
                    LOG.warn("can not find column ref id:{} name:{} in target relation:{}",
                            columnRef.getId(), columnRef.getName(), targetRelationId);
                }
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
