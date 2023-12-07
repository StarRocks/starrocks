// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;

import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType.EQ_FOR_NULL;

// Rewrite ScalarOperator as DictMappingOperator
// if a ScalarOperator support dictionary optimization, we will rewrite it to DictMappingOperator
// if a ScalarOperator don't support dictionary optimization but its children support dictionary optimization,
// we will rewrite its child as DictMappingOperator
public class DictMappingRewriter {
    AddDecodeNodeForDictStringRule.DecodeContext decodeContext;
    RewriterContext rewriterContext = new RewriterContext();

    public DictMappingRewriter(AddDecodeNodeForDictStringRule.DecodeContext decodeContext) {
        this.decodeContext = decodeContext;
        decodeContext.stringColumnIdToDictColumnIds.keySet().forEach(rewriterContext.stringColumnSet::union);
    }

    public ScalarOperator rewrite(ScalarOperator scalarOperator) {
        Rewriter rewriter = new Rewriter();
        ScalarOperator operator = scalarOperator.accept(rewriter, rewriterContext);
        if (rewriterContext.hasAppliedOperator) {
            Type returnType = operator.getType().isVarchar() ? AddDecodeNodeForDictStringRule.ID_TYPE :
                    operator.getType();
            operator = rewriteAsDictMapping(operator, returnType);
        }
        return operator;
    }

    private static class RewriterContext {
        // It is believed that a low cardinality optimization has been applied
        boolean hasAppliedOperator = false;
        boolean hasUnsupportedOperator = false;

        ColumnRefSet stringColumnSet = new ColumnRefSet();

        void reset() {
            hasAppliedOperator = false;
            hasUnsupportedOperator = false;
        }
    }

    // rewrite scalar operator as dict mapping operator
    ScalarOperator rewriteAsDictMapping(ScalarOperator scalarOperator, Type type) {
        final ColumnRefSet usedColumns = scalarOperator.getUsedColumns();
        Preconditions.checkState(usedColumns.cardinality() == 1);
        final Integer dictColumnId = decodeContext.stringColumnIdToDictColumnIds.get(usedColumns.getFirstId());
        ColumnRefOperator dictColumn = decodeContext.columnRefFactory.getColumnRef(dictColumnId);
        scalarOperator = new DictMappingOperator(dictColumn, scalarOperator.clone(), type);
        return scalarOperator;
    }

    private class Rewriter extends ScalarOperatorVisitor<ScalarOperator, RewriterContext> {

        ScalarOperator rewriteForScalarOperator(ScalarOperator operator, RewriterContext context) {
            final ColumnRefSet usedColumns = operator.getUsedColumns();
            // without intersect column, skip rewrite
            if (!context.stringColumnSet.isIntersect(usedColumns)) {
                context.hasAppliedOperator = false;
                return operator;
            }
            // Currently, only single column input is supported using DictExpr rewriting,
            // multiple columns are only partially supported for rewriting.
            if (usedColumns.cardinality() == 1) {
                // check child support opt
                List<ScalarOperator> children = Lists.newArrayList(operator.getChildren());
                boolean hasApplied = false;
                boolean disableApplied = false;
                for (int i = 0; i < children.size(); i++) {
                    context.reset();
                    children.set(i, children.get(i).accept(this, context));
                    hasApplied = hasApplied || context.hasAppliedOperator;
                    disableApplied = disableApplied || context.hasUnsupportedOperator;
                }
                if (!disableApplied || !hasApplied) {
                    context.hasAppliedOperator = hasApplied;
                    return operator;
                } else {
                    context.hasAppliedOperator = false;
                    return visit(operator, context);
                }
            } else {
                // add dict operator for each child
                return visit(operator, context);
            }
        }

        private ScalarOperator addDictExprToBlockDictOpt(ScalarOperator scalarOperator, RewriterContext context) {
            List<ScalarOperator> children = Lists.newArrayList(scalarOperator.getChildren());
            boolean hasApplied = false;
            boolean disableApplied = context.hasUnsupportedOperator;
            // For any expression that does not support low-cardinality optimization,
            // if child already uses optimization, we need to add a DictExpr
            for (int i = 0; i < children.size(); i++) {
                context.reset();
                ScalarOperator child = scalarOperator.getChild(i).accept(this, context);
                // wrapper using DictExpr
                if (context.hasAppliedOperator) {
                    child = rewriteAsDictMapping(child, child.getType());
                    context.hasUnsupportedOperator = true;
                }
                scalarOperator.setChild(i, child);
                hasApplied = hasApplied || context.hasAppliedOperator;
                disableApplied = disableApplied || context.hasUnsupportedOperator;
            }
            context.hasAppliedOperator = false;
            context.hasUnsupportedOperator = disableApplied;
            return scalarOperator;
        }

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, RewriterContext context) {
            // When we call the function "visit(ScalarOperator,...)",
            // this means that this current operator no longer supports using dictionary optimization,
            // and at this time we need to block the dictionary optimization if child has already applied it.
            // So at this time we need to rewrite the child using DictExpr in time
            return addDictExprToBlockDictOpt(scalarOperator, context);
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, RewriterContext context) {
            if (call.getFunction() == null || !call.getFunction().isCouldApplyDictOptimize()) {
                context.hasAppliedOperator = false;
                context.hasUnsupportedOperator = true;
                return visit(call, context);
            }
            return rewriteForScalarOperator(call, context);
        }

        @Override
        public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, RewriterContext context) {
            if (predicate.getBinaryType() == EQ_FOR_NULL || !predicate.getChild(1).isConstant() ||
                    !predicate.getChild(0).isColumnRef()) {
                context.hasAppliedOperator = false;
                context.hasUnsupportedOperator = true;
                return visit(predicate, context);
            }
            return rewriteForScalarOperator(predicate, context);
        }

        @Override
        public ScalarOperator visitInPredicate(InPredicateOperator predicate, RewriterContext context) {
            if (!predicate.getChild(0).isColumnRef()) {
                context.hasAppliedOperator = false;
                context.hasUnsupportedOperator = true;
                return visit(predicate, context);
            }
            return rewriteForScalarOperator(predicate, context);
        }

        @Override
        public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate, RewriterContext context) {
            if (!predicate.getChild(0).isColumnRef()) {
                context.hasAppliedOperator = false;
                context.hasUnsupportedOperator = true;
                return visit(predicate, context);
            }

            return rewriteForScalarOperator(predicate, context);
        }

        @Override
        public ScalarOperator visitCastOperator(CastOperator operator, RewriterContext context) {
            operator.setChild(0, operator.getChild(0).accept(this, context));
            return operator;
        }

        @Override
        public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, RewriterContext context) {
            return rewriteForScalarOperator(operator, context);
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator operator, RewriterContext context) {
            context.hasAppliedOperator = rewriterContext.stringColumnSet.contains(operator.getId());
            context.hasUnsupportedOperator = false;
            return operator;
        }

        @Override
        public ScalarOperator visitConstant(ConstantOperator operator, RewriterContext context) {
            context.hasAppliedOperator = false;
            context.hasUnsupportedOperator = false;
            return operator;
        }

        @Override
        public ScalarOperator visitLikePredicateOperator(LikePredicateOperator operator, RewriterContext context) {
            operator.setChild(0, operator.getChild(0).accept(this, context));
            return operator;
        }

        @Override
        public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator operator, RewriterContext context) {
            return rewriteForScalarOperator(operator, context);
        }

    }

}
