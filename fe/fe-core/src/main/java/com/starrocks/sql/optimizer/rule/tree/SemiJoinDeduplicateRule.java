package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;

/*
 *
 *
 * */

public class SemiJoinDeduplicateRule implements TreeRewriteRule {
    private static final DeduplicateVisitor HANDLER = new DeduplicateVisitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        return null;
    }

    private static class DeduplicateContext {
        boolean canDeduplicate;
        boolean parentIsSemiAntiJoin;

        public DeduplicateContext(boolean parentIsSemiJoin, boolean canDeduplicate) {
            this.parentIsSemiAntiJoin = parentIsSemiJoin;
            this.canDeduplicate = canDeduplicate;
        }

        public DeduplicateContext() {
            canDeduplicate = false;
            parentIsSemiAntiJoin = false;
        }
    }

    private static class DeduplicateVisitor
            extends OptExpressionVisitor<Void, DeduplicateContext> {

        private Void visitChildren(List<OptExpression> children, DeduplicateContext context) {
            for (OptExpression child : children) {
                // pass "false,false" to children
                child.getOp().accept(this, child, new DeduplicateContext());
            }
            return null;
        }

        @Override
        public Void visit(OptExpression opt, DeduplicateContext context) {
            
            visitChildren(opt.getInputs(), context);
            return null;
        }


        @Override
        public Void visitLogicalProject(OptExpression opt, DeduplicateContext context) {
            // pass through the context from parent to child
            opt.inputAt(0).getOp().accept(this, opt.inputAt(0), context);
            return null;
        }

        @Override
        public Void visitLogicalJoin(OptExpression opt, DeduplicateContext context) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) opt.getOp();
            List<OptExpression> children = opt.getInputs();
            boolean isSemiAntiJoin = joinOperator.getJoinType().isSemiAntiJoin();
            if (isSemiAntiJoin) {
                // left join's right side or right join's left side can always deduplicate
                OptExpression alwaysCanDeduplicateChild =
                        joinOperator.getJoinType().isLeftSemiAntiJoin() ? children.get(1) : children.get(0);
                // another child depends on parent
                OptExpression dependOnParentChild =
                        joinOperator.getJoinType().isLeftSemiAntiJoin() ? children.get(0) : children.get(1);

                alwaysCanDeduplicateChild.getOp()
                        .accept(this, alwaysCanDeduplicateChild, new DeduplicateContext(true, true));
                dependOnParentChild.getOp()
                        .accept(this, dependOnParentChild, new DeduplicateContext(true, context.canDeduplicate));
            } else {
                visitChildren(children, context);
            }

            return null;
        }
    }
}
