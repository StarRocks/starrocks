// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;
import java.util.Map;

// Replace the corresponding ColumnRef with ScalarOperator
public class ReplaceColumnRefRewriter {
    private final Rewriter rewriter = new Rewriter();
    private final Map<ColumnRefOperator, ScalarOperator> operatorMap;

    private final boolean isRecursively;

    public ReplaceColumnRefRewriter(Map<ColumnRefOperator, ScalarOperator> operatorMap) {
        this.operatorMap = operatorMap;
        this.isRecursively = false;
    }

    public ReplaceColumnRefRewriter(Map<ColumnRefOperator, ScalarOperator> operatorMap, boolean isRecursively) {
        this.operatorMap = operatorMap;
        this.isRecursively = isRecursively;
    }

    public ScalarOperator rewrite(ScalarOperator origin) {
        if (origin == null) {
            return null;
        }
        return origin.clone().accept(rewriter, null);
    }

    private class Rewriter extends ScalarOperatorVisitor<ScalarOperator, Void> {
        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
            List<ScalarOperator> children = Lists.newArrayList(scalarOperator.getChildren());
            for (int i = 0; i < children.size(); ++i) {
                scalarOperator.setChild(i, scalarOperator.getChild(i).accept(this, null));
            }
            return scalarOperator;
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator column, Void context) {
            if (!operatorMap.containsKey(column)) {
                return column;
            }
            // Must clone here because
            // The rewritten predicate will be rewritten continually,
            // Rewiring predicate shouldn't change the origin project columnRefMap

            ScalarOperator mapperOperator = operatorMap.get(column).clone();
            for (int i = 0; i < mapperOperator.getChildren().size() && isRecursively; ++i) {
                mapperOperator.setChild(i, mapperOperator.getChild(i).accept(this, null));
            }
            return mapperOperator;
        }
    }
}
