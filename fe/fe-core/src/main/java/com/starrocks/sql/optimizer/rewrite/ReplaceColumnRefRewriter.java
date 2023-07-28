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
    private final Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap;

    private final boolean isRecursively;

    public ReplaceColumnRefRewriter(Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap) {
        this.operatorMap = operatorMap;
        this.isRecursively = false;
    }

    public ReplaceColumnRefRewriter(Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap,
                                    boolean isRecursively) {
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
            if (isRecursively) {
                while (mapperOperator.getChildren().isEmpty() && operatorMap.containsKey(mapperOperator)) {
                    ScalarOperator mapped = operatorMap.get(mapperOperator);
                    if (mapped.equals(mapperOperator)) {
                        break;
                    }
                    mapperOperator = mapped.clone();
                }
                for (int i = 0; i < mapperOperator.getChildren().size(); ++i) {
                    mapperOperator.setChild(i, mapperOperator.getChild(i).accept(this, null));
                }
            }
            return mapperOperator;
        }
    }
}
