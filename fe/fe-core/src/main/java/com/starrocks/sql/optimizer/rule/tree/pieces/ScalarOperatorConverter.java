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

package com.starrocks.sql.optimizer.rule.tree.pieces;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.Map;

class ScalarOperatorConverter {
    // origin column refs mapping to new column refs
    private final Map<ColumnRefOperator, ColumnRefOperator> refsMapping = Maps.newHashMap();

    private final ReplaceColumnRefRewriter refRewriter = new ReplaceColumnRefRewriter(refsMapping, false);

    // new refs -> refs expression, use for normalize projection
    private final Map<ColumnRefOperator, ScalarOperator> expressionMapping = Maps.newHashMap();

    private final ReplaceColumnRefRewriter exprRewriter = new ReplaceColumnRefRewriter(expressionMapping, true);

    private int nextID = 0;

    public int getNextID() {
        Preconditions.checkState(nextID > -1);
        return nextID++;
    }

    public void disableCreateRef() {
        nextID = -100;
    }

    public void add(ColumnRefOperator originRef, ColumnRefOperator newRef) {
        refsMapping.put(originRef, newRef);
    }

    public boolean contains(ColumnRefOperator ref) {
        return refsMapping.containsKey(ref);
    }

    public ColumnRefOperator convertRef(ColumnRefOperator columnRef) {
        if (refsMapping.containsKey(columnRef)) {
            return refsMapping.get(columnRef);
        }

        Preconditions.checkState(nextID > -1);
        return convertRef(columnRef, nextID++);
    }

    public ColumnRefOperator convertRef(ColumnRefOperator originRef, int newId) {
        ColumnRefOperator newRef =
                new ColumnRefOperator(newId, originRef.getType(), originRef.getName(), originRef.isNullable());
        refsMapping.put(originRef, newRef);
        return newRef;
    }

    public ScalarOperator convert(ScalarOperator operator) {
        return refRewriter.rewrite(operator);
    }

    public void addExpr(ColumnRefOperator ref, ScalarOperator expr) {
        expressionMapping.put(ref, expr);
    }

    public ScalarOperator convertExpr(ScalarOperator operator) {
        return exprRewriter.rewrite(refRewriter.rewrite(operator));
    }
}
