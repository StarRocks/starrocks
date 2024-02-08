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

package com.starrocks.sql.optimizer.rule.tree.prunesubfield;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;

import java.util.List;

/*
 * collect all complex expressions, such as: MAP_KEYS, MAP_VALUES, map['key'], struct.a.b.c ...
 */
public class SubfieldExpressionCollector extends ScalarOperatorVisitor<Void, Void> {
    private final List<ScalarOperator> complexExpressions = Lists.newArrayList();

    private final boolean enableJsonCollect;

    public List<ScalarOperator> getComplexExpressions() {
        return complexExpressions;
    }

    public SubfieldExpressionCollector() {
        this(true);
    }

    public SubfieldExpressionCollector(boolean enableJsonCollect) {
        this.enableJsonCollect = enableJsonCollect;
    }

    @Override
    public Void visit(ScalarOperator scalarOperator, Void context) {
        for (ScalarOperator child : scalarOperator.getChildren()) {
            child.accept(this, context);
        }
        return null;
    }

    @Override
    public Void visitVariableReference(ColumnRefOperator variable, Void context) {
        if (variable.getType().isComplexType() || variable.getType().isJsonType()) {
            complexExpressions.add(variable);
        }
        return null;
    }

    @Override
    public Void visitCollectionElement(CollectionElementOperator collectionElementOp, Void context) {
        if (collectionElementOp.getUsedColumns().isEmpty()) {
            return null;
        }
        complexExpressions.add(collectionElementOp);
        return null;
    }

    @Override
    public Void visitSubfield(SubfieldOperator subfieldOperator, Void context) {
        if (subfieldOperator.getUsedColumns().isEmpty()) {
            return null;
        }
        complexExpressions.add(subfieldOperator);
        return null;
    }

    @Override
    public Void visitCall(CallOperator call, Void context) {
        if (call.getUsedColumns().isEmpty()) {
            return null;
        }

        if (!PruneSubfieldRule.SUPPORT_FUNCTIONS.contains(call.getFnName())) {
            return visit(call, context);
        }

        // Json function has multi-version, support use path version
        if (PruneSubfieldRule.SUPPORT_JSON_FUNCTIONS.contains(call.getFnName())) {
            if (!enableJsonCollect) {
                return visit(call, context);
            }
            Type[] args = call.getFunction().getArgs();
            if (args.length <= 1 || !args[0].isJsonType() || !args[1].isStringType()) {
                return visit(call, context);
            }
        }

        complexExpressions.add(call);
        return null;
    }
}
