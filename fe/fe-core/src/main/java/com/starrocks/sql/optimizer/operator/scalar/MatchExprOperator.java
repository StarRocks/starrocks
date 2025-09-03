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

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;
import java.util.Objects;

public class MatchExprOperator extends ArgsScalarOperator {
    private MatchExpr.MatchOperator matchOperator = MatchExpr.MatchOperator.MATCH;

    public MatchExprOperator(MatchExpr.MatchOperator matchOperator, ScalarOperator... arguments) {
        this(Lists.newArrayList(arguments));
        this.matchOperator = matchOperator;
    }

    public MatchExprOperator(List<ScalarOperator> arguments) {
        super(OperatorType.MATCH_EXPR, Type.BOOLEAN);
        Preconditions.checkState(arguments.size() == 2);
        this.arguments = arguments;
        this.incrDepth(arguments);
    }

    public MatchExpr.MatchOperator getMatchOperator() {
        return matchOperator;
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return arguments;
    }

    @Override
    public String toString() {
        return getChild(0).toString() + " " + matchOperator.getName() + " " + getChild(1).toString();
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return  visitor.visitMatchExprOperator(this, context);
    }

    @Override
    public String debugString() {
        return getChild(0).debugString() + " " + matchOperator.getName() + " " + getChild(1).debugString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(arguments.get(0), arguments.get(1));
    }

    @Override
    public int hashCodeSelf() {
        return Objects.hash(opType);
    }

    @Override
    public ScalarOperator getChild(int index) {
        return arguments.get(index);
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
        arguments.set(index, child);
    }

    @Override
    public boolean isNullable() {
        return arguments.stream().anyMatch(ScalarOperator::isNullable);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet used = new ColumnRefSet();
        for (ScalarOperator child : arguments) {
            used.union(child.getUsedColumns());
        }
        return used;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MatchExprOperator other = (MatchExprOperator) obj;
        return Objects.equals(this.arguments, other.arguments) && this.matchOperator == other.matchOperator;
    }

    @Override
    public ScalarOperator clone() {
        MatchExprOperator operator = (MatchExprOperator) super.clone();
        // Deep copy here
        List<ScalarOperator> newArguments = Lists.newArrayList();
        this.arguments.forEach(p -> newArguments.add(p.clone()));
        operator.arguments = newArguments;
        operator.matchOperator = matchOperator;
        return operator;
    }
}
