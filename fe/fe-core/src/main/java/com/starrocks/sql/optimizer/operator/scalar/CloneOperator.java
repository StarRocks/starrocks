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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;
import java.util.Objects;

public class CloneOperator extends ScalarOperator {
    private List<ScalarOperator> arguments;

    public CloneOperator(ScalarOperator argument) {
        super(OperatorType.CLONE, argument.getType());
        arguments = Lists.newArrayList(argument);
        setType(argument.getType());
    }

    @Override
    public boolean isNullable() {
        return arguments.get(0).isNullable();
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return arguments;
    }

    @Override
    public ScalarOperator getChild(int index) {
        return arguments.get(0);
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
        arguments.set(0, child);
    }

    @Override
    public String toString() {
        return "Clone(" + arguments.get(0) + ")";
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return Objects.hash(arguments);
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitCloneOperator(this, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        return arguments.get(0).getUsedColumns();
    }

    @Override
    public ScalarOperator clone() {
        CloneOperator operator = (CloneOperator) super.clone();
        // Deep copy here
        List<ScalarOperator> newArguments = Lists.newArrayList();
        this.arguments.forEach(p -> newArguments.add(p.clone()));
        operator.arguments = newArguments;
        return operator;
    }
}
