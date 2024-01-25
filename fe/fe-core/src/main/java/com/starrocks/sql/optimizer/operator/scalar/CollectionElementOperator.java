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

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.OperatorType.COLLECTION_ELEMENT;

public class CollectionElementOperator extends ScalarOperator {
    protected List<ScalarOperator> arguments = Lists.newArrayList();
    private boolean isCheckOutOfBounds = false;

    public CollectionElementOperator(Type type, ScalarOperator arrayOperator, ScalarOperator subscriptOperator,
                                     boolean isCheckOutOfBounds) {
        super(COLLECTION_ELEMENT, type);
        this.arguments.add(arrayOperator);
        this.arguments.add(subscriptOperator);
        this.isCheckOutOfBounds = isCheckOutOfBounds;
    }

    public boolean isCheckOutOfBounds() {
        return isCheckOutOfBounds;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return arguments;
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
    public String toString() {
        return arguments.stream().map(ScalarOperator::toString).collect(Collectors.joining(","));
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
    public ScalarOperator clone() {
        CollectionElementOperator operator = (CollectionElementOperator) super.clone();
        // Deep copy here
        List<ScalarOperator> newArguments = Lists.newArrayList();
        this.arguments.forEach(p -> newArguments.add(p.clone()));
        operator.arguments = newArguments;
        return operator;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitCollectionElement(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CollectionElementOperator that = (CollectionElementOperator) o;
        return Objects.equal(arguments, that.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(arguments);
    }
}
