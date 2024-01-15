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

import com.starrocks.sql.optimizer.base.ColumnRefSet;

import java.util.List;
import java.util.Objects;

public class HashCachedScalarOperator extends ScalarOperator {
    public ScalarOperator operator;
    private transient Integer hashValue = null;

    public HashCachedScalarOperator(ScalarOperator operator) {
        super(operator.getOpType(), operator.getType());
        this.operator = operator;
    }

    public ScalarOperator getOperator() {
        return operator;
    }

    @Override
    public int hashCode() {
        if (hashValue == null) {
            hashValue = operator.hashCode();
        }
        return hashValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HashCachedScalarOperator that = (HashCachedScalarOperator) o;
        return Objects.equals(this.getOperator(), that.getOperator());
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return null;
    }

    @Override
    public ScalarOperator getChild(int index) {
        return null;
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return null;
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        return null;
    }
}
