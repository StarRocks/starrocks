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

package com.starrocks.sql.automv.pn;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.Type;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

// Val means typed constant
public final class Val extends Op implements Comparable {
    public static final Val FALSE_VAL = new Val(ConstantOperator.FALSE);
    public static final Val NULL_VAL = new Val(ConstantOperator.NULL);
    public static final Val TRUE_VAL = new Val(ConstantOperator.TRUE);
    private final ConstantOperator value;

    public Val(ConstantOperator value) {
        super(value.getType());
        this.value = value;
    }

    @Override
    public Op clone() {
        return new Val(this.value);
    }

    @Override
    protected int hashCodeImpl() {
        return Objects.hash(type, value);
    }

    @Override
    protected String toStringImpl() {
        return String.format("(val[%s] %s)", type.toSql(), value.toString());
    }

    @Override
    protected String toIsomorphicStringImpl() {
        return toStringImpl();
    }

    public ConstantOperator getValue() {
        return value;
    }

    @Override
    protected SymTab getSymTabImpl() {
        return SymTab.EMPTY;
    }

    @Override
    public <R, C> R accept(OpVisitor<R, C> visitor, C context) {
        return visitor.visitVal(this, context);
    }

    @Override
    public int nary() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Val val = (Val) o;
        return Objects.equals(value, val.value);
    }

    @Override
    protected int getHeightImpl() {
        return 1;
    }

    @Override
    public int compareTo(@NotNull Object o) {
        Preconditions.checkArgument(o.getClass().equals(Val.class));
        Val that = (Val) o;
        Preconditions.checkArgument(this.getType().equals(that.getType()));
        Type type = this.getType();
        if (type.isDate()) {
            return this.getValue().getDate().compareTo(that.getValue().getDate());
        } else if (type.isDatetime()) {
            return this.getValue().getDatetime().compareTo(that.getValue().getDatetime());
        } else if (type.isIntegerType()) {
            return Integer.compare(this.getValue().getInt(), that.getValue().getInt());
        } else if (type.isStringType()) {
            return this.getValue().getVarchar().compareTo(that.getValue().getVarchar());
        } else {
            Preconditions.checkArgument(type.isDateType() || type.isIntegerType() || type.isStringType());
        }
        return 0;
    }
}