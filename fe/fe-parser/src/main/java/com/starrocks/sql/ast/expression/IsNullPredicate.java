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

package com.starrocks.sql.ast.expression;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public class IsNullPredicate extends Predicate {
    private final boolean isNotNull;

    public IsNullPredicate(Expr e, boolean isNotNull) {
        this(e, isNotNull, NodePosition.ZERO);
    }

    public IsNullPredicate(Expr e, boolean isNotNull, NodePosition pos) {
        super(pos);
        this.isNotNull = isNotNull;
        Preconditions.checkNotNull(e);
        children.add(e);
    }

    protected IsNullPredicate(IsNullPredicate other) {
        super(other);
        this.isNotNull = other.isNotNull;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    @Override
    public Expr clone() {
        return new IsNullPredicate(this);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), isNotNull);
    }
    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (!super.equalsWithoutChild(obj)) {
            return false;
        }
        return ((IsNullPredicate) obj).isNotNull == isNotNull;
    }


    public boolean isNullable() {
        return false;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)  {
        return visitor.visitIsNullPredicate(this, context);
    }
}
